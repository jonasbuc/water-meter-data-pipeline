"""
ingestion.py
------------
Formål: læse CSV/JSON-filer og indsætte dem UÆNDREDE i raw_meter_readings.

Designprincip: ingestion-laget laver INGEN forretningslogik.
- Ingen validering af FORRETNINGSREGLER (det sker i staging)
- Ingen type-konvertering (alt gemmes som tekst)
- Ingen record-level dedup (det sker i staging)

Hvorfor? Fordi hvis vi en dag opdager en fejl i vores validerings-logik,
skal vi kunne re-køre validering/transformation fra raw uden at have mistet
eller ændret noget fra kilden. Dette er "bevar rådata"-princippet.

Alternativ, vi IKKE vælger: parse og validere direkte ved ingestion.
Trade-off ved det: hurtigere pipeline, men vi mister evnen til at
'replaye' historikken, og fejl i logik kan betyde permanent datatab.

FILE-LEVEL IDEMPOTENCY:
Uden dette ville raw_meter_readings vokse hver gang pipeline'en kørte,
selvom kildemappen ikke indeholder nogen nye filer (fx fordi filer
arkiveres i stedet for at blive slettet). Vi sporer allerede-indlæste
filer i `ingested_files` via SHA-256 af filens indhold - se schema.sql
for en uddybning af content- vs. filnavn-baseret idempotency.

TRANSAKTIONSGRÆNSE (rettet i denne review-runde):
Tidligere skete "registrér fil", "indsæt raw-rækker" og "markér PROCESSED"
i TRE separate transaktioner. Det betød at et crash mellem trin 2 og 3
kunne efterlade en fil markeret PENDING med raw-rækker allerede committet -
og et efterfølgende genkørsel ville ikke genkende filen som færdigbehandlet
(kun `status == 'PROCESSED'` tæller som "allerede set"), så raw-rækkerne
blev indsat IGEN. Det brød selve løftet om file-level idempotency.

Nu er "claim fil" + "indsæt raw-rækker" + "markér PROCESSED" ÉN atomisk
transaktion: enten lykkes alle tre sammen, eller ingen af dem gør
(rollback). Selve fil-læsning/parsing sker bevidst UDENFOR transaktionen
(det er CPU/IO-arbejde, ikke database-arbejde, og bør ikke holde en
database-transaktion åben unødigt længe).

Hvis noget fejler UNDER selve databasetransaktionen, fanges det, og filen
markeres i stedet FAILED i en lille, SEPARAT transaktion (så vi kan
observere fejlen, uden at det er en del af den transaktion der lige
rullede tilbage). Et PENDING-forsøg der aldrig commiter, efterlader IKKE
et spor - kun et succesfuldt (PROCESSED) eller mislykket (FAILED) forsøg
bliver synligt i ingested_files.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# --- Kilde-kontrakt (schema drift skal være eksplicit, ikke tilfældig) ---
# Disse kolonner SKAL findes i kildefilen, ellers fejler filen synligt
# (status=FAILED i ingested_files) i stedet for at fejle uforudsigeligt
# længere nede i pipelinen.
REQUIRED_COLUMNS = [
    "meter_id",
    "timestamp",
    "consumption_liters",
]

# Disse kolonner er FORVENTEDE, men ikke strengt påkrævet at kildesystemet
# medsender dem - mangler de, behandles de som NULL/tomme, ikke som en fejl.
OPTIONAL_COLUMNS = [
    "temperature",
    "status",
]

# Bevaret for bagudkompatibilitet med kode/tests der importerer denne liste.
EXPECTED_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _file_hash(file_path: Path) -> str:
    """SHA-256 af filens rå bytes - vores content-baserede idempotency-nøgle."""
    return hashlib.sha256(file_path.read_bytes()).hexdigest()


def read_csv_file(file_path: Path) -> pd.DataFrame:
    """Læser en CSV-fil ind som DataFrame. Alt læses som string (dtype=str),
    fordi raw-laget ikke skal type-konvertere - det sker i staging."""
    df = pd.read_csv(file_path, dtype=str)
    return df


def read_json_file(file_path: Path) -> pd.DataFrame:
    """
    Læser en JSON-fil ind som DataFrame, alt som string.

    SCHEMA-DRIFT-KONTRAKT: kilden forventes at sende en LISTE af objekter
    (fx `[{...}, {...}]`). Sender kilden i stedet et enkelt objekt eller en
    anden JSON-struktur, fejler vi EKSPLICIT med en klar besked, i stedet
    for at pandas stille laver noget uventet (fx én kolonne pr. nøgle i et
    enkelt objekt, transponeret forkert).
    """
    with open(file_path, "r") as f:
        records = json.load(f)
    if not isinstance(records, list):
        raise ValueError(
            f"Fil {file_path.name}: forventede en JSON-liste af rækker, "
            f"fik {type(records).__name__}"
        )
    df = pd.DataFrame(records)
    return df.astype(str)


def read_source_file(file_path: Path) -> pd.DataFrame:
    """Vælger parser ud fra filendelse."""
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return read_csv_file(file_path)
    elif suffix == ".json":
        return read_json_file(file_path)
    else:
        raise ValueError(f"Ukendt filtype: {suffix}")


def _validate_source_contract(df: pd.DataFrame, file_name: str) -> None:
    """
    Håndhæver den eksplicitte kilde-kontrakt (item #6 i denne review):

    - Manglende PÅKRÆVET kolonne -> filen fejler (rejses som ValueError,
      fanges af ingest_file og markeres FAILED).
    - Ekstra/ukendte kolonner    -> accepteres, med en WARNING i loggen.
      Kildesystemer tilføjer ofte nye felter over tid; det bør ikke i sig
      selv stoppe pipelinen.
    - Tom, men i øvrigt gyldig fil (0 rækker, korrekte kolonner) -> IKKE en
      fejl. Det behandles som SUCCESS med 0 rækker - en fil kan legitimt
      være tom (fx ingen målinger i en periode).
    """
    missing_required = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_required:
        raise ValueError(
            f"Fil {file_name} mangler påkrævede kolonner: {sorted(missing_required)}"
        )

    known_columns = set(REQUIRED_COLUMNS) | set(OPTIONAL_COLUMNS)
    unexpected = set(df.columns) - known_columns
    if unexpected:
        logger.warning(
            "Fil %s indeholder ukendte ekstra kolonner (accepteres): %s",
            file_name, sorted(unexpected),
        )


def _claim_or_reuse_file(conn, file_hash: str, file_name: str) -> tuple:
    """
    Slår filens hash op INDE I den aktive transaktion (den autoritative
    tjek - se ingest_file for en optimerings-tjek uden for transaktionen).

    Returnerer (file_id, already_processed: bool, is_new: bool).
    """
    existing = conn.execute(
        text("SELECT file_id, status FROM ingested_files WHERE file_hash = :h"),
        {"h": file_hash},
    ).fetchone()

    if existing is not None:
        file_id, status = existing
        return file_id, status == "PROCESSED", False

    result = conn.execute(
        text(
            """
            INSERT INTO ingested_files (file_name, file_hash, first_seen_at, status)
            VALUES (:file_name, :file_hash, :first_seen_at, 'PENDING')
            """
        ),
        {
            "file_name": file_name,
            "file_hash": file_hash,
            "first_seen_at": _now_iso(),
        },
    )
    return result.lastrowid, False, True


def _mark_file_failed(engine: Engine, file_hash: str, file_name: str) -> None:
    """
    Registrerer at et ingestion-forsøg for denne fil FEJLEDE.

    Kører i sin EGEN lille transaktion, adskilt fra det forsøg der lige
    rullede tilbage - ellers ville denne markering også blive rullet
    tilbage, og fejlen ville ikke være synlig noget sted.
    """
    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT file_id FROM ingested_files WHERE file_hash = :h"),
            {"h": file_hash},
        ).fetchone()
        if existing is not None:
            conn.execute(
                text("UPDATE ingested_files SET status = 'FAILED' WHERE file_id = :file_id"),
                {"file_id": existing[0]},
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO ingested_files (file_name, file_hash, first_seen_at, status)
                    VALUES (:file_name, :file_hash, :first_seen_at, 'FAILED')
                    """
                ),
                {"file_name": file_name, "file_hash": file_hash, "first_seen_at": _now_iso()},
            )


def ingest_file(engine: Engine, file_path: Path) -> int:
    """
    Læser en fil og indsætter alle rækker i raw_meter_readings, MEDMINDRE
    filens indhold allerede er set før (file-level idempotency).

    Returnerer antal indsatte rækker (0 hvis filen blev sprunget over eller
    var tom).

    ATOMICITET: claim/registrering af filen, indsættelse af raw-rækker, og
    markering som PROCESSED sker i ÉN transaktion. Hvis NOGET fejler
    undervejs, ruller alting tilbage, og filen markeres i stedet FAILED
    (se _mark_file_failed) - den efterlades ALDRIG "hængende" som PENDING
    med delvist committede raw-rækker.
    """
    file_hash = _file_hash(file_path)

    # Hurtig, IKKE-autoritativ optimering: undgå at læse/parse store filer
    # der med stor sandsynlighed allerede er færdigbehandlede. Den
    # AUTORITATIVE tjek sker inde i transaktionen i _claim_or_reuse_file.
    with engine.connect() as conn:
        precheck = conn.execute(
            text("SELECT status FROM ingested_files WHERE file_hash = :h"),
            {"h": file_hash},
        ).fetchone()
    if precheck is not None and precheck[0] == "PROCESSED":
        logger.info(
            "Springer %s over - indhold (hash=%s) er allerede indlæst",
            file_path.name, file_hash[:8],
        )
        return 0

    try:
        df = read_source_file(file_path)
        _validate_source_contract(df, file_path.name)

        ingested_at = _now_iso()
        rows = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            rows.append(
                {
                    "meter_id": row.get("meter_id"),
                    "timestamp_raw": row.get("timestamp"),
                    "consumption_liters": row.get("consumption_liters"),
                    "temperature": row.get("temperature"),
                    "status": row.get("status"),
                    "raw_payload": json.dumps(row_dict, default=str),
                    "source_file": file_path.name,
                    "ingested_at": ingested_at,
                }
            )

        with engine.begin() as conn:
            file_id, already_processed, _ = _claim_or_reuse_file(conn, file_hash, file_path.name)
            if already_processed:
                # Race mod en anden proces siden vores precheck - stadig trygt.
                return 0

            for r in rows:
                r["file_id"] = file_id

            if rows:
                insert_sql = text(
                    """
                    INSERT INTO raw_meter_readings
                        (file_id, meter_id, timestamp_raw, consumption_liters, temperature, status,
                         raw_payload, source_file, ingested_at)
                    VALUES
                        (:file_id, :meter_id, :timestamp_raw, :consumption_liters, :temperature, :status,
                         :raw_payload, :source_file, :ingested_at)
                    """
                )
                conn.execute(insert_sql, rows)

            conn.execute(
                text(
                    """
                    UPDATE ingested_files
                    SET status = 'PROCESSED', processed_at = :processed_at, row_count = :row_count
                    WHERE file_id = :file_id
                    """
                ),
                {"processed_at": _now_iso(), "row_count": len(rows), "file_id": file_id},
            )

        logger.info("Ingested %d rows from %s", len(rows), file_path.name)
        return len(rows)

    except Exception:
        logger.exception("Ingestion af %s fejlede - markerer FAILED", file_path.name)
        _mark_file_failed(engine, file_hash, file_path.name)
        raise


def ingest_directory(engine: Engine, directory: Path) -> dict:
    """
    Ingester alle .csv/.json filer i en mappe.

    Returnerer et summary-dict med:
      files_discovered, files_ingested, files_skipped, raw_rows_ingested
    """
    files: Iterable[Path] = sorted(
        list(directory.glob("*.csv")) + list(directory.glob("*.json"))
    )

    files_discovered = 0
    files_ingested = 0
    files_skipped = 0
    raw_rows_ingested = 0

    for file_path in files:
        files_discovered += 1
        rows = ingest_file(engine, file_path)
        if rows > 0:
            files_ingested += 1
            raw_rows_ingested += rows
        else:
            files_skipped += 1

    return {
        "files_discovered": files_discovered,
        "files_ingested": files_ingested,
        "files_skipped": files_skipped,
        "raw_rows_ingested": raw_rows_ingested,
    }

