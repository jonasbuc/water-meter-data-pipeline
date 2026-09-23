"""
ingestion.py
------------
Formål: læse CSV/JSON-filer og indsætte dem UÆNDREDE i raw_meter_readings.

Designprincip: ingestion-laget laver INGEN forretningslogik.
- Ingen validering
- Ingen type-konvertering (alt gemmes som tekst)
- Ingen record-level dedup (det sker i staging)

Hvorfor? Fordi hvis vi en dag opdager en fejl i vores validerings-logik,
skal vi kunne re-køre validering/transformation fra raw uden at have mistet
eller ændret noget fra kilden. Dette er "bevar rådata"-princippet.

Alternativ, vi IKKE vælger: parse og validere direkte ved ingestion.
Trade-off ved det: hurtigere pipeline, men vi mister evnen til at
'replaye' historikken, og fejl i logik kan betyde permanent datatab.

FILE-LEVEL IDEMPOTENCY (nyt):
Uden dette ville raw_meter_readings vokse hver gang pipeline'en kørte,
selvom kildemappen ikke indeholder nogen nye filer (fx fordi filer
arkiveres i stedet for at blive slettet). Vi sporer allerede-indlæste
filer i `ingested_files` via SHA-256 af filens indhold - se schema.sql
for en uddybning af content- vs. filnavn-baseret idempotency.
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
    """Læser en JSON-fil (liste af objekter) ind som DataFrame, alt som string."""
    with open(file_path, "r") as f:
        records = json.load(f)
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


# Forventede kildekolonner. Vi mapper eksplicit i stedet for at stole på at
# kildefilen altid har præcis de rigtige kolonnenavne/rækkefølge.
EXPECTED_COLUMNS = [
    "meter_id",
    "timestamp",
    "consumption_liters",
    "temperature",
    "status",
]


def _register_file(engine, file_path: Path, file_hash: str) -> tuple:
    """
    Slår filens hash op i ingested_files.

    Returnerer (file_id, already_processed: bool).
    Hvis hashen ikke findes, oprettes en ny PENDING-række (samme unit of
    work som selve raw-indsættelsen håndteres af kalderen).
    """
    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT file_id, status FROM ingested_files WHERE file_hash = :h"),
            {"h": file_hash},
        ).fetchone()
        if existing is not None:
            file_id, status = existing
            return file_id, status == "PROCESSED"

        result = conn.execute(
            text(
                """
                INSERT INTO ingested_files (file_name, file_hash, first_seen_at, status)
                VALUES (:file_name, :file_hash, :first_seen_at, 'PENDING')
                """
            ),
            {
                "file_name": file_path.name,
                "file_hash": file_hash,
                "first_seen_at": _now_iso(),
            },
        )
        return result.lastrowid, False


def _mark_file_processed(engine, file_id: int, row_count: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE ingested_files
                SET status = 'PROCESSED', processed_at = :processed_at, row_count = :row_count
                WHERE file_id = :file_id
                """
            ),
            {"processed_at": _now_iso(), "row_count": row_count, "file_id": file_id},
        )


def ingest_file(engine: Engine, file_path: Path) -> int:
    """
    Læser en fil og indsætter alle rækker i raw_meter_readings, MEDMINDRE
    filens indhold allerede er set før (file-level idempotency).

    Returnerer antal indsatte rækker (0 hvis filen blev sprunget over).

    Bemærk: record-level dedup sker først i staging-laget (UNIQUE constraint
    på meter_id+timestamp). Ingestion garanterer nu FIL-level idempotency:
    samme fysiske fil (samme hash) indlæses kun én gang.
    """
    file_hash = _file_hash(file_path)
    file_id, already_processed = _register_file(engine, file_path, file_hash)

    if already_processed:
        logger.info(
            "Springer %s over - indhold (hash=%s) er allerede indlæst", file_path.name, file_hash[:8]
        )
        return 0

    df = read_source_file(file_path)

    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Fil {file_path.name} mangler kolonner: {missing_cols}")

    ingested_at = _now_iso()
    rows = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        rows.append(
            {
                "file_id": file_id,
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

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    _mark_file_processed(engine, file_id, len(rows))

    logger.info("Ingested %d rows from %s", len(rows), file_path.name)
    return len(rows)


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

