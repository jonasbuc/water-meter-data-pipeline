"""
validation.py
-------------
Formål: flytte data fra RAW til STAGING, med validering undervejs.

Alt der IKKE består valideringen havner i data_quality_errors i stedet for
bare at blive smidt væk. Princip: "Bad data should be observable, not
silently discarded."

Valideringsregler (jf. opgavebeskrivelsen):
- meter_id må ikke være NULL/tom
- timestamp skal kunne parses (ISO8601 eller lignende)
- consumption_liters må ikke være negativ, og skal kunne parses som tal
- duplicate (meter_id, timestamp) inden for samme validerings-batch afvises
  (databasens UNIQUE constraint fanger dubletter på tværs af kørsler)
- ekstremt høje målinger markeres IKKE som fejl, men flages som "suspicious"
  længere nede i pipeline (i transformation/fact) - de er stadig gyldig data,
  bare usædvanlig. Det er en vigtig skelnen: ugyldig data != usædvanlig data.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_timestamp(raw_value: Optional[str]) -> Optional[str]:
    """Forsøger at parse en rå timestamp-streng til ISO8601.
    Returnerer None hvis parsing fejler (fanges af kalderen som fejl)."""
    if raw_value is None or str(raw_value).strip() in ("", "nan", "None"):
        return None
    try:
        # pandas' to_datetime er tolerant over for flere formater
        parsed = pd.to_datetime(raw_value, utc=True)
        return parsed.isoformat()
    except (ValueError, TypeError):
        return None


def _parse_float(raw_value: Optional[str]) -> Optional[float]:
    """Forsøger at parse en rå streng til float. Returnerer None ved fejl."""
    if raw_value is None or str(raw_value).strip() in ("", "nan", "None"):
        return None
    try:
        return float(raw_value)
    except (ValueError, TypeError):
        return None


def _is_blank(raw_value: Optional[str]) -> bool:
    """Afgør om en rå værdi er 'tom' (manglende), modsat 'til stede men ugyldig'."""
    return raw_value is None or str(raw_value).strip() in ("", "nan", "None")


def _parse_temperature(raw_value: Optional[str]) -> tuple:
    """
    Temperatur er et OPTIONALT felt, men vi skelner eksplicit mellem:
      - manglende/tom værdi   -> accepteres stille som NULL (ikke en fejl)
      - til stede, men ikke parsbar -> kernemålingen kan stadig accepteres,
        men vi registrerer en WARNING (rækken er ikke forkert, bare ufuldstændig)

    Returnerer (value: float|None, warning_detail: str|None).
    """
    if raw_value is None or _is_blank(raw_value):
        return None, None
    try:
        return float(str(raw_value)), None
    except (ValueError, TypeError):
        return None, f"Kunne ikke parse temperature: {raw_value!r}"


def fetch_unprocessed_raw(engine: Engine, since_raw_id: int = 0) -> pd.DataFrame:
    """
    Henter raw-rækker med raw_id > since_raw_id.

    Dette ER vores incremental-load-mekanisme for raw -> staging trinnet:
    vi husker den sidst behandlede raw_id (via pipeline_runs) og henter
    kun nyere rækker næste gang, i stedet for at genbehandle hele raw-tabellen.
    """
    query = text(
        "SELECT * FROM raw_meter_readings WHERE raw_id > :since ORDER BY raw_id"
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"since": since_raw_id})
    return df


def validate_and_load_staging(engine: Engine, raw_df: pd.DataFrame) -> dict:
    """
    Validerer rækker fra raw_df og indsætter gyldige rækker i stg_meter_readings.
    Ugyldige rækker logges i data_quality_errors (severity=ERROR).
    Accepterede rækker med en mindre kvalitetsbemærkning (fx ikke-parsbar
    temperatur) logges også, men med severity=WARNING - rækken forkastes IKKE.

    Transaktionsgrænse (atomicity): staging-indsættelse og fejl/warning-
    indsættelse sker i ÉN transaktion (ét kald til engine.begin()). De hører
    logisk sammen som ét pipeline-trin ("RAW -> STAGING"), så enten committer
    begge dele, eller ingen af dem gør (rollback ved exception).

    Dubletter (samme meter_id+reading_timestamp) skelnes eksplicit:
      - DUPLICATE_IN_BATCH:   optræder to gange i SAMME batch -> reel
        data-kvalitetsfejl (severity=ERROR), da kilden har sendt samme
        måling to gange i én fil.
      - DUPLICATE_EXISTING:   findes allerede i staging fra en TIDLIGERE
        kørsel. Dette er IKKE en datakvalitetsfejl - det er forventet
        operationel adfærd når fx en fil delvist er behandlet før, eller
        watermarks overlapper. Den tælles i `duplicates_skipped` i stedet
        for at blive gemt i data_quality_errors, fordi den ikke fortæller
        noget om dataens kvalitet - kun at pipelinen allerede har set den.

    Returnerer et summary-dict: {read, accepted, rejected, duplicates_skipped}.
    """
    accepted_rows = []
    rejected_rows = []  # severity=ERROR -> rækken er IKKE i staging
    warning_rows = []   # severity=WARNING -> rækken ER i staging
    seen_keys_in_batch = set()  # fanger dubletter INDEN for samme batch
    duplicates_skipped = 0

    processed_at = _now_iso()

    # --- Skalerbarhed: batch-scoped opslag i stedet for fuld tabel-scan ---
    # Tidligere hentede vi ALLE (meter_id, reading_timestamp)-par fra hele
    # stg_meter_readings for hver eneste validerings-kørsel. Det betyder at
    # arbejdet skalerede med den TOTALE historiske staging-størrelse, ikke
    # med den aktuelle batch - jo mere historik, jo langsommere blev hver
    # ny (lille) batch valideret.
    #
    # I stedet bygger vi først en liste af KANDIDAT-nøgler ud fra selve
    # raw_df'en (de meter_id'er der rent faktisk optræder i denne batch),
    # og slår KUN dem op i staging. Det gør arbejdet proportionalt med
    # batch-størrelsen, ikke hele tabellen - uden at ofre læsbarhed eller
    # indføre N+1 forespørgsler (det er stadig ét enkelt SELECT).
    candidate_meter_ids = [
        m for m in raw_df["meter_id"].dropna().unique().tolist()
        if str(m).strip() not in ("", "nan", "None")
    ]

    with engine.begin() as conn:
        if candidate_meter_ids:
            placeholders = ", ".join(f":m{i}" for i in range(len(candidate_meter_ids)))
            params = {f"m{i}": mid for i, mid in enumerate(candidate_meter_ids)}
            existing_keys = set(
                tuple(r) for r in conn.execute(
                    text(
                        f"SELECT meter_id, reading_timestamp FROM stg_meter_readings "
                        f"WHERE meter_id IN ({placeholders})"
                    ),
                    params,
                ).fetchall()
            )
        else:
            existing_keys = set()

        for _, row in raw_df.iterrows():
            raw_id = row["raw_id"]
            meter_id = row["meter_id"]
            source_file = row["source_file"]

            # --- Regel 1: meter_id må ikke mangle ---
            if meter_id is None or str(meter_id).strip() in ("", "nan", "None"):
                rejected_rows.append(
                    _make_error(raw_id, meter_id, None, "MISSING_METER_ID",
                                "meter_id er tom eller NULL", source_file, processed_at)
                )
                continue

            # --- Regel 2: timestamp skal kunne parses ---
            parsed_ts = _parse_timestamp(row["timestamp_raw"])
            if parsed_ts is None:
                rejected_rows.append(
                    _make_error(raw_id, meter_id, row["timestamp_raw"], "INVALID_TIMESTAMP",
                                f"Kunne ikke parse timestamp: {row['timestamp_raw']!r}",
                                source_file, processed_at)
                )
                continue

            # --- Regel 3: consumption skal kunne parses og må ikke være negativ ---
            consumption = _parse_float(row["consumption_liters"])
            if consumption is None:
                rejected_rows.append(
                    _make_error(raw_id, meter_id, parsed_ts, "INVALID_CONSUMPTION",
                                f"Kunne ikke parse consumption_liters: {row['consumption_liters']!r}",
                                source_file, processed_at)
                )
                continue
            if consumption < 0:
                rejected_rows.append(
                    _make_error(raw_id, meter_id, parsed_ts, "NEGATIVE_CONSUMPTION",
                                f"consumption_liters er negativ: {consumption}",
                                source_file, processed_at)
                )
                continue

            key = (meter_id, parsed_ts)

            # --- Regel 4a: dublet inden for samme batch -> reel datakvalitetsfejl ---
            if key in seen_keys_in_batch:
                rejected_rows.append(
                    _make_error(raw_id, meter_id, parsed_ts, "DUPLICATE_IN_BATCH",
                                "Samme meter_id + timestamp optræder to gange i denne batch",
                                source_file, processed_at)
                )
                continue

            # --- Regel 4b: dublet ift. allerede committet staging -> operationel, ikke fejl ---
            if key in existing_keys:
                duplicates_skipped += 1
                continue

            seen_keys_in_batch.add(key)

            # --- Regel 5: temperatur er optional. Skeln mangler vs. ugyldig ---
            temperature, temp_warning = _parse_temperature(row["temperature"])
            if temp_warning:
                warning_rows.append(
                    _make_error(raw_id, meter_id, parsed_ts, "MALFORMED_TEMPERATURE",
                                temp_warning, source_file, processed_at, severity="WARNING")
                )

            accepted_rows.append(
                {
                    "raw_id": int(raw_id),
                    "meter_id": meter_id,
                    "reading_timestamp": parsed_ts,
                    "consumption_liters": consumption,
                    "temperature": temperature,
                    "status": row["status"],
                    "source_file": source_file,
                    "processed_at": processed_at,
                }
            )

        inserted_count = _insert_staging_rows(conn, accepted_rows)
        _insert_error_rows(conn, rejected_rows + warning_rows)

    return {
        "read": len(raw_df),
        "accepted": inserted_count,
        "rejected": len(rejected_rows),
        "duplicates_skipped": duplicates_skipped,
    }


def _make_error(raw_id, meter_id, reading_timestamp, error_type, error_detail,
                 source_file, detected_at, severity: str = "ERROR") -> dict:
    return {
        "raw_id": int(raw_id) if raw_id is not None else None,
        "meter_id": meter_id,
        "reading_timestamp": reading_timestamp,
        "error_type": error_type,
        "severity": severity,
        "error_detail": error_detail,
        "source_file": source_file,
        "detected_at": detected_at,
    }


def _insert_staging_rows(conn, rows: list) -> int:
    """
    Indsætter accepterede rækker i staging (på en overdraget Connection, så
    dette er en del af kalderens transaktion - se validate_and_load_staging).

    Bruger 'INSERT OR IGNORE' (SQLite-specifik syntaks) som SIDSTE
    sikkerhedsnet mod dubletter (database-uniqueness). Applikationskoden
    ovenfor har allerede filtreret DUPLICATE_IN_BATCH og DUPLICATE_EXISTING
    fra, så denne constraint bør normalt ikke ramme noget her - men den
    beskytter mod race conditions og fremtidige kode-ændringer.

    SQL Server-ækvivalent: MERGE-statement, eller
    "INSERT ... WHERE NOT EXISTS (...)".
    """
    if not rows:
        return 0

    insert_sql = text(
        """
        INSERT OR IGNORE INTO stg_meter_readings
            (raw_id, meter_id, reading_timestamp, consumption_liters, temperature,
             status, source_file, processed_at)
        VALUES
            (:raw_id, :meter_id, :reading_timestamp, :consumption_liters, :temperature,
             :status, :source_file, :processed_at)
        """
    )
    result = conn.execute(insert_sql, rows)
    return result.rowcount if result.rowcount is not None else len(rows)


def _insert_error_rows(conn, rows: list) -> None:
    if not rows:
        return
    insert_sql = text(
        """
        INSERT INTO data_quality_errors
            (raw_id, meter_id, reading_timestamp, error_type, severity, error_detail,
             source_file, detected_at)
        VALUES
            (:raw_id, :meter_id, :reading_timestamp, :error_type, :severity, :error_detail,
             :source_file, :detected_at)
        """
    )
    conn.execute(insert_sql, rows)
