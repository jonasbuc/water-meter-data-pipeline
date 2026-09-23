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
    Ugyldige rækker logges i data_quality_errors.

    Returnerer et dict med tælleres til logging: {read, accepted, rejected}.
    """
    accepted_rows = []
    rejected_rows = []
    seen_keys_in_batch = set()  # fanger dubletter INDEN for samme batch

    processed_at = _now_iso()

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

        # --- Regel 4: dublet inden for samme batch ---
        key = (meter_id, parsed_ts)
        if key in seen_keys_in_batch:
            rejected_rows.append(
                _make_error(raw_id, meter_id, parsed_ts, "DUPLICATE_IN_BATCH",
                            "Samme meter_id + timestamp optræder to gange i denne batch",
                            source_file, processed_at)
            )
            continue
        seen_keys_in_batch.add(key)

        temperature = _parse_float(row["temperature"])

        accepted_rows.append(
            {
                "meter_id": meter_id,
                "reading_timestamp": parsed_ts,
                "consumption_liters": consumption,
                "temperature": temperature,
                "status": row["status"],
                "source_file": source_file,
                "processed_at": processed_at,
            }
        )

    inserted_count = _insert_staging_rows(engine, accepted_rows)
    _insert_error_rows(engine, rejected_rows)

    return {
        "read": len(raw_df),
        "accepted": inserted_count,
        "rejected": len(rejected_rows),
    }


def _make_error(raw_id, meter_id, reading_timestamp, error_type, error_detail,
                 source_file, detected_at) -> dict:
    return {
        "raw_id": int(raw_id) if raw_id is not None else None,
        "meter_id": meter_id,
        "reading_timestamp": reading_timestamp,
        "error_type": error_type,
        "error_detail": error_detail,
        "source_file": source_file,
        "detected_at": detected_at,
    }


def _insert_staging_rows(engine: Engine, rows: list) -> int:
    """
    Indsætter accepterede rækker i staging.

    Bruger 'INSERT OR IGNORE' (SQLite-specifik syntaks) til at håndhæve
    idempotency: hvis (meter_id, reading_timestamp) allerede findes fra en
    tidligere kørsel, springes rækken stille over i stedet for at fejle.

    SQL Server-ækvivalent: MERGE-statement, eller
    "INSERT ... WHERE NOT EXISTS (...)".
    """
    if not rows:
        return 0

    insert_sql = text(
        """
        INSERT OR IGNORE INTO stg_meter_readings
            (meter_id, reading_timestamp, consumption_liters, temperature,
             status, source_file, processed_at)
        VALUES
            (:meter_id, :reading_timestamp, :consumption_liters, :temperature,
             :status, :source_file, :processed_at)
        """
    )
    with engine.begin() as conn:
        result = conn.execute(insert_sql, rows)
        return result.rowcount if result.rowcount is not None else len(rows)


def _insert_error_rows(engine: Engine, rows: list) -> None:
    if not rows:
        return
    insert_sql = text(
        """
        INSERT INTO data_quality_errors
            (raw_id, meter_id, reading_timestamp, error_type, error_detail,
             source_file, detected_at)
        VALUES
            (:raw_id, :meter_id, :reading_timestamp, :error_type, :error_detail,
             :source_file, :detected_at)
        """
    )
    with engine.begin() as conn:
        conn.execute(insert_sql, rows)
