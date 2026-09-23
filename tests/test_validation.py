"""
test_validation.py
-------------------
Tester de vigtigste valideringsregler i validation.py.
"""

import pandas as pd

from src.validation import validate_and_load_staging


def _raw_row(raw_id, meter_id, timestamp_raw, consumption, temperature="14.0",
             status="OK", source_file="test.csv"):
    return {
        "raw_id": raw_id,
        "meter_id": meter_id,
        "timestamp_raw": timestamp_raw,
        "consumption_liters": consumption,
        "temperature": temperature,
        "status": status,
        "source_file": source_file,
    }


def test_negative_consumption_is_rejected(engine):
    raw_df = pd.DataFrame([
        _raw_row(1, "M-001", "2026-09-20T08:00:00", "-10.0"),
    ])

    summary = validate_and_load_staging(engine, raw_df)

    assert summary["accepted"] == 0
    assert summary["rejected"] == 1

    with engine.connect() as conn:
        errors = conn.exec_driver_sql(
            "SELECT error_type FROM data_quality_errors"
        ).fetchall()
    assert errors[0][0] == "NEGATIVE_CONSUMPTION"


def test_missing_meter_id_is_rejected(engine):
    raw_df = pd.DataFrame([
        _raw_row(1, None, "2026-09-20T08:00:00", "50.0"),
    ])

    summary = validate_and_load_staging(engine, raw_df)

    assert summary["accepted"] == 0
    assert summary["rejected"] == 1


def test_invalid_timestamp_is_rejected(engine):
    raw_df = pd.DataFrame([
        _raw_row(1, "M-001", "not-a-date", "50.0"),
    ])

    summary = validate_and_load_staging(engine, raw_df)

    assert summary["accepted"] == 0
    assert summary["rejected"] == 1


def test_valid_record_is_accepted(engine):
    raw_df = pd.DataFrame([
        _raw_row(1, "M-001", "2026-09-20T08:00:00", "50.0"),
    ])

    summary = validate_and_load_staging(engine, raw_df)

    assert summary["accepted"] == 1
    assert summary["rejected"] == 0

    with engine.connect() as conn:
        rows = conn.exec_driver_sql(
            "SELECT meter_id, consumption_liters FROM stg_meter_readings"
        ).fetchall()
    assert rows[0][0] == "M-001"
    assert rows[0][1] == 50.0


def test_duplicate_record_is_not_inserted_twice(engine):
    """
    Idempotency-test: samme (meter_id, timestamp) valideret to gange
    (fx fordi samme fil blev indlæst to gange) må kun give én staging-række.
    """
    raw_df = pd.DataFrame([
        _raw_row(1, "M-001", "2026-09-20T08:00:00", "50.0"),
    ])

    validate_and_load_staging(engine, raw_df)
    # Kør igen med "ny" raw_id, men samme meter_id + timestamp
    raw_df_again = pd.DataFrame([
        _raw_row(2, "M-001", "2026-09-20T08:00:00", "50.0"),
    ])
    summary = validate_and_load_staging(engine, raw_df_again)

    # INSERT OR IGNORE betyder rowcount for den anden insert er 0
    assert summary["accepted"] == 0

    with engine.connect() as conn:
        count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM stg_meter_readings"
        ).fetchone()[0]
    assert count == 1
