"""
test_lineage_and_constraints.py
--------------------------------
Tester nye garantier introduceret i dette review:
- SQLite foreign keys håndhæves faktisk (ikke kun defineret i schema.sql)
- data-lineage overlever RAW -> STAGING -> FACT
- fil-niveau idempotency (samme indhold indlæses ikke to gange)
- dubletter på tværs af batches er observerbare (duplicates_skipped)
- manglende vs. ugyldig temperatur giver forskellig severity
"""

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text

from src.ingestion import ingest_directory
from src.validation import fetch_unprocessed_raw, validate_and_load_staging
from src.transformation import fetch_unprocessed_staging, transform_staging_to_fact
from src.pipeline import run_pipeline
from tests.conftest import insert_raw_row, insert_staging_row


def test_foreign_key_violation_is_actually_rejected(engine):
    """
    Beviser at PRAGMA foreign_keys=ON reelt håndhæves af SQLite - ikke bare
    defineret i schema.sql. Indsætter en fact-række med et meter_key der
    ikke findes i dim_meter, og forventer at databasen afviser den.
    """
    insert_staging_row(engine, 1, 1, "M-001", "2026-09-20T08:00:00")
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO fact_water_consumption
                        (meter_key, source_stg_id, reading_timestamp, consumption_liters)
                    VALUES (999999, 1, '2026-09-20T08:00:00', 10.0)
                    """
                )
            )


def test_lineage_survives_raw_to_staging_to_fact(engine):
    """End-to-end lineage: fact.source_stg_id -> stg.stg_id, stg.raw_id -> raw.raw_id."""
    insert_raw_row(engine, 1)
    raw_df = pd.DataFrame([
        {
            "raw_id": 1,
            "meter_id": "M-001",
            "timestamp_raw": "2026-09-20T08:00:00",
            "consumption_liters": "50.0",
            "temperature": "14.0",
            "status": "OK",
            "source_file": "test.csv",
        }
    ])
    validate_and_load_staging(engine, raw_df)

    staging_df = fetch_unprocessed_staging(engine, since_stg_id=0)
    transform_staging_to_fact(engine, staging_df)

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT r.raw_id, s.stg_id, s.raw_id AS stg_raw_id, f.source_stg_id
                FROM fact_water_consumption f
                JOIN stg_meter_readings s ON s.stg_id = f.source_stg_id
                JOIN raw_meter_readings r ON r.raw_id = s.raw_id
                """
            )
        ).fetchone()

    assert row.raw_id == 1
    assert row.stg_raw_id == 1
    assert row.stg_id == row.source_stg_id


def test_same_file_ingested_twice_does_not_grow_raw(engine, tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "readings.csv").write_text(
        "meter_id,timestamp,consumption_liters,temperature,status\n"
        "M-001,2026-09-20T08:00:00,50.0,14.0,OK\n"
    )

    first = ingest_directory(engine, raw_dir)
    second = ingest_directory(engine, raw_dir)

    assert first["files_ingested"] == 1
    assert second["files_ingested"] == 0
    assert second["files_skipped"] == 1

    with engine.connect() as conn:
        raw_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM raw_meter_readings"
        ).fetchone()[0]
    assert raw_count == 1


def test_duplicate_across_batches_is_observable(engine):
    """
    Duplicate-observability: en record der allerede findes i staging fra en
    tidligere batch må ikke bare 'forsvinde' fra metrikkerne - den tælles
    eksplicit i duplicates_skipped, adskilt fra rigtige datakvalitetsfejl.
    """
    insert_raw_row(engine, 1)
    insert_raw_row(engine, 2)
    raw_df = pd.DataFrame([
        {
            "raw_id": 1, "meter_id": "M-001", "timestamp_raw": "2026-09-20T08:00:00",
            "consumption_liters": "50.0", "temperature": "14.0", "status": "OK",
            "source_file": "a.csv",
        }
    ])
    validate_and_load_staging(engine, raw_df)

    raw_df_again = pd.DataFrame([
        {
            "raw_id": 2, "meter_id": "M-001", "timestamp_raw": "2026-09-20T08:00:00",
            "consumption_liters": "50.0", "temperature": "14.0", "status": "OK",
            "source_file": "b.csv",
        }
    ])
    summary = validate_and_load_staging(engine, raw_df_again)

    assert summary["duplicates_skipped"] == 1
    assert summary["rejected"] == 0  # ikke en datakvalitetsfejl

    with engine.connect() as conn:
        error_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM data_quality_errors"
        ).fetchone()[0]
    assert error_count == 0  # duplicate-existing havner IKKE i data_quality_errors


def test_missing_temperature_is_accepted_without_warning(engine):
    insert_raw_row(engine, 1)
    raw_df = pd.DataFrame([
        {
            "raw_id": 1, "meter_id": "M-001", "timestamp_raw": "2026-09-20T08:00:00",
            "consumption_liters": "50.0", "temperature": "", "status": "OK",
            "source_file": "a.csv",
        }
    ])
    summary = validate_and_load_staging(engine, raw_df)

    assert summary["accepted"] == 1
    with engine.connect() as conn:
        warning_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM data_quality_errors WHERE severity='WARNING'"
        ).fetchone()[0]
        temp = conn.exec_driver_sql(
            "SELECT temperature FROM stg_meter_readings"
        ).fetchone()[0]
    assert warning_count == 0
    assert temp is None


def test_malformed_temperature_is_accepted_with_warning(engine):
    insert_raw_row(engine, 1)
    raw_df = pd.DataFrame([
        {
            "raw_id": 1, "meter_id": "M-001", "timestamp_raw": "2026-09-20T08:00:00",
            "consumption_liters": "50.0", "temperature": "not-a-number", "status": "OK",
            "source_file": "a.csv",
        }
    ])
    summary = validate_and_load_staging(engine, raw_df)

    # Rækken accepteres stadig - kernemålingen (consumption) er gyldig.
    assert summary["accepted"] == 1
    with engine.connect() as conn:
        warning_row = conn.exec_driver_sql(
            "SELECT error_type, severity FROM data_quality_errors"
        ).fetchone()
        temp = conn.exec_driver_sql(
            "SELECT temperature FROM stg_meter_readings"
        ).fetchone()[0]
    assert warning_row == ("MALFORMED_TEMPERATURE", "WARNING")
    assert temp is None
