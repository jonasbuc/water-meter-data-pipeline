"""
test_transformation.py
-----------------------
Tester transformation af staging-data til fact/dimension-modellen.
"""

import pandas as pd

from src.transformation import transform_staging_to_fact, SUSPICIOUS_THRESHOLD_LITERS
from tests.conftest import insert_staging_row


def _staging_row(stg_id, meter_id, reading_timestamp, consumption_liters, temperature=14.0,
                  status="OK"):
    return {
        "stg_id": stg_id,
        "meter_id": meter_id,
        "reading_timestamp": reading_timestamp,
        "consumption_liters": consumption_liters,
        "temperature": temperature,
        "status": status,
    }


def test_new_meter_creates_dimension_row(engine):
    insert_staging_row(engine, 1, 1, "M-001", "2026-09-20T08:00:00")
    staging_df = pd.DataFrame([
        _staging_row(1, "M-001", "2026-09-20T08:00:00", 50.0),
    ])

    transform_staging_to_fact(engine, staging_df)

    with engine.connect() as conn:
        meters = conn.exec_driver_sql("SELECT meter_id FROM dim_meter").fetchall()
    assert meters[0][0] == "M-001"


def test_high_consumption_is_flagged_suspicious(engine):
    insert_staging_row(engine, 1, 1, "M-001", "2026-09-20T08:00:00")
    staging_df = pd.DataFrame([
        _staging_row(1, "M-001", "2026-09-20T08:00:00", SUSPICIOUS_THRESHOLD_LITERS + 1),
    ])

    transform_staging_to_fact(engine, staging_df)

    with engine.connect() as conn:
        flag = conn.exec_driver_sql(
            "SELECT is_suspicious FROM fact_water_consumption"
        ).fetchone()[0]
    assert flag == 1


def test_duplicate_fact_not_inserted_twice(engine):
    insert_staging_row(engine, 1, 1, "M-001", "2026-09-20T08:00:00")
    staging_df = pd.DataFrame([
        _staging_row(1, "M-001", "2026-09-20T08:00:00", 50.0),
    ])

    transform_staging_to_fact(engine, staging_df)
    inserted_second_time = transform_staging_to_fact(engine, staging_df)

    assert inserted_second_time == 0

    with engine.connect() as conn:
        count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM fact_water_consumption"
        ).fetchone()[0]
    assert count == 1


def test_fact_retains_lineage_to_staging_row(engine):
    """Fact -> staging lineage: fact.source_stg_id skal pege på stg_id."""
    insert_staging_row(engine, 42, 1, "M-001", "2026-09-20T08:00:00")
    staging_df = pd.DataFrame([
        _staging_row(42, "M-001", "2026-09-20T08:00:00", 50.0),
    ])

    transform_staging_to_fact(engine, staging_df)

    with engine.connect() as conn:
        source_stg_id = conn.exec_driver_sql(
            "SELECT source_stg_id FROM fact_water_consumption"
        ).fetchone()[0]
    assert source_stg_id == 42


def test_late_arriving_reading_updates_dimension_min_max(engine):
    """
    EVENT TIME vs PROCESSING TIME: selvom en tidligere (event time) måling
    ankommer/behandles SENERE (processing time), skal first_reading_at
    stadig blive korrekt via MIN-semantik.
    """
    insert_staging_row(engine, 1, 1, "M-001", "2026-09-21T10:00:00")
    first_batch = pd.DataFrame([
        _staging_row(1, "M-001", "2026-09-21T10:00:00", 50.0),
    ])
    transform_staging_to_fact(engine, first_batch)

    # Denne måling er ÆLDRE (event time), men behandles i en SENERE kørsel.
    insert_staging_row(engine, 2, 2, "M-001", "2026-09-20T10:00:00")
    late_batch = pd.DataFrame([
        _staging_row(2, "M-001", "2026-09-20T10:00:00", 40.0),
    ])
    transform_staging_to_fact(engine, late_batch)

    with engine.connect() as conn:
        first_reading_at, last_reading_at = conn.exec_driver_sql(
            "SELECT first_reading_at, last_reading_at FROM dim_meter WHERE meter_id = 'M-001'"
        ).fetchone()

    assert first_reading_at.startswith("2026-09-20")
    assert last_reading_at.startswith("2026-09-21")
