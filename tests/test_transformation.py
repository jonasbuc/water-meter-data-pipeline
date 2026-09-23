"""
test_transformation.py
-----------------------
Tester transformation af staging-data til fact/dimension-modellen.
"""

import pandas as pd

from src.transformation import transform_staging_to_fact, SUSPICIOUS_THRESHOLD_LITERS


def _staging_row(meter_id, reading_timestamp, consumption_liters, temperature=14.0,
                  status="OK"):
    return {
        "meter_id": meter_id,
        "reading_timestamp": reading_timestamp,
        "consumption_liters": consumption_liters,
        "temperature": temperature,
        "status": status,
    }


def test_new_meter_creates_dimension_row(engine):
    staging_df = pd.DataFrame([
        _staging_row("M-001", "2026-09-20T08:00:00", 50.0),
    ])

    transform_staging_to_fact(engine, staging_df)

    with engine.connect() as conn:
        meters = conn.exec_driver_sql("SELECT meter_id FROM dim_meter").fetchall()
    assert meters[0][0] == "M-001"


def test_high_consumption_is_flagged_suspicious(engine):
    staging_df = pd.DataFrame([
        _staging_row("M-001", "2026-09-20T08:00:00", SUSPICIOUS_THRESHOLD_LITERS + 1),
    ])

    transform_staging_to_fact(engine, staging_df)

    with engine.connect() as conn:
        flag = conn.exec_driver_sql(
            "SELECT is_suspicious FROM fact_water_consumption"
        ).fetchone()[0]
    assert flag == 1


def test_duplicate_fact_not_inserted_twice(engine):
    staging_df = pd.DataFrame([
        _staging_row("M-001", "2026-09-20T08:00:00", 50.0),
    ])

    transform_staging_to_fact(engine, staging_df)
    inserted_second_time = transform_staging_to_fact(engine, staging_df)

    assert inserted_second_time == 0

    with engine.connect() as conn:
        count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM fact_water_consumption"
        ).fetchone()[0]
    assert count == 1
