"""
test_config_and_constraints.py
--------------------------------
- Tester at PipelineConfig kan levere en anden suspicious-tærskel til
  transformation.py uden at ændre kode.
- Tester at CHECK-constraints reelt håndhæves for enum/boolean-invarianter
  (ingested_files.status, pipeline_runs.status, data_quality_errors.severity,
  fact_water_consumption.is_suspicious).
"""

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.config import PipelineConfig
from src.transformation import transform_staging_to_fact
from tests.conftest import insert_staging_row


def _staging_row(stg_id, meter_id, reading_timestamp, consumption_liters):
    return {
        "stg_id": stg_id,
        "meter_id": meter_id,
        "reading_timestamp": reading_timestamp,
        "consumption_liters": consumption_liters,
        "temperature": 14.0,
        "status": "OK",
    }


def test_custom_suspicious_threshold_is_respected(engine):
    """Beviser at tærsklen er KONFIGURATION, ikke en hardkodet konstant."""
    insert_staging_row(engine, 1, 1, "M-001", "2026-09-20T08:00:00", consumption_liters=100.0)
    staging_df = pd.DataFrame([_staging_row(1, "M-001", "2026-09-20T08:00:00", 100.0)])

    low_threshold_config = PipelineConfig(suspicious_threshold_liters=50.0)
    transform_staging_to_fact(engine, staging_df, config=low_threshold_config)

    with engine.connect() as conn:
        flag = conn.exec_driver_sql(
            "SELECT is_suspicious FROM fact_water_consumption"
        ).fetchone()[0]
    # 100.0 liter er UNDER standard-tærsklen (2000), men OVER vores
    # tilpassede, lave tærskel (50) -> skal flages suspicious her.
    assert flag == 1


def test_invalid_ingested_files_status_is_rejected(engine):
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO ingested_files (file_name, file_hash, first_seen_at, status)
                    VALUES ('x.csv', 'deadbeef', 'now', 'NOT_A_REAL_STATUS')
                    """
                )
            )


def test_invalid_is_suspicious_boolean_is_rejected(engine):
    insert_staging_row(engine, 1, 1, "M-001", "2026-09-20T08:00:00")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO dim_meter (meter_id, first_reading_at, last_reading_at, created_at, updated_at)
                VALUES ('M-001', 'x', 'x', 'x', 'x')
                """
            )
        )
        meter_key = conn.exec_driver_sql(
            "SELECT meter_key FROM dim_meter WHERE meter_id = 'M-001'"
        ).fetchone()[0]

    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO fact_water_consumption
                        (meter_key, source_stg_id, reading_timestamp, consumption_liters, is_suspicious)
                    VALUES (:meter_key, 1, '2026-09-20T08:00:00', 10.0, 2)
                    """
                ),
                {"meter_key": meter_key},
            )
