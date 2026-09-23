"""
conftest.py
-----------
Delt pytest-fixture: en frisk, in-memory SQLite-database pr. test.

Hvorfor in-memory (':memory:') og ikke fil-baseret databasen?
- Tests skal være hurtige og isolerede fra hinanden og fra din rigtige
  udviklings-database (data/warehouse.db).
- Hver test får sin egen tomme database med schemaet kørt ind, så tests
  ikke kan påvirke hinanden (ingen delt state).
"""

from pathlib import Path

import pytest
from sqlalchemy import text

from src.database import get_engine, init_db, SCHEMA_PATH


@pytest.fixture
def engine():
    # get_engine() (i stedet for create_engine direkte) sikrer at
    # PRAGMA foreign_keys=ON også er slået til for test-databasen.
    test_engine = get_engine(db_path=Path(":memory:"))
    init_db(test_engine, schema_path=SCHEMA_PATH)
    return test_engine


def insert_raw_row(engine, raw_id: int, source_file: str = "test.csv") -> None:
    """
    Test-helper: indsætter en minimal raw_meter_readings-række med et
    EKSPLICIT raw_id, så unit-tests for validation/transformation kan
    fabrikere en staging/fact-DataFrame direkte uden at køre hele
    ingestion-trinnet, mens de stadig overholder FOREIGN KEY-constraints.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO raw_meter_readings
                    (raw_id, meter_id, timestamp_raw, consumption_liters, temperature,
                     status, source_file, ingested_at)
                VALUES (:raw_id, 'FIXTURE', 'x', 'x', 'x', 'OK', :source_file, 'x')
                """
            ),
            {"raw_id": raw_id, "source_file": source_file},
        )


def insert_staging_row(engine, stg_id: int, raw_id: int, meter_id: str,
                        reading_timestamp: str, consumption_liters: float = 1.0) -> None:
    """Test-helper: indsætter en minimal stg_meter_readings-række, inkl. sit
    påkrævede raw_id-fremmednøgle (opretter selv raw-rækken om nødvendigt)."""
    insert_raw_row(engine, raw_id)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO stg_meter_readings
                    (stg_id, raw_id, meter_id, reading_timestamp, consumption_liters,
                     status, source_file, processed_at)
                VALUES (:stg_id, :raw_id, :meter_id, :reading_timestamp, :consumption_liters,
                        'OK', 'test.csv', 'x')
                """
            ),
            {
                "stg_id": stg_id, "raw_id": raw_id, "meter_id": meter_id,
                "reading_timestamp": reading_timestamp, "consumption_liters": consumption_liters,
            },
        )
