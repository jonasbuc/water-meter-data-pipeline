"""
test_dashboard_data.py
------------------------
Fokuserede tests af dashboard/data.py's read-only forespørgsler.
Tester IKKE Streamlit-præsentation (se dashboard/app.py) - kun at
data-laget returnerer korrekte, filtrerede resultater fra kendte
fixture-data, samt at det håndterer tom database gracefully.

Bruger de samme test-helpers (insert_raw_row/insert_staging_row) som de
øvrige tests, plus lokale helpers til at indsætte dim_meter/fact-rækker,
da dashboard-laget primært læser fra analytics-laget.
"""

from datetime import date

import pandas as pd
from sqlalchemy import text

from dashboard.data import (
    DashboardFilters,
    load_filtered_fact_data,
    load_kpis,
    load_top_meters,
    load_suspicious_readings,
    load_quality_kpis,
    load_pipeline_health_kpis,
    load_lineage,
    load_meter_list,
)
from tests.conftest import insert_staging_row


def _insert_dim_meter(engine, meter_id: str) -> int:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO dim_meter (meter_id, first_reading_at, last_reading_at, created_at, updated_at)
                VALUES (:meter_id, 'x', 'x', 'x', 'x')
                """
            ),
            {"meter_id": meter_id},
        )
        return conn.execute(
            text("SELECT meter_key FROM dim_meter WHERE meter_id = :meter_id"),
            {"meter_id": meter_id},
        ).fetchone()[0]


def _insert_fact_row(engine, meter_key: int, stg_id: int, reading_timestamp: str,
                      consumption_liters: float, is_suspicious: int = 0):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO fact_water_consumption
                    (meter_key, source_stg_id, reading_timestamp, consumption_liters,
                     temperature, status, is_suspicious)
                VALUES (:meter_key, :stg_id, :reading_timestamp, :consumption_liters,
                        14.0, 'OK', :is_suspicious)
                """
            ),
            {
                "meter_key": meter_key, "stg_id": stg_id,
                "reading_timestamp": reading_timestamp,
                "consumption_liters": consumption_liters,
                "is_suspicious": is_suspicious,
            },
        )


def _seed_two_meters_with_readings(engine):
    """Fælles fixture-opsætning: to målere, hver med to målinger på
    forskellige datoer, én markeret suspicious."""
    insert_staging_row(engine, 1, 1, "M-001", "2026-09-20T08:00:00", 100.0)
    insert_staging_row(engine, 2, 2, "M-002", "2026-09-21T08:00:00", 50.0)

    meter_key_1 = _insert_dim_meter(engine, "M-001")
    meter_key_2 = _insert_dim_meter(engine, "M-002")

    _insert_fact_row(engine, meter_key_1, 1, "2026-09-20T08:00:00", 100.0, is_suspicious=0)
    _insert_fact_row(engine, meter_key_2, 2, "2026-09-21T08:00:00", 3000.0, is_suspicious=1)


def test_load_filtered_fact_data_empty_database_returns_empty_dataframe(engine):
    df = load_filtered_fact_data(engine, DashboardFilters())
    assert df.empty


def test_load_filtered_fact_data_returns_all_rows_with_no_filters(engine):
    _seed_two_meters_with_readings(engine)
    df = load_filtered_fact_data(engine, DashboardFilters())
    assert len(df) == 2
    assert set(df["meter_id"]) == {"M-001", "M-002"}


def test_load_filtered_fact_data_meter_filter(engine):
    _seed_two_meters_with_readings(engine)
    df = load_filtered_fact_data(engine, DashboardFilters(meter_ids=["M-001"]))
    assert len(df) == 1
    assert df.iloc[0]["meter_id"] == "M-001"


def test_load_filtered_fact_data_date_range_filter(engine):
    _seed_two_meters_with_readings(engine)
    df = load_filtered_fact_data(
        engine, DashboardFilters(start_date=date(2026, 9, 21), end_date=date(2026, 9, 21))
    )
    assert len(df) == 1
    assert df.iloc[0]["meter_id"] == "M-002"


def test_load_filtered_fact_data_suspicious_filter(engine):
    _seed_two_meters_with_readings(engine)
    suspicious_df = load_filtered_fact_data(engine, DashboardFilters(suspicious_only=True))
    normal_df = load_filtered_fact_data(engine, DashboardFilters(suspicious_only=False))
    assert len(suspicious_df) == 1 and suspicious_df.iloc[0]["meter_id"] == "M-002"
    assert len(normal_df) == 1 and normal_df.iloc[0]["meter_id"] == "M-001"


def test_load_kpis_on_empty_database_does_not_crash(engine):
    kpis = load_kpis(engine, DashboardFilters())
    assert kpis["total_consumption_liters"] == 0.0
    assert kpis["active_meters"] == 0
    assert kpis["data_quality_rate"] is None


def test_load_kpis_computes_expected_totals(engine):
    _seed_two_meters_with_readings(engine)
    kpis = load_kpis(engine, DashboardFilters())
    assert kpis["total_consumption_liters"] == 3100.0
    assert kpis["active_meters"] == 2
    assert kpis["suspicious_readings"] == 1


def test_load_top_meters_orders_by_total_descending(engine):
    _seed_two_meters_with_readings(engine)
    df = load_top_meters(engine, DashboardFilters(), limit=10)
    assert list(df["meter_id"]) == ["M-002", "M-001"]


def test_load_suspicious_readings_only_returns_flagged_rows(engine):
    _seed_two_meters_with_readings(engine)
    df = load_suspicious_readings(engine, DashboardFilters())
    assert len(df) == 1
    assert df.iloc[0]["meter_id"] == "M-002"


def test_load_suspicious_readings_empty_when_none_flagged(engine):
    insert_staging_row(engine, 1, 1, "M-001", "2026-09-20T08:00:00", 10.0)
    meter_key = _insert_dim_meter(engine, "M-001")
    _insert_fact_row(engine, meter_key, 1, "2026-09-20T08:00:00", 10.0, is_suspicious=0)
    df = load_suspicious_readings(engine, DashboardFilters())
    assert df.empty


def test_load_quality_kpis_on_empty_database(engine):
    kpis = load_quality_kpis(engine)
    assert kpis["total_issues"] == 0
    assert kpis["errors"] == 0


def test_load_pipeline_health_kpis_on_empty_database(engine):
    kpis = load_pipeline_health_kpis(engine)
    assert kpis["last_run_status"] is None


def test_load_lineage_returns_none_for_unknown_fact_id(engine):
    assert load_lineage(engine, 999) is None


def test_load_lineage_follows_full_chain(engine):
    _seed_two_meters_with_readings(engine)
    with engine.connect() as conn:
        fact_id = conn.execute(
            text("SELECT fact_id FROM fact_water_consumption LIMIT 1")
        ).fetchone()[0]
    lineage = load_lineage(engine, fact_id)
    assert lineage is not None
    assert lineage["stg_id"] is not None
    assert lineage["raw_id"] is not None


def test_load_meter_list_empty_when_no_meters(engine):
    assert load_meter_list(engine) == []
