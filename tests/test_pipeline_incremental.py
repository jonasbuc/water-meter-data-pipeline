"""
test_pipeline_incremental.py
-----------------------------
Tester at incremental loading rent faktisk kun behandler nye records,
og at det er trygt at køre pipeline'en flere gange (idempotency på
pipeline-niveau).
"""

from pathlib import Path

from src.pipeline import run_pipeline


def test_second_run_with_no_new_files_processes_nothing_new(engine, tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "batch1.csv").write_text(
        "meter_id,timestamp,consumption_liters,temperature,status\n"
        "M-001,2026-09-20T08:00:00,50.0,14.0,OK\n"
    )

    first_summary = run_pipeline(engine, raw_dir)
    assert first_summary["staging_accepted"] == 1
    assert first_summary["facts_inserted"] == 1

    # Kør igen UDEN nye filer, men samme fil ligger stadig i mappen.
    # Ingestion vil læse filen igen (raw vokser), men validation/transformation
    # er incremental og idempotent, så der må ikke opstå nye staging/fact-rækker.
    second_summary = run_pipeline(engine, raw_dir)

    with engine.connect() as conn:
        staging_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM stg_meter_readings"
        ).fetchone()[0]
        fact_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM fact_water_consumption"
        ).fetchone()[0]

    assert staging_count == 1
    assert fact_count == 1
    assert second_summary["status"] == "SUCCESS"


def test_pipeline_run_is_logged(engine, tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "batch1.csv").write_text(
        "meter_id,timestamp,consumption_liters,temperature,status\n"
        "M-001,2026-09-20T08:00:00,50.0,14.0,OK\n"
    )

    run_pipeline(engine, raw_dir)

    with engine.connect() as conn:
        run_row = conn.exec_driver_sql(
            "SELECT status, records_read FROM pipeline_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()

    assert run_row[0] == "SUCCESS"
    assert run_row[1] == 1
