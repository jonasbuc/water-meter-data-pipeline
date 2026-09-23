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
    assert first_summary["staging_rows_inserted"] == 1
    assert first_summary["facts_inserted"] == 1

    # Kør igen med SAMME fil (uændret indhold) stadig liggende i mappen.
    # File-level idempotency betyder ingestion springer filen over denne
    # gang (samme hash), så raw_meter_readings vokser IKKE.
    second_summary = run_pipeline(engine, raw_dir)

    with engine.connect() as conn:
        raw_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM raw_meter_readings"
        ).fetchone()[0]
        staging_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM stg_meter_readings"
        ).fetchone()[0]
        fact_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM fact_water_consumption"
        ).fetchone()[0]

    assert raw_count == 1
    assert staging_count == 1
    assert fact_count == 1
    assert second_summary["files_skipped"] == 1
    assert second_summary["files_ingested"] == 0
    assert second_summary["status"] == "SUCCESS"


def test_changed_file_content_is_processed_as_new_version(engine, tmp_path):
    """Samme filnavn, men ændret indhold, skal behandles som en ny fil."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    source_file = raw_dir / "batch1.csv"

    source_file.write_text(
        "meter_id,timestamp,consumption_liters,temperature,status\n"
        "M-001,2026-09-20T08:00:00,50.0,14.0,OK\n"
    )
    run_pipeline(engine, raw_dir)

    source_file.write_text(
        "meter_id,timestamp,consumption_liters,temperature,status\n"
        "M-001,2026-09-20T08:00:00,50.0,14.0,OK\n"
        "M-002,2026-09-20T09:00:00,60.0,14.5,OK\n"
    )
    summary = run_pipeline(engine, raw_dir)

    assert summary["files_ingested"] == 1
    with engine.connect() as conn:
        raw_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM raw_meter_readings"
        ).fetchone()[0]
    # 1 række fra første version + 2 rækker fra anden version = 3
    assert raw_count == 3


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
            "SELECT status, raw_rows_ingested FROM pipeline_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()

    assert run_row[0] == "SUCCESS"
    assert run_row[1] == 1


def test_pipeline_recovers_after_transformation_failure(engine, tmp_path, monkeypatch):
    """
    Integration-test for failure recovery:
    ingestion + staging lykkes, transformation fejler kunstigt -> run FAILED.
    Næste (normale) kørsel skal samle de committede staging-rækker op uden
    at genindsætte raw eller duplikere data_quality_errors, og facts skal
    til sidst blive oprettet.
    """
    import src.pipeline as pipeline_module

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "batch1.csv").write_text(
        "meter_id,timestamp,consumption_liters,temperature,status\n"
        "M-001,2026-09-20T08:00:00,50.0,14.0,OK\n"
    )

    def _boom(engine, staging_df, **kwargs):
        raise RuntimeError("Simuleret transformation-fejl")

    monkeypatch.setattr(pipeline_module, "transform_staging_to_fact", _boom)

    try:
        pipeline_module.run_pipeline(engine, raw_dir)
        assert False, "Forventede at pipeline run kastede en exception"
    except RuntimeError:
        pass

    with engine.connect() as conn:
        staging_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM stg_meter_readings"
        ).fetchone()[0]
        failed_run = conn.exec_driver_sql(
            "SELECT status, failed_stage, error_type, error_message "
            "FROM pipeline_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()

    assert staging_count == 1  # staging blev committet trods senere fejl
    assert failed_run[0] == "FAILED"
    assert failed_run[1] == "TRANSFORMATION"
    assert failed_run[2] == "RuntimeError"
    assert "Simuleret transformation-fejl" in failed_run[3]

    monkeypatch.undo()  # gendan den rigtige transform_staging_to_fact

    recovery_summary = pipeline_module.run_pipeline(engine, raw_dir)

    with engine.connect() as conn:
        fact_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM fact_water_consumption"
        ).fetchone()[0]
        staging_count_after = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM stg_meter_readings"
        ).fetchone()[0]
        success_run = conn.exec_driver_sql(
            "SELECT status, failed_stage, error_type, error_message "
            "FROM pipeline_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()

    assert recovery_summary["status"] == "SUCCESS"
    assert fact_count == 1          # den staging-række der ventede blev nu transformeret
    assert staging_count_after == 1  # ingen duplikeret staging-række
    # SUCCESS-rækker må ALDRIG bære en gammel fejlbesked videre
    assert success_run[0] == "SUCCESS"
    assert success_run[1] is None
    assert success_run[2] is None
    assert success_run[3] is None
