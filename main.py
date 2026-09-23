"""
main.py
-------
Indgangspunkt til at køre hele pipeline'en manuelt.

Kør med:  python main.py
"""

import argparse
import logging
from pathlib import Path

from sqlalchemy import text

from src.database import get_engine, init_db
from src.pipeline import run_pipeline

RAW_DATA_DIR = Path(__file__).resolve().parent / "data" / "raw"
DEMO_DB_PATH = Path(__file__).resolve().parent / "data" / "demo.db"


def configure_logging() -> None:
    """
    Simpel logging-opsætning. I et rigtigt produktionssystem ville dette
    typisk gå til en fil eller et centralt logsystem (fx CloudWatch, ELK),
    men til dette projekt er stdout tilstrækkeligt og nemt at følge med i.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)

    engine = get_engine()
    init_db(engine)

    logger.info("Starter pipeline med kildemappe: %s", RAW_DATA_DIR)
    summary = run_pipeline(engine, RAW_DATA_DIR)

    print("\n--- Pipeline Summary ---")
    for key, value in summary.items():
        print(f"{key}: {value}")


def run_demo() -> None:
    """
    Selvstændig demo: kører hele pipelinen mod en FRISK demo-database
    (data/demo.db, slettes og genskabes ved hver kørsel) og printer et
    kort, læsbart overblik - beregnet til at kunne vises på under et
    minut uden nogen UI.
    """
    configure_logging()
    logger = logging.getLogger(__name__)

    if DEMO_DB_PATH.exists():
        DEMO_DB_PATH.unlink()

    engine = get_engine(db_path=DEMO_DB_PATH)
    init_db(engine)

    logger.info("Demo: kører pipeline mod frisk database %s", DEMO_DB_PATH)
    summary = run_pipeline(engine, RAW_DATA_DIR)

    with engine.connect() as conn:
        raw_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM raw_meter_readings"
        ).scalar()
        staging_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM stg_meter_readings"
        ).scalar()
        fact_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM fact_water_consumption"
        ).scalar()
        error_row = conn.exec_driver_sql(
            """
            SELECT
                SUM(CASE WHEN severity = 'ERROR' THEN 1 ELSE 0 END),
                SUM(CASE WHEN severity = 'WARNING' THEN 1 ELSE 0 END)
            FROM data_quality_errors
            """
        ).fetchone()
        error_count = error_row[0] if error_row else 0
        warning_count = error_row[1] if error_row else 0
        suspicious_count = conn.exec_driver_sql(
            "SELECT COUNT(*) FROM fact_water_consumption WHERE is_suspicious = 1"
        ).scalar()
        last_run = conn.exec_driver_sql(
            """
            SELECT run_id, status, start_time, end_time, failed_stage, error_type
            FROM pipeline_runs ORDER BY run_id DESC LIMIT 1
            """
        ).fetchone()
        lineage_example = conn.execute(
            text(
                """
                SELECT f.fact_id, f.meter_key, f.reading_timestamp,
                       s.stg_id, r.raw_id, r.source_file
                FROM fact_water_consumption f
                JOIN stg_meter_readings s ON s.stg_id = f.source_stg_id
                JOIN raw_meter_readings r ON r.raw_id = s.raw_id
                LIMIT 1
                """
            )
        ).fetchone()

    print("\n=== DEMO: Vandmåler Data Pipeline ===")
    print(f"Database: {DEMO_DB_PATH}")
    print("\n--- Rækker pr. lag ---")
    print(f"raw_meter_readings:       {raw_count}")
    print(f"stg_meter_readings:       {staging_count}")
    print(f"fact_water_consumption:   {fact_count}")
    print("\n--- Datakvalitet ---")
    print(f"ERROR (afvist):           {error_count or 0}")
    print(f"WARNING (accepteret):     {warning_count or 0}")
    print(f"Mistænkelige målinger:    {suspicious_count}")
    print("\n--- Seneste pipeline-kørsel ---")
    if last_run:
        print(f"run_id={last_run[0]} status={last_run[1]} "
              f"start={last_run[2]} end={last_run[3]} "
              f"failed_stage={last_run[4]} error_type={last_run[5]}")
    print("\n--- Lineage-eksempel (fact -> staging -> raw) ---")
    if lineage_example:
        print(f"fact_id={lineage_example[0]} meter_key={lineage_example[1]} "
              f"timestamp={lineage_example[2]} -> stg_id={lineage_example[3]} "
              f"-> raw_id={lineage_example[4]} (kilde: {lineage_example[5]})")
    else:
        print("(ingen fact-rækker at spore endnu)")
    print("\n--- Pipeline Summary ---")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vandmåler Data Pipeline")
    parser.add_argument(
        "--demo", action="store_true",
        help="Kør en selvstændig demo mod en frisk data/demo.db og print et overblik",
    )
    args = parser.parse_args()

    if args.demo:
        run_demo()
    else:
        main()
