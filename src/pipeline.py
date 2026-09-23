"""
pipeline.py
-----------
Formål: orkestrere hele flowet: ingestion -> validation -> transformation,
med logging og incremental-load-bogføring via pipeline_runs.

Incremental load-strategi her:
- Vi gemmer IKKE et globalt "sidste tidspunkt", men i stedet det højeste
  raw_id / stg_id vi har behandlet (se last_processed_raw_id/stg_id logik
  via pipeline_runs). Det er simplere og mere robust end at bruge
  timestamps, fordi raw_id/stg_id er en strengt stigende, database-genereret
  sekvens - modsat kilde-timestamps som kan komme i vilkårlig rækkefølge
  eller have ur-skævheder mellem målere.

Hvorfor ikke bruge reading_timestamp som vandmærke (watermark)?
Fordi hvis måler A sender en reading med timestamp 10:00 og måler B (som er
lidt bagud) sender en reading med timestamp 09:58, men B's record først
ankommer til systemet EFTER vi har sat watermark til 10:00, vil B's record
blive sprunget over permanent. At bruge en monoton, systemgenereret ID
(raw_id) frem for kilde-timestamp undgår dette problem.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.ingestion import ingest_directory
from src.validation import fetch_unprocessed_raw, validate_and_load_staging
from src.transformation import fetch_unprocessed_staging, transform_staging_to_fact

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_last_processed_ids(engine: Engine) -> dict:
    """
    Henter det højeste raw_id og stg_id der er blevet behandlet i tidligere,
    succesfulde kørsler. Bruges til incremental load.

    Simpel tilgang: vi gemmer highwater marks direkte som MAX(raw_id) fra
    stg_meter_readings' kilde-reference ville være mere præcist, men her
    holder vi det simpelt ved at spore det i egne kolonner i pipeline_runs.
    """
    query = text(
        """
        SELECT last_processed_raw_id, last_processed_stg_id
        FROM pipeline_runs
        WHERE status = 'SUCCESS'
        ORDER BY run_id DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query).fetchone()
    if row is None:
        return {"raw_id": 0, "stg_id": 0}
    return {"raw_id": row[0] or 0, "stg_id": row[1] or 0}


def _start_run(engine: Engine) -> int:
    insert_sql = text(
        """
        INSERT INTO pipeline_runs (start_time, status)
        VALUES (:start_time, 'RUNNING')
        """
    )
    with engine.begin() as conn:
        result = conn.execute(insert_sql, {"start_time": _now_iso()})
        return result.lastrowid


def _finish_run(engine: Engine, run_id: int, status: str, records_read: int,
                 records_inserted: int, records_rejected: int,
                 last_processed_raw_id: int, last_processed_stg_id: int) -> None:
    update_sql = text(
        """
        UPDATE pipeline_runs
        SET end_time = :end_time,
            status = :status,
            records_read = :records_read,
            records_inserted = :records_inserted,
            records_rejected = :records_rejected,
            last_processed_raw_id = :last_processed_raw_id,
            last_processed_stg_id = :last_processed_stg_id
        WHERE run_id = :run_id
        """
    )
    with engine.begin() as conn:
        conn.execute(
            update_sql,
            {
                "end_time": _now_iso(),
                "status": status,
                "records_read": records_read,
                "records_inserted": records_inserted,
                "records_rejected": records_rejected,
                "last_processed_raw_id": last_processed_raw_id,
                "last_processed_stg_id": last_processed_stg_id,
                "run_id": run_id,
            },
        )


def run_pipeline(engine: Engine, raw_data_dir: Path) -> dict:
    """
    Kører hele pipeline'en én gang:
      1. Ingester alle filer i raw_data_dir -> raw_meter_readings
      2. Validerer nye raw-rækker -> stg_meter_readings (+ data_quality_errors)
      3. Transformerer nye staging-rækker -> dim_meter / fact_water_consumption
      4. Logger et pipeline_runs-record med status og tællere

    Returnerer et summary-dict til brug i main.py / logging.
    """
    run_id = _start_run(engine)
    logger.info("Pipeline run %d startet", run_id)

    try:
        # --- Trin 1: Ingestion (altid fuld - filer flyttes/arkiveres i en
        # rigtig løsning, så vi ikke genindlæser dem; her holder vi det
        # simpelt og antager mappen kun indeholder nye filer). ---
        files_read = ingest_directory(engine, raw_data_dir)

        # --- Trin 2: Incremental validation (raw -> staging) ---
        last_ids = _get_last_processed_ids(engine)
        raw_df = fetch_unprocessed_raw(engine, since_raw_id=last_ids["raw_id"])
        validation_summary = validate_and_load_staging(engine, raw_df)

        new_last_raw_id = (
            int(raw_df["raw_id"].max()) if not raw_df.empty else last_ids["raw_id"]
        )

        # --- Trin 3: Incremental transformation (staging -> analytics) ---
        staging_df = fetch_unprocessed_staging(engine, since_stg_id=last_ids["stg_id"])
        inserted_facts = transform_staging_to_fact(engine, staging_df)

        new_last_stg_id = (
            int(staging_df["stg_id"].max()) if not staging_df.empty else last_ids["stg_id"]
        )

        _finish_run(
            engine,
            run_id,
            status="SUCCESS",
            records_read=len(raw_df),
            records_inserted=inserted_facts,
            records_rejected=validation_summary["rejected"],
            last_processed_raw_id=new_last_raw_id,
            last_processed_stg_id=new_last_stg_id,
        )

        summary = {
            "run_id": run_id,
            "files_ingested_rows": files_read,
            "raw_rows_read": len(raw_df),
            "staging_accepted": validation_summary["accepted"],
            "staging_rejected": validation_summary["rejected"],
            "facts_inserted": inserted_facts,
            "status": "SUCCESS",
        }
        logger.info("Pipeline run %d fuldført: %s", run_id, summary)
        return summary

    except Exception:
        logger.exception("Pipeline run %d fejlede", run_id)
        _finish_run(
            engine,
            run_id,
            status="FAILED",
            records_read=0,
            records_inserted=0,
            records_rejected=0,
            last_processed_raw_id=0,
            last_processed_stg_id=0,
        )
        raise
