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

    FAILURE RECOVERY: watermarks opdateres KUN når en kørsel ender med
    status='SUCCESS' (se _finish_run). Hvis fx transformation fejler efter
    at staging allerede er committet, forbliver last_processed_stg_id på
    den GAMLE værdi. Næste kørsel vil derfor:
      - ikke genindsætte de samme staging-rækker (UNIQUE constraint + vores
        DUPLICATE_EXISTING-tjek forhindrer det)
      - men VIL forsøge at transformere de staging-rækker igen, fordi
        watermarket for stg_id ikke blev flyttet forbi dem
    Det er netop pointen: ingen data tabes, og ingen dubletter opstår,
    fordi hvert trin er idempotent uafhængigt af de andre.
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


def _finish_run(engine: Engine, run_id: int, status: str, metrics: dict,
                 last_processed_raw_id: int, last_processed_stg_id: int) -> None:
    update_sql = text(
        """
        UPDATE pipeline_runs
        SET end_time = :end_time,
            status = :status,
            files_discovered = :files_discovered,
            files_ingested = :files_ingested,
            files_skipped = :files_skipped,
            raw_rows_ingested = :raw_rows_ingested,
            raw_rows_validated = :raw_rows_validated,
            staging_rows_inserted = :staging_rows_inserted,
            staging_rows_rejected = :staging_rows_rejected,
            duplicates_skipped = :duplicates_skipped,
            facts_inserted = :facts_inserted,
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
                "files_discovered": metrics.get("files_discovered", 0),
                "files_ingested": metrics.get("files_ingested", 0),
                "files_skipped": metrics.get("files_skipped", 0),
                "raw_rows_ingested": metrics.get("raw_rows_ingested", 0),
                "raw_rows_validated": metrics.get("raw_rows_validated", 0),
                "staging_rows_inserted": metrics.get("staging_rows_inserted", 0),
                "staging_rows_rejected": metrics.get("staging_rows_rejected", 0),
                "duplicates_skipped": metrics.get("duplicates_skipped", 0),
                "facts_inserted": metrics.get("facts_inserted", 0),
                "last_processed_raw_id": last_processed_raw_id,
                "last_processed_stg_id": last_processed_stg_id,
                "run_id": run_id,
            },
        )


def run_pipeline(engine: Engine, raw_data_dir: Path) -> dict:
    """
    Kører hele pipeline'en én gang:
      1. Ingester filer i raw_data_dir -> raw_meter_readings (file-level
         idempotent: uændrede filer springes over, se ingestion.py)
      2. Validerer nye raw-rækker -> stg_meter_readings (+ data_quality_errors)
      3. Transformerer nye staging-rækker -> dim_meter / fact_water_consumption
      4. Logger et pipeline_runs-record med status og eksplicitte tællere

    Hvert trin (2 og 3) er sin egen atomiske transaktion (se validation.py
    og transformation.py) - IKKE én kæmpe transaktion for hele pipelinen.
    Det betyder at hvis trin 3 fejler, er trin 2's resultater allerede
    holdbart committet, og næste kørsel kan genoptage derfra (se
    _get_last_processed_ids).

    Returnerer et summary-dict til brug i main.py / logging.
    """
    run_id = _start_run(engine)
    logger.info("Pipeline run %d startet", run_id)

    metrics = {}
    try:
        # --- Trin 1: Ingestion (file-level idempotent) ---
        ingestion_summary = ingest_directory(engine, raw_data_dir)
        metrics.update(ingestion_summary)

        # --- Trin 2: Incremental validation (raw -> staging), atomisk ---
        last_ids = _get_last_processed_ids(engine)
        raw_df = fetch_unprocessed_raw(engine, since_raw_id=last_ids["raw_id"])
        validation_summary = validate_and_load_staging(engine, raw_df)

        new_last_raw_id = (
            int(raw_df["raw_id"].max()) if not raw_df.empty else last_ids["raw_id"]
        )

        metrics["raw_rows_validated"] = len(raw_df)
        metrics["staging_rows_inserted"] = validation_summary["accepted"]
        metrics["staging_rows_rejected"] = validation_summary["rejected"]
        metrics["duplicates_skipped"] = validation_summary["duplicates_skipped"]

        # --- Trin 3: Incremental transformation (staging -> analytics), atomisk ---
        staging_df = fetch_unprocessed_staging(engine, since_stg_id=last_ids["stg_id"])
        inserted_facts = transform_staging_to_fact(engine, staging_df)

        new_last_stg_id = (
            int(staging_df["stg_id"].max()) if not staging_df.empty else last_ids["stg_id"]
        )
        metrics["facts_inserted"] = inserted_facts

        _finish_run(
            engine, run_id, status="SUCCESS", metrics=metrics,
            last_processed_raw_id=new_last_raw_id,
            last_processed_stg_id=new_last_stg_id,
        )

        summary = {
            "run_id": run_id,
            "status": "SUCCESS",
            **metrics,
        }
        logger.info("Pipeline run %d fuldført: %s", run_id, summary)
        return summary

    except Exception:
        logger.exception("Pipeline run %d fejlede", run_id)
        _finish_run(
            engine, run_id, status="FAILED", metrics=metrics,
            last_processed_raw_id=0, last_processed_stg_id=0,
        )
        raise
