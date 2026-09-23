"""
transformation.py
------------------
Formål: transformere STAGING-data til ANALYTICS-laget
(dim_meter + fact_water_consumption).

Hvorfor et separat transformationstrin i stedet for at query'e stg direkte
i Power BI?
- Fact/dimension-modellen er optimeret til analyse (star schema): færre
  joins, konsistente surrogate keys, og plads til forretningslogik som
  "suspicious"-flaget.
- Staging kan stadig indeholde "rå men gyldig" data (fx forskellige
  statuskoder); analytics-laget er det kuraterede, stabile lag BI-værktøjer
  bygger rapporter oven på.
"""

import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config import PipelineConfig, DEFAULT_CONFIG

logger = logging.getLogger(__name__)

# Bevaret for bagudkompatibilitet (fx eksisterende tests der importerer
# konstanten direkte). Den REELLE konfiguration bør nu ske via
# PipelineConfig (src/config.py) - se transform_staging_to_fact.
SUSPICIOUS_THRESHOLD_LITERS = DEFAULT_CONFIG.suspicious_threshold_liters


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_unprocessed_staging(engine: Engine, since_stg_id: int = 0) -> pd.DataFrame:
    """Incremental load: hent kun staging-rækker nyere end sidst behandlede stg_id."""
    query = text(
        "SELECT * FROM stg_meter_readings WHERE stg_id > :since ORDER BY stg_id"
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"since": since_stg_id})
    return df


def upsert_dim_meter(conn, meter_ids: list, min_ts_by_meter: dict, max_ts_by_meter: dict,
                      processing_time: str) -> dict:
    """
    Sikrer at hver meter_id i inputtet findes i dim_meter, og opdaterer
    EVENT TIME-kolonnerne (first_reading_at/last_reading_at) korrekt med
    MIN/MAX-semantik - også hvis data ankommer i vilkårlig rækkefølge
    (late-arriving data).

    - Ny meter_id  -> indsæt ny dimension-række (created_at = updated_at = nu)
    - Kendt meter_id -> first_reading_at = MIN(eksisterende, ny batch),
                        last_reading_at  = MAX(eksisterende, ny batch),
                        updated_at = nu (processing time)

    Skalerbarhed: vi henter og opdaterer kun de meter_id'er der optræder i
    DENNE batch - ikke hele dim_meter-tabellen. Med tusindvis af målere og
    kun få i en given batch, er det væsentligt billigere end at loade/gemme
    hele dimensionen hver gang.

    Modtager en Connection (ikke en Engine): dette skal være en del af den
    samme transaktion som fact-indsættelsen (se transform_staging_to_fact).

    Returnerer et dict {meter_id: meter_key}.
    """
    unique_meter_ids = list(set(meter_ids))
    if not unique_meter_ids:
        return {}

    placeholders = ", ".join(f":m{i}" for i in range(len(unique_meter_ids)))
    params = {f"m{i}": mid for i, mid in enumerate(unique_meter_ids)}
    existing = conn.execute(
        text(f"SELECT meter_key, meter_id, first_reading_at, last_reading_at "
             f"FROM dim_meter WHERE meter_id IN ({placeholders})"),
        params,
    ).fetchall()
    existing_map = {row.meter_id: row for row in existing}

    for meter_id in unique_meter_ids:
        batch_min = min_ts_by_meter[meter_id]
        batch_max = max_ts_by_meter[meter_id]

        if meter_id not in existing_map:
            conn.execute(
                text(
                    """
                    INSERT INTO dim_meter
                        (meter_id, first_reading_at, last_reading_at, created_at, updated_at)
                    VALUES (:meter_id, :first_reading_at, :last_reading_at, :created_at, :updated_at)
                    """
                ),
                {
                    "meter_id": meter_id,
                    "first_reading_at": batch_min,
                    "last_reading_at": batch_max,
                    "created_at": processing_time,
                    "updated_at": processing_time,
                },
            )
        else:
            row = existing_map[meter_id]
            new_first = min(row.first_reading_at, batch_min)
            new_last = max(row.last_reading_at, batch_max)
            conn.execute(
                text(
                    """
                    UPDATE dim_meter
                    SET first_reading_at = :first_reading_at,
                        last_reading_at = :last_reading_at,
                        updated_at = :updated_at
                    WHERE meter_id = :meter_id
                    """
                ),
                {
                    "meter_id": meter_id,
                    "first_reading_at": new_first,
                    "last_reading_at": new_last,
                    "updated_at": processing_time,
                },
            )

    refreshed = conn.execute(
        text(f"SELECT meter_key, meter_id FROM dim_meter WHERE meter_id IN ({placeholders})"),
        params,
    ).fetchall()
    return {row.meter_id: row.meter_key for row in refreshed}


def transform_staging_to_fact(engine: Engine, staging_df: pd.DataFrame,
                               config: PipelineConfig = DEFAULT_CONFIG) -> int:
    """
    Transformerer staging-rækker til fact_water_consumption + opdaterer dim_meter.
    Bruger INSERT OR IGNORE for idempotency (samme grund som i staging-trinnet).

    `config` (PipelineConfig) holder FORRETNINGSREGLER som
    suspicious_threshold_liters adskilt fra selve transformations-koden -
    se src/config.py for hvorfor. Tests kan nemt levere deres egen
    PipelineConfig med en anden tærskel.

    Transaktionsgrænse (atomicity): dimension-upsert og fact-indsættelse sker
    i ÉN transaktion. De hører logisk sammen som ét pipeline-trin
    ("STAGING -> DIMENSION + FACT") - enten committer begge dele, eller ingen.

    Returnerer antal indsatte fact-rækker.
    """
    if staging_df.empty:
        return 0

    processing_time = _now_iso()

    min_ts_by_meter = staging_df.groupby("meter_id")["reading_timestamp"].min().to_dict()
    max_ts_by_meter = staging_df.groupby("meter_id")["reading_timestamp"].max().to_dict()

    with engine.begin() as conn:
        meter_key_map = upsert_dim_meter(
            conn, staging_df["meter_id"].tolist(), min_ts_by_meter, max_ts_by_meter,
            processing_time,
        )

        rows = []
        for _, row in staging_df.iterrows():
            is_suspicious = 1 if row["consumption_liters"] > config.suspicious_threshold_liters else 0
            rows.append(
                {
                    "meter_key": int(meter_key_map[row["meter_id"]]),
                    "source_stg_id": int(row["stg_id"]),
                    "reading_timestamp": row["reading_timestamp"],
                    "consumption_liters": float(row["consumption_liters"]),
                    "temperature": row["temperature"] if pd.notna(row["temperature"]) else None,
                    "status": row["status"],
                    "is_suspicious": is_suspicious,
                }
            )

        insert_sql = text(
            """
            INSERT OR IGNORE INTO fact_water_consumption
                (meter_key, source_stg_id, reading_timestamp, consumption_liters, temperature,
                 status, is_suspicious)
            VALUES
                (:meter_key, :source_stg_id, :reading_timestamp, :consumption_liters, :temperature,
                 :status, :is_suspicious)
            """
        )
        result = conn.execute(insert_sql, rows)
        return result.rowcount if result.rowcount is not None else len(rows)
