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

logger = logging.getLogger(__name__)

# Simpel regel for "usædvanligt højt forbrug": alt over denne grænse (liter
# pr. måling) flages som suspicious. I et rigtigt projekt ville denne
# grænse nok være statistisk (fx X standardafvigelser over målerens eget
# gennemsnit) - her holder vi den bevidst simpel til læringsformål.
SUSPICIOUS_THRESHOLD_LITERS = 2000.0


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


def upsert_dim_meter(engine: Engine, meter_ids: list, event_time: str) -> dict:
    """
    Sikrer at hver meter_id i inputtet findes i dim_meter.
    - Ny meter_id -> indsæt ny dimension-række
    - Kendt meter_id -> opdater last_seen_at

    Returnerer et dict {meter_id: meter_key} til brug ved fact-indsættelse.
    """
    with engine.begin() as conn:
        existing = pd.read_sql(text("SELECT meter_key, meter_id FROM dim_meter"), conn)
    existing_map = dict(zip(existing["meter_id"], existing["meter_key"]))

    new_meters = [m for m in set(meter_ids) if m not in existing_map]

    with engine.begin() as conn:
        for meter_id in new_meters:
            conn.execute(
                text(
                    """
                    INSERT INTO dim_meter (meter_id, first_seen_at, last_seen_at)
                    VALUES (:meter_id, :ts, :ts)
                    """
                ),
                {"meter_id": meter_id, "ts": event_time},
            )
        # Opdater last_seen_at for alle målere vi netop har set data fra
        for meter_id in set(meter_ids):
            conn.execute(
                text(
                    "UPDATE dim_meter SET last_seen_at = :ts WHERE meter_id = :meter_id"
                ),
                {"meter_id": meter_id, "ts": event_time},
            )

    with engine.connect() as conn:
        refreshed = pd.read_sql(text("SELECT meter_key, meter_id FROM dim_meter"), conn)
    return dict(zip(refreshed["meter_id"], refreshed["meter_key"]))


def transform_staging_to_fact(engine: Engine, staging_df: pd.DataFrame) -> int:
    """
    Transformerer staging-rækker til fact_water_consumption.
    Bruger INSERT OR IGNORE for idempotency (samme grund som i staging-trinnet).

    Returnerer antal indsatte fact-rækker.
    """
    if staging_df.empty:
        return 0

    event_time = _now_iso()
    meter_key_map = upsert_dim_meter(engine, staging_df["meter_id"].tolist(), event_time)

    rows = []
    for _, row in staging_df.iterrows():
        is_suspicious = 1 if row["consumption_liters"] > SUSPICIOUS_THRESHOLD_LITERS else 0
        rows.append(
            {
                "meter_key": int(meter_key_map[row["meter_id"]]),
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
            (meter_key, reading_timestamp, consumption_liters, temperature,
             status, is_suspicious)
        VALUES
            (:meter_key, :reading_timestamp, :consumption_liters, :temperature,
             :status, :is_suspicious)
        """
    )
    with engine.begin() as conn:
        result = conn.execute(insert_sql, rows)
        return result.rowcount if result.rowcount is not None else len(rows)
