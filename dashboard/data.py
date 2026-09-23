"""
dashboard/data.py
------------------
Read-only data-adgangslag for BI-dashboardet.

VIGTIGT ARKITEKTUR-PRINCIP: dette modul må ALDRIG indsætte, opdatere,
slette eller køre migrationer/pipeline. Det konsumerer udelukkende det
allerede transformerede analytics-lag (og staging/raw/pipeline_runs til de
mere tekniske sider) - præcis som Power BI ville gøre via en read-only
forbindelse. Al forretningslogik (validering, transformation, hvad der er
"suspicious") er allerede besluttet af pipelinen; dashboardet aggregerer og
filtrerer, det opfinder ikke nye regler.

Alle funktioner tager en SQLAlchemy Engine som parameter (ingen globale
forbindelser), så både `dashboard/app.py` og tests kan pege dem mod
forskellige databaser (fx en tom test-in-memory-database).
"""

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import List, Optional
import os

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Database dashboardet peger på som standard - den "rigtige" udviklings-
# database. Kan overstyres med miljøvariablen DASHBOARD_DB_PATH, fx for at
# pege på data/dashboard_demo.db (se scripts/run_dashboard_demo_pipeline.py)
# uden at ændre kode. Dashboardet viser altid den AKTIVE sti i sidebaren,
# så det aldrig er tvetydigt hvilken database der ses.
DEFAULT_DASHBOARD_DB_PATH = Path(
    os.environ.get(
        "DASHBOARD_DB_PATH",
        str(Path(__file__).resolve().parent.parent / "data" / "warehouse.db"),
    )
)


@dataclass
class DashboardFilters:
    """
    Samler sidebar-filtrene ét sted, så data-funktionerne har en enkelt,
    typet parameter i stedet for 5 løse argumenter hver.

    `meter_ids=None` eller tom liste betyder "alle målere" (ingen filter).
    `status=None` betyder "alle statusser".
    `suspicious_only=None` betyder "alle" (ikke filtreret på suspicious).
    """

    start_date: Optional[date] = None
    end_date: Optional[date] = None
    meter_ids: List[str] = field(default_factory=list)
    status: Optional[str] = None
    suspicious_only: Optional[bool] = None  # True=kun suspicious, False=kun normale, None=alle


def _fact_where_clause(filters: DashboardFilters, alias: str = "f") -> tuple:
    """
    Bygger en parameteriseret WHERE-klausul der er FÆLLES for alle
    fact-forespørgsler, så filtrering er konsistent på tværs af hele
    dashboardet (samme filter giver samme resultat i KPI'er, grafer og
    tabeller).

    Returnerer (sql_fragment, params) - sql_fragment starter med "WHERE 1=1"
    så hver betingelse simpelt kan tilføjes med "AND ...".
    """
    clauses = ["1=1"]
    params: dict = {}

    if filters.start_date:
        clauses.append(f"DATE({alias}.reading_timestamp) >= :start_date")
        params["start_date"] = filters.start_date.isoformat()
    if filters.end_date:
        clauses.append(f"DATE({alias}.reading_timestamp) <= :end_date")
        params["end_date"] = filters.end_date.isoformat()
    if filters.meter_ids:
        placeholders = ", ".join(f":meter_{i}" for i in range(len(filters.meter_ids)))
        clauses.append(f"d.meter_id IN ({placeholders})")
        for i, meter_id in enumerate(filters.meter_ids):
            params[f"meter_{i}"] = meter_id
    if filters.status:
        clauses.append(f"{alias}.status = :status")
        params["status"] = filters.status
    if filters.suspicious_only is True:
        clauses.append(f"{alias}.is_suspicious = 1")
    elif filters.suspicious_only is False:
        clauses.append(f"{alias}.is_suspicious = 0")

    return "WHERE " + " AND ".join(clauses), params


def load_meter_list(engine: Engine) -> List[str]:
    """Alle kendte meter_id'er, til sidebar-multiselect. Tom liste hvis
    dim_meter er tom (ingen data endnu)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT meter_id FROM dim_meter ORDER BY meter_id")
        ).fetchall()
    return [r[0] for r in rows]


def load_status_list(engine: Engine) -> List[str]:
    """Distinkte statusser der reelt findes i fact-tabellen."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DISTINCT status FROM fact_water_consumption "
                "WHERE status IS NOT NULL ORDER BY status"
            )
        ).fetchall()
    return [r[0] for r in rows]


def load_filtered_fact_data(engine: Engine, filters: DashboardFilters) -> pd.DataFrame:
    """
    Den centrale forespørgsel: fact JOIN dim, filtreret efter sidebar-valg.
    Bruges som grundlag for KPI'er, tidsserie-graf og top-målere-graf, så
    ALLE dele af Overview-siden bruger PRÆCIS samme filtrerede datasæt
    (ingen risiko for at KPI'er og grafer bygger på forskellige filtre).
    """
    where_sql, params = _fact_where_clause(filters)
    query = text(
        f"""
        SELECT
            f.fact_id,
            d.meter_id,
            f.reading_timestamp,
            f.consumption_liters,
            f.temperature,
            f.status,
            f.is_suspicious,
            f.source_stg_id
        FROM fact_water_consumption f
        JOIN dim_meter d ON d.meter_key = f.meter_key
        {where_sql}
        ORDER BY f.reading_timestamp
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)
    if not df.empty:
        df["reading_timestamp"] = pd.to_datetime(df["reading_timestamp"])
    return df


def load_kpis(engine: Engine, filters: DashboardFilters) -> dict:
    """
    Beregner de fem KPI'er vist øverst på Overview-siden - alle udledt af
    det SAMME filtrerede datasæt som graferne (se `load_filtered_fact_data`).

    DATA QUALITY RATE-definition (eksplicit, da schemaet ikke gør den
    entydig af sig selv):
        accepterede staging-rækker / valideret rå-rækker
      = COUNT(stg_meter_readings) / COUNT(raw_meter_readings behandlet af validation)
    Vi bruger her `staging_rows_inserted` og `raw_rows_validated` summeret
    over ALLE pipeline_runs (kumulativ definition, uafhængig af
    dashboard-filtrene, fordi validation-metrikker ikke er en del af
    fact-tabellen og derfor ikke kan tidsfiltreres på samme måde - se
    docstring nedenfor for hvorfor).
    """
    fact_df = load_filtered_fact_data(engine, filters)

    total_consumption = float(fact_df["consumption_liters"].sum()) if not fact_df.empty else 0.0
    avg_consumption = float(fact_df["consumption_liters"].mean()) if not fact_df.empty else 0.0
    active_meters = int(fact_df["meter_id"].nunique()) if not fact_df.empty else 0
    suspicious_count = int(fact_df["is_suspicious"].sum()) if not fact_df.empty else 0

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    SUM(raw_rows_validated) AS total_validated,
                    SUM(staging_rows_inserted) AS total_accepted
                FROM pipeline_runs
                """
            )
        ).fetchone()
    total_validated = (row[0] or 0) if row else 0
    total_accepted = (row[1] or 0) if row else 0
    data_quality_rate = (total_accepted / total_validated) if total_validated else None

    return {
        "total_consumption_liters": total_consumption,
        "avg_consumption_liters": avg_consumption,
        "active_meters": active_meters,
        "suspicious_readings": suspicious_count,
        "data_quality_rate": data_quality_rate,
        "readings_count": int(len(fact_df)),
    }


def load_consumption_over_time(engine: Engine, filters: DashboardFilters) -> pd.DataFrame:
    """
    Tidsserie af SUM(consumption_liters) pr. dag - samme koncept som
    forespørgsel 1 i sql/analytics_queries.sql ("Samlet vandforbrug pr.
    dag"), men parameteriseret med dashboard-filtrene.
    """
    where_sql, params = _fact_where_clause(filters)
    query = text(
        f"""
        SELECT
            DATE(f.reading_timestamp) AS reading_date,
            SUM(f.consumption_liters) AS total_liters
        FROM fact_water_consumption f
        JOIN dim_meter d ON d.meter_key = f.meter_key
        {where_sql}
        GROUP BY DATE(f.reading_timestamp)
        ORDER BY reading_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)
    if not df.empty:
        df["reading_date"] = pd.to_datetime(df["reading_date"])
    return df


def load_consumption_over_time_by_meter(engine: Engine, filters: DashboardFilters) -> pd.DataFrame:
    """Samme som `load_consumption_over_time`, men opdelt pr. måler - bruges
    til den valgfrie 'By meter'-visning."""
    where_sql, params = _fact_where_clause(filters)
    query = text(
        f"""
        SELECT
            DATE(f.reading_timestamp) AS reading_date,
            d.meter_id,
            SUM(f.consumption_liters) AS total_liters
        FROM fact_water_consumption f
        JOIN dim_meter d ON d.meter_key = f.meter_key
        {where_sql}
        GROUP BY DATE(f.reading_timestamp), d.meter_id
        ORDER BY reading_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)
    if not df.empty:
        df["reading_date"] = pd.to_datetime(df["reading_date"])
    return df


def load_top_meters(engine: Engine, filters: DashboardFilters, limit: int = 10) -> pd.DataFrame:
    """
    Top-N målere efter samlet forbrug - samme koncept som forespørgsel 2 i
    sql/analytics_queries.sql ("De 10 målere med størst samlet forbrug").
    """
    where_sql, params = _fact_where_clause(filters)
    params["limit"] = limit
    query = text(
        f"""
        SELECT
            d.meter_id,
            SUM(f.consumption_liters) AS total_liters
        FROM fact_water_consumption f
        JOIN dim_meter d ON d.meter_key = f.meter_key
        {where_sql}
        GROUP BY d.meter_id
        ORDER BY total_liters DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


def load_status_breakdown(engine: Engine, filters: DashboardFilters) -> pd.DataFrame:
    """Antal målinger pr. status - bruges til fordelings-visualiseringen på
    Overview-siden."""
    where_sql, params = _fact_where_clause(filters)
    query = text(
        f"""
        SELECT
            COALESCE(f.status, 'UKENDT') AS status,
            COUNT(*) AS reading_count
        FROM fact_water_consumption f
        JOIN dim_meter d ON d.meter_key = f.meter_key
        {where_sql}
        GROUP BY COALESCE(f.status, 'UKENDT')
        ORDER BY reading_count DESC
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


def load_suspicious_readings(engine: Engine, filters: DashboardFilters) -> pd.DataFrame:
    """Alle målinger markeret is_suspicious=1, inden for de aktive filtre."""
    forced_filters = DashboardFilters(
        start_date=filters.start_date,
        end_date=filters.end_date,
        meter_ids=filters.meter_ids,
        status=filters.status,
        suspicious_only=True,
    )
    where_sql, params = _fact_where_clause(forced_filters)
    query = text(
        f"""
        SELECT
            d.meter_id,
            f.reading_timestamp,
            f.consumption_liters,
            f.temperature,
            f.status
        FROM fact_water_consumption f
        JOIN dim_meter d ON d.meter_key = f.meter_key
        {where_sql}
        ORDER BY f.reading_timestamp DESC
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)
    if not df.empty:
        df["reading_timestamp"] = pd.to_datetime(df["reading_timestamp"])
    return df


def load_meter_detail_kpis(engine: Engine, meter_id: str) -> dict:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    SUM(f.consumption_liters) AS total_liters,
                    AVG(f.consumption_liters) AS avg_liters,
                    MAX(f.consumption_liters) AS max_liters,
                    COUNT(*) AS reading_count,
                    SUM(f.is_suspicious) AS suspicious_count
                FROM fact_water_consumption f
                JOIN dim_meter d ON d.meter_key = f.meter_key
                WHERE d.meter_id = :meter_id
                """
            ),
            {"meter_id": meter_id},
        ).fetchone()
    if row is None or row[3] == 0:
        return {
            "total_liters": 0.0, "avg_liters": 0.0, "max_liters": 0.0,
            "reading_count": 0, "suspicious_count": 0,
        }
    return {
        "total_liters": float(row[0] or 0.0),
        "avg_liters": float(row[1] or 0.0),
        "max_liters": float(row[2] or 0.0),
        "reading_count": int(row[3] or 0),
        "suspicious_count": int(row[4] or 0),
    }


def load_meter_readings(engine: Engine, meter_id: str) -> pd.DataFrame:
    """Alle målinger for én måler, nyeste først - grundlag for
    Meter Analysis-sidens grafer og tabel."""
    query = text(
        """
        SELECT
            f.reading_timestamp,
            f.consumption_liters,
            f.temperature,
            f.status,
            f.is_suspicious
        FROM fact_water_consumption f
        JOIN dim_meter d ON d.meter_key = f.meter_key
        WHERE d.meter_id = :meter_id
        ORDER BY f.reading_timestamp DESC
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"meter_id": meter_id})
    if not df.empty:
        df["reading_timestamp"] = pd.to_datetime(df["reading_timestamp"])
    return df


def load_quality_kpis(engine: Engine) -> dict:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) AS total_issues,
                    SUM(CASE WHEN severity = 'ERROR' THEN 1 ELSE 0 END) AS errors,
                    SUM(CASE WHEN severity = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
                    COUNT(DISTINCT meter_id) AS affected_meters,
                    COUNT(DISTINCT source_file) AS affected_files
                FROM data_quality_errors
                """
            )
        ).fetchone()
    if row is None or row[0] == 0:
        return {"total_issues": 0, "errors": 0, "warnings": 0,
                "affected_meters": 0, "affected_files": 0}
    return {
        "total_issues": int(row[0] or 0),
        "errors": int(row[1] or 0),
        "warnings": int(row[2] or 0),
        "affected_meters": int(row[3] or 0),
        "affected_files": int(row[4] or 0),
    }


def load_quality_issues_by_type(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT error_type, COUNT(*) AS issue_count
        FROM data_quality_errors
        GROUP BY error_type
        ORDER BY issue_count DESC
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def load_quality_severity_split(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT severity, COUNT(*) AS issue_count
        FROM data_quality_errors
        GROUP BY severity
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def load_recent_quality_issues(engine: Engine, severity: Optional[str] = None,
                                 limit: int = 200) -> pd.DataFrame:
    clauses = ["1=1"]
    params: dict = {"limit": limit}
    if severity:
        clauses.append("severity = :severity")
        params["severity"] = severity
    query = text(
        f"""
        SELECT
            severity, error_type, meter_id, reading_timestamp,
            source_file, error_detail, detected_at
        FROM data_quality_errors
        WHERE {" AND ".join(clauses)}
        ORDER BY detected_at DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


def load_pipeline_health_kpis(engine: Engine) -> dict:
    with engine.connect() as conn:
        last_run = conn.execute(
            text(
                """
                SELECT run_id, status, start_time, end_time,
                       raw_rows_ingested, staging_rows_inserted,
                       staging_rows_rejected, facts_inserted
                FROM pipeline_runs ORDER BY run_id DESC LIMIT 1
                """
            )
        ).fetchone()
        last_success = conn.execute(
            text(
                """
                SELECT run_id, end_time FROM pipeline_runs
                WHERE status = 'SUCCESS' ORDER BY run_id DESC LIMIT 1
                """
            )
        ).fetchone()

    if last_run is None:
        return {
            "last_run_status": None, "last_success_run_id": None,
            "last_success_end_time": None, "raw_rows_ingested": 0,
            "staging_rows_rejected": 0, "facts_inserted": 0,
            "duration_seconds": None,
        }

    duration_seconds = None
    if last_run[2] and last_run[3]:
        try:
            start = pd.to_datetime(last_run[2])
            end = pd.to_datetime(last_run[3])
            duration_seconds = (end - start).total_seconds()
        except Exception:
            duration_seconds = None

    return {
        "last_run_status": last_run[1],
        "last_success_run_id": last_success[0] if last_success else None,
        "last_success_end_time": last_success[1] if last_success else None,
        "raw_rows_ingested": int(last_run[4] or 0),
        "staging_rows_rejected": int(last_run[6] or 0),
        "facts_inserted": int(last_run[7] or 0),
        "duration_seconds": duration_seconds,
    }


def load_pipeline_runs(engine: Engine, limit: int = 50) -> pd.DataFrame:
    query = text(
        """
        SELECT
            run_id, start_time, end_time, status,
            files_discovered, files_ingested, files_skipped,
            raw_rows_ingested, raw_rows_validated,
            staging_rows_inserted, staging_rows_rejected, duplicates_skipped,
            facts_inserted, failed_stage, error_type
        FROM pipeline_runs
        ORDER BY run_id DESC
        LIMIT :limit
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"limit": limit})


def load_failed_pipeline_runs(engine: Engine) -> pd.DataFrame:
    query = text(
        """
        SELECT run_id, start_time, failed_stage, error_type, error_message
        FROM pipeline_runs
        WHERE status = 'FAILED'
        ORDER BY run_id DESC
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def load_fact_id_options(engine: Engine, limit: int = 500) -> List[int]:
    """Liste af fact_id'er til lineage-sidens selectbox (begrænset for at
    holde UI'et hurtigt - dette er en demo, ikke et search-index)."""
    query = text("SELECT fact_id FROM fact_water_consumption ORDER BY fact_id LIMIT :limit")
    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": limit}).fetchall()
    return [r[0] for r in rows]


def load_lineage(engine: Engine, fact_id: int) -> Optional[dict]:
    """
    Følger lineage-kæden for ÉN fact-række tilbage til kilde-filen:
    fact -> stg -> raw -> ingested_files. Returnerer None hvis fact_id
    ikke findes (fx et forkert indtastet ID).
    """
    query = text(
        """
        SELECT
            f.fact_id, f.meter_key, d.meter_id, f.reading_timestamp,
            f.consumption_liters, f.is_suspicious,
            s.stg_id, s.raw_id, s.processed_at,
            r.raw_id AS r_raw_id, r.source_file, r.ingested_at, r.raw_payload,
            r.file_id,
            i.file_name, i.file_hash, i.status
        FROM fact_water_consumption f
        JOIN dim_meter d ON d.meter_key = f.meter_key
        JOIN stg_meter_readings s ON s.stg_id = f.source_stg_id
        JOIN raw_meter_readings r ON r.raw_id = s.raw_id
        LEFT JOIN ingested_files i ON i.file_id = r.file_id
        WHERE f.fact_id = :fact_id
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"fact_id": fact_id}).fetchone()
    if row is None:
        return None
    return {
        "fact_id": row[0], "meter_id": row[2], "reading_timestamp": row[3],
        "consumption_liters": row[4], "is_suspicious": row[5],
        "stg_id": row[6], "stg_raw_id": row[7], "processed_at": row[8],
        "raw_id": row[9], "source_file": row[10], "ingested_at": row[11],
        "raw_payload": row[12], "file_id": row[13],
        "file_name": row[14], "file_hash": row[15], "file_status": row[16],
    }
