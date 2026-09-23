"""
dashboard/app.py
------------------
Streamlit BI-dashboard: en READ-ONLY konsument af analytics-laget
(dim_meter + fact_water_consumption), samt staging/raw/pipeline_runs til de
mere tekniske sider (Data Quality, Pipeline Health, Data Lineage).

ARKITEKTUR-GRÆNSE (vigtig): dette modul indeholder INGEN ingestion-,
validerings- eller transformationslogik. Det forespørger, filtrerer,
aggregerer og visualiserer - præcis de ansvarsområder et BI-værktøj som
Power BI ville have, hvis det pegede på den samme SQLite-fil. Se README
"BI dashboard" for hvordan dette kortlægger til Power BI-koncepter
(dimension/fact/measures/slicers/visuals).

Kør med:
    streamlit run dashboard/app.py
"""

from pathlib import Path

import pandas as pd
import streamlit as st

from src.database import get_engine
from dashboard import charts, data as dd

st.set_page_config(
    page_title="Water Consumption Analytics",
    page_icon="💧",
    layout="wide",
)

PAGES = [
    "Overview",
    "Meter Analysis",
    "Data Quality",
    "Pipeline Health",
    "Data Lineage",
]


# ---------------------------------------------------------------------------
# Cached data-adgang
#
# @st.cache_data holder forespørgsler hurtige ved genkørsel af scriptet
# (Streamlit genkører hele filen ved hver interaktion) - men cachen SKAL
# kunne invalideres eksplicit (se "Refresh data"-knappen i sidebaren),
# ellers ville dashboardet vise forældede tal efter en ny pipeline-kørsel.
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_cached_engine(db_path_str: str):
    return get_engine(db_path=Path(db_path_str))


@st.cache_data
def _cached_meter_list(db_path_str: str, _cache_key: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_meter_list(engine)


@st.cache_data
def _cached_status_list(db_path_str: str, _cache_key: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_status_list(engine)


@st.cache_data
def _cached_fact_data(db_path_str: str, _cache_key: int, filters_key: tuple):
    engine = _get_cached_engine(db_path_str)
    filters = _filters_from_key(filters_key)
    return dd.load_filtered_fact_data(engine, filters)


@st.cache_data
def _cached_kpis(db_path_str: str, _cache_key: int, filters_key: tuple):
    engine = _get_cached_engine(db_path_str)
    filters = _filters_from_key(filters_key)
    return dd.load_kpis(engine, filters)


@st.cache_data
def _cached_consumption_over_time(db_path_str: str, _cache_key: int, filters_key: tuple):
    engine = _get_cached_engine(db_path_str)
    filters = _filters_from_key(filters_key)
    return dd.load_consumption_over_time(engine, filters)


@st.cache_data
def _cached_consumption_over_time_by_meter(db_path_str: str, _cache_key: int, filters_key: tuple):
    engine = _get_cached_engine(db_path_str)
    filters = _filters_from_key(filters_key)
    return dd.load_consumption_over_time_by_meter(engine, filters)


@st.cache_data
def _cached_top_meters(db_path_str: str, _cache_key: int, filters_key: tuple):
    engine = _get_cached_engine(db_path_str)
    filters = _filters_from_key(filters_key)
    return dd.load_top_meters(engine, filters)


@st.cache_data
def _cached_status_breakdown(db_path_str: str, _cache_key: int, filters_key: tuple):
    engine = _get_cached_engine(db_path_str)
    filters = _filters_from_key(filters_key)
    return dd.load_status_breakdown(engine, filters)


@st.cache_data
def _cached_suspicious_readings(db_path_str: str, _cache_key: int, filters_key: tuple):
    engine = _get_cached_engine(db_path_str)
    filters = _filters_from_key(filters_key)
    return dd.load_suspicious_readings(engine, filters)


@st.cache_data
def _cached_meter_readings(db_path_str: str, _cache_key: int, meter_id: str):
    engine = _get_cached_engine(db_path_str)
    return dd.load_meter_readings(engine, meter_id)


@st.cache_data
def _cached_meter_detail_kpis(db_path_str: str, _cache_key: int, meter_id: str):
    engine = _get_cached_engine(db_path_str)
    return dd.load_meter_detail_kpis(engine, meter_id)


@st.cache_data
def _cached_quality_kpis(db_path_str: str, _cache_key: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_quality_kpis(engine)


@st.cache_data
def _cached_quality_by_type(db_path_str: str, _cache_key: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_quality_issues_by_type(engine)


@st.cache_data
def _cached_quality_severity(db_path_str: str, _cache_key: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_quality_severity_split(engine)


@st.cache_data
def _cached_recent_quality_issues(db_path_str: str, _cache_key: int, severity):
    engine = _get_cached_engine(db_path_str)
    return dd.load_recent_quality_issues(engine, severity=severity)


@st.cache_data
def _cached_pipeline_health_kpis(db_path_str: str, _cache_key: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_pipeline_health_kpis(engine)


@st.cache_data
def _cached_pipeline_runs(db_path_str: str, _cache_key: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_pipeline_runs(engine)


@st.cache_data
def _cached_failed_pipeline_runs(db_path_str: str, _cache_key: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_failed_pipeline_runs(engine)


@st.cache_data
def _cached_fact_id_options(db_path_str: str, _cache_key: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_fact_id_options(engine)


@st.cache_data
def _cached_lineage(db_path_str: str, _cache_key: int, fact_id: int):
    engine = _get_cached_engine(db_path_str)
    return dd.load_lineage(engine, fact_id)


def _filters_from_key(filters_key: tuple) -> dd.DashboardFilters:
    start_date, end_date, meter_ids, status, suspicious_only = filters_key
    return dd.DashboardFilters(
        start_date=start_date, end_date=end_date,
        meter_ids=list(meter_ids), status=status,
        suspicious_only=suspicious_only,
    )


def _filters_to_key(filters: dd.DashboardFilters) -> tuple:
    """Cache-nøgler skal være hashable - laver filtrene om til en tuple."""
    return (
        filters.start_date, filters.end_date,
        tuple(sorted(filters.meter_ids)), filters.status, filters.suspicious_only,
    )


def _format_liters(value: float) -> str:
    return f"{value:,.0f} L"


def _format_liters_per_reading(value: float) -> str:
    return f"{value:,.0f} L / reading"


def _format_percent(value):
    if value is None:
        return "N/A"
    return f"{value * 100:,.1f}%"


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar(db_path_str: str, cache_key: int):
    st.sidebar.title("💧 Water Consumption Analytics")
    st.sidebar.caption(f"Database: `{db_path_str}`")

    page = st.sidebar.radio("Navigation", PAGES, label_visibility="collapsed")

    st.sidebar.divider()
    st.sidebar.subheader("Filters")

    all_meters = _cached_meter_list(db_path_str, cache_key)
    all_statuses = _cached_status_list(db_path_str, cache_key)

    fact_df_unfiltered = _cached_fact_data(
        db_path_str, cache_key, _filters_to_key(dd.DashboardFilters())
    )
    if not fact_df_unfiltered.empty:
        min_date = fact_df_unfiltered["reading_timestamp"].min().date()
        max_date = fact_df_unfiltered["reading_timestamp"].max().date()
    else:
        min_date = max_date = None

    if min_date and max_date:
        date_range = st.sidebar.date_input(
            "Date range", value=(min_date, max_date),
            min_value=min_date, max_value=max_date,
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date, end_date = min_date, max_date
    else:
        start_date, end_date = None, None
        st.sidebar.info("No readings available yet to filter by date.")

    meter_selection = st.sidebar.multiselect(
        "Meter ID", options=all_meters, default=[],
        help="Leave empty to include all meters.",
    )

    status_selection = st.sidebar.selectbox(
        "Status", options=["All"] + all_statuses,
    )
    status_filter = None if status_selection == "All" else status_selection

    suspicious_selection = st.sidebar.selectbox(
        "Suspicious readings", options=["All", "Suspicious only", "Normal only"],
    )
    suspicious_filter = {
        "All": None, "Suspicious only": True, "Normal only": False,
    }[suspicious_selection]

    st.sidebar.divider()
    if st.sidebar.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

    filters = dd.DashboardFilters(
        start_date=start_date, end_date=end_date,
        meter_ids=meter_selection, status=status_filter,
        suspicious_only=suspicious_filter,
    )
    return page, filters


# ---------------------------------------------------------------------------
# Page 1: Overview
# ---------------------------------------------------------------------------

def render_overview(db_path_str: str, cache_key: int, filters: dd.DashboardFilters):
    st.title("Overview")
    st.caption("Executive summary of water consumption across all meters.")

    filters_key = _filters_to_key(filters)
    kpis = _cached_kpis(db_path_str, cache_key, filters_key)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total consumption", _format_liters(kpis["total_consumption_liters"]))
    col2.metric("Average consumption", _format_liters_per_reading(kpis["avg_consumption_liters"]))
    col3.metric("Active meters", kpis["active_meters"])
    col4.metric("Suspicious readings", kpis["suspicious_readings"])
    col5.metric("Data quality rate", _format_percent(kpis["data_quality_rate"]),
                help="Accepted staging records ÷ validated raw records, "
                     "summed across all pipeline runs.")

    st.divider()

    view_mode = st.radio("Consumption view", ["Total", "By meter"], horizontal=True)
    if view_mode == "Total":
        ts_df = _cached_consumption_over_time(db_path_str, cache_key, filters_key)
        st.plotly_chart(charts.consumption_over_time_chart(ts_df), use_container_width=True)
    else:
        ts_by_meter_df = _cached_consumption_over_time_by_meter(db_path_str, cache_key, filters_key)
        st.plotly_chart(charts.consumption_over_time_by_meter_chart(ts_by_meter_df), use_container_width=True)

    left, right = st.columns(2)
    with left:
        top_meters_df = _cached_top_meters(db_path_str, cache_key, filters_key)
        st.plotly_chart(charts.top_meters_chart(top_meters_df), use_container_width=True)
    with right:
        status_df = _cached_status_breakdown(db_path_str, cache_key, filters_key)
        st.plotly_chart(charts.status_breakdown_chart(status_df), use_container_width=True)

    st.divider()
    st.subheader("Suspicious readings")
    st.caption(
        "The anomaly threshold used here is illustrative (a fixed, "
        "configured liter value — see `src/config.py`), not a statistical "
        "leak-detection algorithm. These are simply readings that exceed "
        "the configured demonstration threshold, not confirmed leaks."
    )
    suspicious_df = _cached_suspicious_readings(db_path_str, cache_key, filters_key)
    if suspicious_df.empty:
        st.success("No suspicious readings for the selected filters.")
    else:
        st.dataframe(
            suspicious_df.style.apply(
                lambda row: ["background-color: #fff1f0"] * len(row), axis=1
            ),
            use_container_width=True, hide_index=True,
        )


# ---------------------------------------------------------------------------
# Page 2: Meter Analysis
# ---------------------------------------------------------------------------

def render_meter_analysis(db_path_str: str, cache_key: int):
    st.title("Meter Analysis")
    st.caption("Detailed consumption and quality profile for a single meter.")

    all_meters = _cached_meter_list(db_path_str, cache_key)
    if not all_meters:
        st.info("No meters available yet — run the pipeline to populate data.")
        return

    meter_id = st.selectbox("Select meter", options=all_meters)

    kpis = _cached_meter_detail_kpis(db_path_str, cache_key, meter_id)
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total consumption", _format_liters(kpis["total_liters"]))
    col2.metric("Average reading", _format_liters_per_reading(kpis["avg_liters"]))
    col3.metric("Maximum reading", _format_liters(kpis["max_liters"]))
    col4.metric("Number of readings", kpis["reading_count"])
    col5.metric("Suspicious readings", kpis["suspicious_count"])

    readings_df = _cached_meter_readings(db_path_str, cache_key, meter_id)
    if readings_df.empty:
        st.info("No readings recorded for this meter yet.")
        return

    st.plotly_chart(
        charts.meter_time_series_chart(readings_df, meter_id), use_container_width=True
    )

    temp_fig = charts.meter_temperature_chart(readings_df, meter_id)
    if temp_fig is not None:
        st.plotly_chart(temp_fig, use_container_width=True)
    else:
        st.caption("No temperature data recorded for this meter.")

    st.subheader("Readings")
    display_df = readings_df.sort_values("reading_timestamp", ascending=False)[
        ["reading_timestamp", "consumption_liters", "temperature", "status", "is_suspicious"]
    ]
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page 3: Data Quality
# ---------------------------------------------------------------------------

def render_data_quality(db_path_str: str, cache_key: int):
    st.title("Data Quality")
    st.caption("Visibility into rejected and flagged records — data engineering is not only about successful data.")

    kpis = _cached_quality_kpis(db_path_str, cache_key)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total quality issues", kpis["total_issues"])
    col2.metric("Errors", kpis["errors"])
    col3.metric("Warnings", kpis["warnings"])
    col4.metric("Affected source files", kpis["affected_files"])

    if kpis["total_issues"] == 0:
        st.success("No data quality issues recorded.")
        return

    left, right = st.columns(2)
    with left:
        by_type_df = _cached_quality_by_type(db_path_str, cache_key)
        st.plotly_chart(charts.quality_issues_by_type_chart(by_type_df), use_container_width=True)
    with right:
        severity_df = _cached_quality_severity(db_path_str, cache_key)
        st.plotly_chart(charts.quality_severity_donut_chart(severity_df), use_container_width=True)

    st.divider()
    st.subheader("Recent quality issues")
    severity_filter = st.selectbox("Filter by severity", options=["All", "ERROR", "WARNING"])
    severity_param = None if severity_filter == "All" else severity_filter
    issues_df = _cached_recent_quality_issues(db_path_str, cache_key, severity_param)
    if issues_df.empty:
        st.info("No quality issues match this filter.")
    else:
        st.dataframe(issues_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page 4: Pipeline Health
# ---------------------------------------------------------------------------

def render_pipeline_health(db_path_str: str, cache_key: int):
    st.title("Pipeline Health")
    st.caption("Operational view of pipeline runs — the engineering side of this project.")

    kpis = _cached_pipeline_health_kpis(db_path_str, cache_key)
    if kpis["last_run_status"] is None:
        st.info("No pipeline runs recorded yet.")
        return

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Last run status", kpis["last_run_status"])
    col2.metric("Last successful run", kpis["last_success_run_id"] or "N/A")
    col3.metric("Rows processed (raw)", kpis["raw_rows_ingested"])
    col4.metric("Rows rejected", kpis["staging_rows_rejected"])
    col5.metric("Facts inserted", kpis["facts_inserted"])

    if kpis["duration_seconds"] is not None:
        st.metric("Latest run duration", f"{kpis['duration_seconds']:.2f} s")

    st.divider()
    st.subheader("Recent pipeline runs")
    runs_df = _cached_pipeline_runs(db_path_str, cache_key)
    st.dataframe(runs_df, use_container_width=True, hide_index=True)

    st.subheader("Rows processed by pipeline run")
    st.plotly_chart(charts.pipeline_run_metrics_chart(runs_df), use_container_width=True)

    st.divider()
    st.subheader("Failed runs")
    failed_df = _cached_failed_pipeline_runs(db_path_str, cache_key)
    if failed_df.empty:
        st.success("No failed pipeline runs recorded.")
    else:
        st.dataframe(failed_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page 5: Data Lineage
# ---------------------------------------------------------------------------

def render_data_lineage(db_path_str: str, cache_key: int):
    st.title("Data Lineage")
    st.caption(
        "Technical traceability page — answers \"where did this exact "
        "number come from?\" by following the fact → staging → raw → "
        "source-file chain."
    )

    fact_id_options = _cached_fact_id_options(db_path_str, cache_key)
    if not fact_id_options:
        st.info("No fact rows available yet — run the pipeline to populate data.")
        return

    fact_id = st.selectbox("Select fact_id", options=fact_id_options)
    lineage = _cached_lineage(db_path_str, cache_key, fact_id)
    if lineage is None:
        st.error("No lineage found for this fact_id.")
        return

    st.markdown("#### FACT")
    st.json({
        "fact_id": lineage["fact_id"],
        "meter_id": lineage["meter_id"],
        "reading_timestamp": str(lineage["reading_timestamp"]),
        "consumption_liters": lineage["consumption_liters"],
        "is_suspicious": bool(lineage["is_suspicious"]),
    })
    st.markdown("⬇️")

    st.markdown("#### STAGING")
    st.json({
        "stg_id": lineage["stg_id"],
        "raw_id": lineage["stg_raw_id"],
        "processed_at": lineage["processed_at"],
    })
    st.markdown("⬇️")

    st.markdown("#### RAW")
    st.json({
        "raw_id": lineage["raw_id"],
        "source_file": lineage["source_file"],
        "ingested_at": lineage["ingested_at"],
    })
    with st.expander("raw_payload (parsed row representation)"):
        st.code(lineage["raw_payload"] or "(empty)")
    st.markdown("⬇️")

    st.markdown("#### SOURCE FILE")
    st.json({
        "file_name": lineage["file_name"],
        "file_hash": lineage["file_hash"],
        "status": lineage["file_status"],
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    db_path_str = str(dd.DEFAULT_DASHBOARD_DB_PATH)

    if "cache_key" not in st.session_state:
        st.session_state["cache_key"] = 0
    cache_key = st.session_state["cache_key"]

    if not dd.DEFAULT_DASHBOARD_DB_PATH.exists():
        st.error(
            f"Database not found at `{db_path_str}`. Run `python main.py` "
            f"or `python main.py --demo` first to create it."
        )
        return

    page, filters = render_sidebar(db_path_str, cache_key)

    if page == "Overview":
        render_overview(db_path_str, cache_key, filters)
    elif page == "Meter Analysis":
        render_meter_analysis(db_path_str, cache_key)
    elif page == "Data Quality":
        render_data_quality(db_path_str, cache_key)
    elif page == "Pipeline Health":
        render_pipeline_health(db_path_str, cache_key)
    elif page == "Data Lineage":
        render_data_lineage(db_path_str, cache_key)


if __name__ == "__main__":
    main()
