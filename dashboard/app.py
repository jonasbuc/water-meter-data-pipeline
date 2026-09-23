"""
dashboard/app.py
----------------
Streamlit BI dashboard — read-only presentation layer on top of the
analytics model (dim_meter + fact_water_consumption + pipeline_runs +
data_quality_errors).

This module never performs ingestion, validation or transformation. It
only queries and renders. All data access goes through ``dashboard.data``.

Run:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

from dashboard import charts, data as dd
from src.database import get_engine


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Water Analytics",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)

NAV_ITEMS = [
    ("Overview", "▣"),
    ("Meter Analysis", "◫"),
    ("Data Quality", "✓"),
    ("Pipeline Health", "↻"),
    ("Data Lineage", "⛓"),
]
PAGES = [name for name, _ in NAV_ITEMS]

PAGE_META = {
    "Overview": ("Operations / Overview", "Monitor consumption, meter activity and data quality."),
    "Meter Analysis": ("Operations / Meters", "Deep-dive into consumption behaviour and telemetry for a single meter."),
    "Data Quality": ("Operations / Quality", "See what was rejected, warned about and why."),
    "Pipeline Health": ("Operations / Pipeline", "Operational status and processing throughput."),
    "Data Lineage": ("Operations / Lineage", "Follow one analytics value all the way back to its source file."),
}

# Human-readable labels for machine-facing values. Database values are
# never altered — this is a display-only mapping.
ISSUE_LABELS = {
    "DUPLICATE_IN_BATCH": "Duplicate in batch",
    "INVALID_TIMESTAMP": "Invalid timestamp",
    "MISSING_METER_ID": "Missing meter ID",
    "NEGATIVE_CONSUMPTION": "Negative consumption",
    "MALFORMED_TEMPERATURE": "Malformed temperature",
    "UNKNOWN_METER": "Unknown meter",
}


def _display_issue_label(code: Optional[str]) -> str:
    if not code:
        return "—"
    return ISSUE_LABELS.get(str(code), str(code).replace("_", " ").title())


def _inject_css() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

        :root {
            --navy: #1B2B3A;
            --blue: #2F80ED;
            --blue-soft: #EAF2FF;
            --teal: #2BB3A3;
            --text: #1B2B3A;
            --muted: #6B7C93;
            --border: #E1E8F0;
            --surface: #FFFFFF;
            --bg: #F5F7FA;
            --red: #D95C5C;
            --red-soft: #FBEAEA;
            --amber: #D9A441;
            --amber-soft: #FBF3E3;
            --green: #2E9E5B;
            --green-soft: #E9F7EF;
        }

        html, body, [class*="css"] {
            font-family: 'Inter', 'Segoe UI', Arial, sans-serif;
        }

        /* ---- Hide Streamlit chrome ---- */
        #MainMenu { visibility: hidden; }
        footer { visibility: hidden; }
        header[data-testid="stHeader"] { background: transparent; height: 0; }
        div[data-testid="stToolbar"] { visibility: hidden; height: 0; }
        div[data-testid="stDecoration"] { display: none; }
        a[href*="streamlit.io"] { display: none !important; }

        .stApp {
            background: var(--bg);
            color: var(--text);
        }

        .block-container {
            max-width: 1440px;
            padding-top: 1.4rem;
            padding-bottom: 2.5rem;
        }

        hr { border-color: var(--border) !important; }

        /* ---- Sidebar ---- */
        [data-testid="stSidebar"] {
            background: #FFFFFF;
            border-right: 1px solid var(--border);
        }
        [data-testid="stSidebar"] .block-container { padding-top: 1.1rem; }

        .brand {
            padding: 0 .1rem 1rem;
            margin-bottom: .6rem;
            border-bottom: 1px solid var(--border);
        }
        .brand-title {
            font-size: 1.02rem;
            font-weight: 800;
            color: var(--navy);
            letter-spacing: -.01em;
        }
        .brand-sub {
            font-size: .74rem;
            color: var(--muted);
            margin-top: .1rem;
        }

        .nav-kicker, .filter-kicker {
            font-size: .68rem;
            font-weight: 700;
            letter-spacing: .09em;
            text-transform: uppercase;
            color: var(--muted);
            margin: .9rem 0 .35rem .1rem;
        }

        /* Custom nav buttons: strip default Streamlit button chrome */
        [data-testid="stSidebar"] div[data-testid="stButton"] button {
            width: 100%;
            text-align: left;
            background: transparent;
            border: 1px solid transparent;
            border-radius: 8px;
            color: var(--muted);
            font-weight: 500;
            font-size: .87rem;
            padding: .45rem .6rem;
            box-shadow: none;
            transition: background .12s ease;
        }
        [data-testid="stSidebar"] div[data-testid="stButton"] button:hover {
            background: var(--blue-soft);
            color: var(--blue);
            border-color: transparent;
        }
        [data-testid="stSidebar"] div[data-testid="stButton"] button:focus:not(:active) {
            box-shadow: none;
        }
        .nav-active button {
            background: var(--blue-soft) !important;
            color: var(--blue) !important;
            font-weight: 700 !important;
            border-left: 3px solid var(--blue) !important;
        }

        /* Compact filter widgets */
        [data-testid="stSidebar"] label {
            font-size: .74rem !important;
            font-weight: 600;
            color: var(--muted) !important;
            margin-bottom: .1rem;
        }
        [data-testid="stSidebar"] [data-baseweb="select"] > div,
        [data-testid="stSidebar"] [data-baseweb="input"] > div,
        [data-testid="stSidebar"] [data-baseweb="datepicker"] input {
            background: #FBFCFE;
            border-color: var(--border);
            font-size: .82rem;
            min-height: 2.1rem;
        }
        [data-testid="stSidebar"] [data-testid="stDateInput"] { margin-bottom: .2rem; }
        [data-testid="stSidebar"] .stMultiSelect, [data-testid="stSidebar"] .stSelectbox {
            margin-bottom: .15rem;
        }

        .data-source-box {
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: .55rem .7rem;
            background: #FBFCFE;
            margin-top: .3rem;
        }
        .data-source-title {
            font-size: .68rem;
            font-weight: 700;
            letter-spacing: .08em;
            text-transform: uppercase;
            color: var(--muted);
        }
        .data-source-status {
            font-size: .82rem;
            font-weight: 600;
            color: var(--green);
            margin-top: .2rem;
        }
        .data-source-status .dot {
            display: inline-block;
            width: 7px;
            height: 7px;
            border-radius: 999px;
            background: var(--green);
            margin-right: .35rem;
        }
        .data-source-name {
            font-size: .74rem;
            color: var(--muted);
            margin-top: .05rem;
        }

        /* ---- Page header ---- */
        .app-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            padding-bottom: .9rem;
            margin-bottom: 1.1rem;
            border-bottom: 1px solid var(--border);
        }
        .app-header .eyebrow {
            font-size: .68rem;
            font-weight: 700;
            letter-spacing: .1em;
            text-transform: uppercase;
            color: var(--blue);
            margin-bottom: .3rem;
        }
        .app-header h1 {
            margin: 0;
            font-size: 1.9rem;
            font-weight: 700;
            letter-spacing: -.02em;
            color: var(--navy);
            line-height: 1.15;
        }
        .app-header p {
            margin: .3rem 0 0;
            font-size: .86rem;
            color: var(--muted);
            max-width: 640px;
        }
        .app-header .meta {
            text-align: right;
            font-size: .72rem;
            color: var(--muted);
        }
        .app-header .meta b {
            display: block;
            font-size: .92rem;
            color: var(--navy);
            font-weight: 700;
        }

        /* ---- KPI cards ---- */
        .kpi-row { display: flex; gap: 14px; margin-bottom: 6px; flex-wrap: wrap; }
        .kpi-card {
            flex: 1 1 0;
            min-width: 150px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: .85rem 1rem;
            box-shadow: 0 1px 2px rgba(27,43,58,.04);
            min-height: 96px;
        }
        .kpi-label {
            font-size: .68rem;
            font-weight: 700;
            letter-spacing: .06em;
            text-transform: uppercase;
            color: var(--muted);
        }
        .kpi-value {
            font-size: 1.55rem;
            font-weight: 700;
            color: var(--navy);
            letter-spacing: -.02em;
            margin-top: .25rem;
            line-height: 1.2;
        }
        .kpi-context {
            font-size: .74rem;
            color: var(--muted);
            margin-top: .2rem;
        }
        .kpi-context.warn { color: var(--amber); }
        .kpi-context.bad { color: var(--red); }
        .kpi-context.good { color: var(--green); }

        /* ---- Section / card headers ---- */
        .section-block { margin-top: 1.6rem; margin-bottom: .6rem; }
        .section-title {
            font-size: 1.02rem;
            font-weight: 650;
            color: var(--navy);
            letter-spacing: -.01em;
        }
        .section-note {
            font-size: .8rem;
            color: var(--muted);
            margin-top: .1rem;
        }

        .chart-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: .9rem 1rem .3rem;
            box-shadow: 0 1px 2px rgba(27,43,58,.04);
            margin-bottom: 1rem;
        }
        .chart-card-title {
            font-size: .88rem;
            font-weight: 700;
            color: var(--navy);
        }
        .chart-card-sub {
            font-size: .74rem;
            color: var(--muted);
            margin-top: .05rem;
            margin-bottom: .3rem;
        }

        [data-testid="stPlotlyChart"] { margin-top: -.4rem; }

        [data-testid="stDataFrame"] {
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
        }

        .status-pill {
            display: inline-block;
            padding: .22rem .55rem;
            border-radius: 999px;
            font-size: .72rem;
            font-weight: 700;
            letter-spacing: .01em;
        }
        .pill-success { background: var(--green-soft); color: #1E7A43; }
        .pill-danger  { background: var(--red-soft); color: #B23B3B; }
        .pill-neutral { background: #EEF2F6; color: #617284; }

        .status-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: .9rem 1rem;
            box-shadow: 0 1px 2px rgba(27,43,58,.04);
        }
        .status-card-title {
            font-size: .68rem;
            font-weight: 700;
            letter-spacing: .08em;
            text-transform: uppercase;
            color: var(--muted);
            margin-bottom: .4rem;
        }
        .status-dot {
            display: inline-block;
            width: 9px;
            height: 9px;
            border-radius: 999px;
            margin-right: .4rem;
        }
        .status-dot.ok { background: var(--green); }
        .status-dot.bad { background: var(--red); }
        .status-line {
            font-size: .95rem;
            font-weight: 700;
            color: var(--navy);
        }
        .status-detail {
            font-size: .78rem;
            color: var(--muted);
            margin-top: .3rem;
        }

        .alert-box {
            background: var(--amber-soft);
            border: 1px solid #EFDDAF;
            border-left: 3px solid var(--amber);
            border-radius: 10px;
            padding: .65rem .85rem;
            color: #7A5E22;
            margin: .3rem 0 .9rem;
            font-size: .82rem;
        }

        /* ---- Pipeline flow ---- */
        .flow-row { display: flex; align-items: center; gap: 6px; }
        .flow-stage {
            flex: 1;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: .85rem .8rem;
            text-align: center;
            box-shadow: 0 1px 2px rgba(27,43,58,.04);
        }
        .flow-stage-label {
            font-size: .66rem;
            font-weight: 700;
            letter-spacing: .09em;
            text-transform: uppercase;
            color: var(--muted);
        }
        .flow-stage-value {
            font-size: 1.4rem;
            font-weight: 700;
            color: var(--navy);
            margin-top: .2rem;
        }
        .flow-stage-note {
            font-size: .7rem;
            color: var(--red);
            margin-top: .15rem;
            font-weight: 600;
        }
        .flow-arrow-h {
            color: #B7C4D2;
            font-size: 1.3rem;
            padding: 0 2px;
        }

        /* ---- Lineage ---- */
        .lineage-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: .9rem .95rem;
            min-height: 172px;
            box-shadow: 0 1px 2px rgba(27,43,58,.04);
        }
        .lineage-stage {
            color: var(--blue);
            font-size: .66rem;
            font-weight: 800;
            letter-spacing: .09em;
            text-transform: uppercase;
            margin-bottom: .45rem;
        }
        .lineage-row {
            display: flex;
            justify-content: space-between;
            gap: .7rem;
            padding: .22rem 0;
            border-bottom: 1px solid #EEF2F6;
            font-size: .76rem;
        }
        .lineage-row:last-child { border-bottom: 0; }
        .lineage-key { color: var(--muted); }
        .lineage-value {
            color: var(--navy);
            font-weight: 600;
            text-align: right;
            overflow-wrap: anywhere;
        }
        .flow-arrow {
            text-align: center;
            color: #B7C4D2;
            font-size: 1.4rem;
            padding-top: 4.2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Cache wrappers
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_cached_engine(db_path_str: str):
    return get_engine(db_path=Path(db_path_str))


@st.cache_data
def _cached_meter_list(db_path_str: str, _cache_key: int):
    return dd.load_meter_list(_get_cached_engine(db_path_str))


@st.cache_data
def _cached_status_list(db_path_str: str, _cache_key: int):
    return dd.load_status_list(_get_cached_engine(db_path_str))


@st.cache_data
def _cached_fact_data(db_path_str: str, _cache_key: int, filters_key: tuple):
    return dd.load_filtered_fact_data(
        _get_cached_engine(db_path_str),
        _filters_from_key(filters_key),
    )


@st.cache_data
def _cached_kpis(db_path_str: str, _cache_key: int, filters_key: tuple):
    return dd.load_kpis(_get_cached_engine(db_path_str), _filters_from_key(filters_key))


@st.cache_data
def _cached_consumption_over_time(db_path_str: str, _cache_key: int, filters_key: tuple):
    return dd.load_consumption_over_time(
        _get_cached_engine(db_path_str),
        _filters_from_key(filters_key),
    )


@st.cache_data
def _cached_consumption_over_time_by_meter(db_path_str: str, _cache_key: int, filters_key: tuple):
    return dd.load_consumption_over_time_by_meter(
        _get_cached_engine(db_path_str),
        _filters_from_key(filters_key),
    )


@st.cache_data
def _cached_top_meters(db_path_str: str, _cache_key: int, filters_key: tuple):
    return dd.load_top_meters(_get_cached_engine(db_path_str), _filters_from_key(filters_key))


@st.cache_data
def _cached_status_breakdown(db_path_str: str, _cache_key: int, filters_key: tuple):
    return dd.load_status_breakdown(
        _get_cached_engine(db_path_str),
        _filters_from_key(filters_key),
    )


@st.cache_data
def _cached_suspicious_readings(db_path_str: str, _cache_key: int, filters_key: tuple):
    return dd.load_suspicious_readings(
        _get_cached_engine(db_path_str),
        _filters_from_key(filters_key),
    )


@st.cache_data
def _cached_meter_readings(db_path_str: str, _cache_key: int, meter_id: str):
    return dd.load_meter_readings(_get_cached_engine(db_path_str), meter_id)


@st.cache_data
def _cached_meter_detail_kpis(db_path_str: str, _cache_key: int, meter_id: str):
    return dd.load_meter_detail_kpis(_get_cached_engine(db_path_str), meter_id)


@st.cache_data
def _cached_quality_kpis(db_path_str: str, _cache_key: int):
    return dd.load_quality_kpis(_get_cached_engine(db_path_str))


@st.cache_data
def _cached_quality_by_type(db_path_str: str, _cache_key: int):
    return dd.load_quality_issues_by_type(_get_cached_engine(db_path_str))


@st.cache_data
def _cached_quality_severity(db_path_str: str, _cache_key: int):
    return dd.load_quality_severity_split(_get_cached_engine(db_path_str))


@st.cache_data
def _cached_recent_quality_issues(db_path_str: str, _cache_key: int, severity: Optional[str]):
    return dd.load_recent_quality_issues(
        _get_cached_engine(db_path_str),
        severity=severity,
    )


@st.cache_data
def _cached_pipeline_health_kpis(db_path_str: str, _cache_key: int):
    return dd.load_pipeline_health_kpis(_get_cached_engine(db_path_str))


@st.cache_data
def _cached_pipeline_runs(db_path_str: str, _cache_key: int):
    return dd.load_pipeline_runs(_get_cached_engine(db_path_str))


@st.cache_data
def _cached_failed_pipeline_runs(db_path_str: str, _cache_key: int):
    return dd.load_failed_pipeline_runs(_get_cached_engine(db_path_str))


@st.cache_data
def _cached_fact_id_options(db_path_str: str, _cache_key: int):
    return dd.load_fact_id_options(_get_cached_engine(db_path_str))


@st.cache_data
def _cached_lineage(db_path_str: str, _cache_key: int, fact_id: int):
    return dd.load_lineage(_get_cached_engine(db_path_str), fact_id)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _filters_from_key(filters_key: tuple) -> dd.DashboardFilters:
    start_date, end_date, meter_ids, status, suspicious_only = filters_key
    return dd.DashboardFilters(
        start_date=start_date,
        end_date=end_date,
        meter_ids=list(meter_ids),
        status=status,
        suspicious_only=suspicious_only,
    )


def _filters_to_key(filters: dd.DashboardFilters) -> tuple:
    return (
        filters.start_date,
        filters.end_date,
        tuple(sorted(filters.meter_ids)),
        filters.status,
        filters.suspicious_only,
    )


def _format_liters(value: float) -> str:
    return f"{value:,.0f} L"


def _format_liters_per_reading(value: float) -> str:
    return f"{value:,.0f} L / reading"


def _format_percent(value: Optional[float]) -> str:
    return "N/A" if value is None else f"{value * 100:,.1f}%"


def _format_timestamp(value: Any) -> str:
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return "—"
    return ts.strftime("%d %b %Y · %H:%M")


def _render_header(page: str) -> None:
    eyebrow, subtitle = PAGE_META[page]
    now = pd.Timestamp.now().strftime("%H:%M")
    st.markdown(
        f"""
        <div class="app-header">
            <div>
                <div class="eyebrow">{eyebrow}</div>
                <h1>{page}</h1>
                <p>{subtitle}</p>
            </div>
            <div class="meta">
                Last refresh
                <b>{now}</b>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _kpi_card_html(label: str, value: str, context: str = "", tone: str = "") -> str:
    context_html = f'<div class="kpi-context {tone}">{context}</div>' if context else ""
    return (
        '<div class="kpi-card">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f"{context_html}"
        "</div>"
    )


def _render_kpi_row(cards: list[tuple[str, str, str, str]]) -> None:
    """cards: list of (label, value, context, tone)"""
    html = '<div class="kpi-row">' + "".join(_kpi_card_html(*c) for c in cards) + "</div>"
    st.markdown(html, unsafe_allow_html=True)


def _chart_card_open(title: str, subtitle: str = "") -> None:
    sub_html = f'<div class="chart-card-sub">{subtitle}</div>' if subtitle else '<div style="margin-bottom:.35rem"></div>'
    st.markdown(
        f"""
        <div class="chart-card">
            <div class="chart-card-title">{title}</div>
            {sub_html}
        """,
        unsafe_allow_html=True,
    )


def _chart_card_close() -> None:
    st.markdown("</div>", unsafe_allow_html=True)


def _section(title: str, note: Optional[str] = None) -> None:
    note_html = f'<div class="section-note">{note}</div>' if note else ""
    st.markdown(
        f"""
        <div class="section-block">
            <div class="section-title">{title}</div>
            {note_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _status_html(status: Optional[str]) -> str:
    if not status:
        return '<span class="status-pill pill-neutral">N/A</span>'
    css = "pill-success" if status == "SUCCESS" else "pill-danger" if status == "FAILED" else "pill-neutral"
    return f'<span class="status-pill {css}">{status}</span>'


def _lineage_card(stage: str, rows: list[tuple[str, Any]]) -> str:
    body = "".join(
        (
            '<div class="lineage-row">'
            f'<span class="lineage-key">{key}</span>'
            f'<span class="lineage-value">{value if value is not None else "—"}</span>'
            "</div>"
        )
        for key, value in rows
    )
    return f'<div class="lineage-card"><div class="lineage-stage">{stage}</div>{body}</div>'


PLOTLY_CONFIG = {"displayModeBar": False, "responsive": True}


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar(db_path_str: str, cache_key: int):
    st.sidebar.markdown(
        """
        <div class="brand">
            <div class="brand-title">💧 WATER ANALYTICS</div>
            <div class="brand-sub">Operations intelligence</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if "active_page" not in st.session_state:
        st.session_state["active_page"] = "Overview"

    st.sidebar.markdown('<div class="nav-kicker">Navigation</div>', unsafe_allow_html=True)
    for name, icon in NAV_ITEMS:
        is_active = st.session_state["active_page"] == name
        css_class = "nav-active" if is_active else "nav-inactive"
        st.sidebar.markdown(f'<div class="{css_class}">', unsafe_allow_html=True)
        if st.sidebar.button(f"{icon}  {name}", key=f"nav_{name}", width="stretch"):
            st.session_state["active_page"] = name
        st.sidebar.markdown("</div>", unsafe_allow_html=True)
    page = st.session_state["active_page"]

    st.sidebar.markdown('<div class="filter-kicker">Filters</div>', unsafe_allow_html=True)

    all_meters = _cached_meter_list(db_path_str, cache_key)
    all_statuses = _cached_status_list(db_path_str, cache_key)

    unfiltered = _cached_fact_data(
        db_path_str,
        cache_key,
        _filters_to_key(dd.DashboardFilters()),
    )

    if not unfiltered.empty:
        min_date = unfiltered["reading_timestamp"].min().date()
        max_date = unfiltered["reading_timestamp"].max().date()
        date_range = st.sidebar.date_input(
            "Date",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date, end_date = min_date, max_date
    else:
        start_date = end_date = None
        st.sidebar.caption("No readings available for date filtering.")

    meter_selection = st.sidebar.multiselect(
        "Meter",
        options=all_meters,
        default=[],
        placeholder="All meters",
    )

    status_selection = st.sidebar.selectbox(
        "Status",
        options=["All"] + all_statuses,
    )
    status_filter = None if status_selection == "All" else status_selection

    suspicious_selection = st.sidebar.segmented_control(
        "Threshold",
        options=["All", "Normal", "Exceeded"],
        default="All",
    )
    suspicious_filter = {
        "All": None,
        "Normal": False,
        "Exceeded": True,
    }[suspicious_selection or "All"]

    if st.sidebar.button("Reset filters", width="stretch"):
        st.cache_data.clear()
        st.session_state["cache_key"] = st.session_state.get("cache_key", 0) + 1
        st.rerun()

    st.sidebar.markdown("<div style='margin-top:.7rem'></div>", unsafe_allow_html=True)
    st.sidebar.markdown('<div class="filter-kicker">Data source</div>', unsafe_allow_html=True)
    db_name = Path(db_path_str).name
    st.sidebar.markdown(
        f"""
        <div class="data-source-box">
            <div class="data-source-status"><span class="dot"></span>Connected</div>
            <div class="data-source-name">{db_name}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.sidebar.expander("Data source details"):
        st.caption(db_path_str)
        if st.button("Refresh data", width="stretch", type="primary"):
            st.cache_data.clear()
            st.session_state["cache_key"] = st.session_state.get("cache_key", 0) + 1
            st.rerun()

    filters = dd.DashboardFilters(
        start_date=start_date,
        end_date=end_date,
        meter_ids=meter_selection,
        status=status_filter,
        suspicious_only=suspicious_filter,
    )
    return page, filters


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

def render_overview(db_path_str: str, cache_key: int, filters: dd.DashboardFilters) -> None:
    _render_header("Overview")
    filters_key = _filters_to_key(filters)
    kpis = _cached_kpis(db_path_str, cache_key, filters_key)

    _render_kpi_row(
        [
            ("Total consumption", _format_liters(kpis["total_consumption_liters"]), "", ""),
            ("Average reading", _format_liters_per_reading(kpis["avg_consumption_liters"]), "", ""),
            ("Active meters", f'{kpis["active_meters"]:,}', "", ""),
            (
                "Threshold exceeded",
                f'{kpis["suspicious_readings"]:,}',
                "readings above threshold" if kpis["suspicious_readings"] else "no exceedances",
                "warn" if kpis["suspicious_readings"] else "good",
            ),
            (
                "Data quality rate",
                _format_percent(kpis["data_quality_rate"]),
                "accepted vs. validated",
                "good" if (kpis["data_quality_rate"] or 0) >= 0.95 else "warn",
            ),
        ]
    )

    _section(
        "Consumption trend",
        "Daily interval consumption across the selected meters and date range.",
    )
    view_mode = st.segmented_control(
        "Consumption view",
        options=["Portfolio total", "By meter"],
        default="Portfolio total",
        label_visibility="collapsed",
    )

    _chart_card_open(
        "Consumption over time",
        "Portfolio total" if view_mode != "By meter" else "Split by individual meter",
    )
    if view_mode == "By meter":
        ts_df = _cached_consumption_over_time_by_meter(db_path_str, cache_key, filters_key)
        fig = charts.consumption_over_time_by_meter_chart(ts_df)
    else:
        ts_df = _cached_consumption_over_time(db_path_str, cache_key, filters_key)
        fig = charts.consumption_over_time_chart(ts_df)
    st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)
    _chart_card_close()

    left, right = st.columns([1.12, 0.88], gap="medium")
    with left:
        _chart_card_open("Top meters", "Ranked by total consumption in litres")
        top_df = _cached_top_meters(db_path_str, cache_key, filters_key)
        st.plotly_chart(charts.top_meters_chart(top_df), width="stretch", config=PLOTLY_CONFIG)
        _chart_card_close()
    with right:
        _chart_card_open("Reading status", "Share of readings by processing status")
        status_df = _cached_status_breakdown(db_path_str, cache_key, filters_key)
        st.plotly_chart(charts.status_breakdown_chart(status_df), width="stretch", config=PLOTLY_CONFIG)
        _chart_card_close()

    _section(
        "Attention required",
        "High-consumption readings above the configured demonstration threshold — not confirmed leaks.",
    )
    st.markdown(
        """
        <div class="alert-box">
            The anomaly rule is intentionally simple and illustrative. In production this would typically
            be replaced by meter-specific baselines, seasonality or statistical anomaly detection.
        </div>
        """,
        unsafe_allow_html=True,
    )

    suspicious_df = _cached_suspicious_readings(db_path_str, cache_key, filters_key)
    if suspicious_df.empty:
        st.success("No threshold exceedances for the selected filters.")
    else:
        st.dataframe(
            suspicious_df,
            width="stretch",
            hide_index=True,
            column_config={
                "meter_id": st.column_config.TextColumn("Meter"),
                "reading_timestamp": st.column_config.DatetimeColumn("Timestamp", format="DD MMM YYYY · HH:mm"),
                "consumption_liters": st.column_config.NumberColumn("Consumption", format="%.1f L"),
                "temperature": st.column_config.NumberColumn("Temperature", format="%.1f °C"),
                "status": st.column_config.TextColumn("Status"),
            },
        )


# ---------------------------------------------------------------------------
# Meter analysis
# ---------------------------------------------------------------------------

def render_meter_analysis(db_path_str: str, cache_key: int) -> None:
    _render_header("Meter Analysis")

    meters = _cached_meter_list(db_path_str, cache_key)
    if not meters:
        st.info("No meters available yet. Run the pipeline to populate analytics data.")
        return

    meter_id = st.selectbox("Meter", options=meters)
    kpis = _cached_meter_detail_kpis(db_path_str, cache_key, meter_id)

    _render_kpi_row(
        [
            ("Total consumption", _format_liters(kpis["total_liters"]), "", ""),
            ("Average reading", _format_liters_per_reading(kpis["avg_liters"]), "", ""),
            ("Peak reading", _format_liters(kpis["max_liters"]), "", ""),
            ("Readings", f'{kpis["reading_count"]:,}', "", ""),
            (
                "Threshold exceeded",
                f'{kpis["suspicious_count"]:,}',
                "",
                "warn" if kpis["suspicious_count"] else "good",
            ),
        ]
    )

    readings = _cached_meter_readings(db_path_str, cache_key, meter_id)
    if readings.empty:
        st.info("No readings recorded for this meter.")
        return

    _section("Consumption profile", f"{meter_id} · {len(readings):,} readings")
    _chart_card_open("Consumption profile", f"{meter_id} · {len(readings):,} readings")
    st.plotly_chart(
        charts.meter_time_series_chart(readings, meter_id),
        width="stretch",
        config=PLOTLY_CONFIG,
    )
    _chart_card_close()

    temp_fig = charts.meter_temperature_chart(readings, meter_id)
    if temp_fig is not None:
        _chart_card_open("Temperature profile", f"{meter_id}")
        st.plotly_chart(temp_fig, width="stretch", config=PLOTLY_CONFIG)
        _chart_card_close()

    _section("Reading history", "Newest readings first.")
    display_df = readings.sort_values("reading_timestamp", ascending=False)[
        ["reading_timestamp", "consumption_liters", "temperature", "status", "is_suspicious"]
    ].copy()
    display_df["is_suspicious"] = display_df["is_suspicious"].map({1: "Exceeded", 0: "Normal"})

    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "reading_timestamp": st.column_config.DatetimeColumn("Timestamp", format="DD MMM YYYY · HH:mm"),
            "consumption_liters": st.column_config.NumberColumn("Consumption", format="%.1f L"),
            "temperature": st.column_config.NumberColumn("Temperature", format="%.1f °C"),
            "status": st.column_config.TextColumn("Status"),
            "is_suspicious": st.column_config.TextColumn("Threshold"),
        },
    )


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------

def render_data_quality(db_path_str: str, cache_key: int) -> None:
    _render_header("Data Quality")
    kpis = _cached_quality_kpis(db_path_str, cache_key)

    _render_kpi_row(
        [
            ("Issues", f'{kpis["total_issues"]:,}', "", ""),
            ("Errors", f'{kpis["errors"]:,}', "rejected", "bad" if kpis["errors"] else "good"),
            ("Warnings", f'{kpis["warnings"]:,}', "accepted with notice", "warn" if kpis["warnings"] else "good"),
            ("Affected meters", f'{kpis["affected_meters"]:,}', "", ""),
            ("Affected sources", f'{kpis["affected_files"]:,}', "", ""),
        ]
    )

    if kpis["total_issues"] == 0:
        st.success("No data quality issues recorded.")
        return

    left, right = st.columns([1.15, 0.85], gap="medium")
    with left:
        _chart_card_open("Issues by type", "Count of quality events grouped by rule")
        by_type = _cached_quality_by_type(db_path_str, cache_key).copy()
        if not by_type.empty and "error_type" in by_type.columns:
            by_type["error_type"] = by_type["error_type"].map(_display_issue_label)
        st.plotly_chart(charts.quality_issues_by_type_chart(by_type), width="stretch", config=PLOTLY_CONFIG)
        _chart_card_close()
    with right:
        _chart_card_open("Severity", "Errors vs. warnings")
        st.plotly_chart(
            charts.quality_severity_donut_chart(_cached_quality_severity(db_path_str, cache_key)),
            width="stretch",
            config=PLOTLY_CONFIG,
        )
        _chart_card_close()

    _section(
        "Recent issues",
        "Errors were rejected; warnings were accepted but kept visible for observability.",
    )
    severity_filter = st.segmented_control(
        "Severity",
        options=["All", "ERROR", "WARNING"],
        default="All",
        label_visibility="collapsed",
    )
    severity = None if severity_filter == "All" else severity_filter
    issues = _cached_recent_quality_issues(db_path_str, cache_key, severity)

    if issues.empty:
        st.info("No quality issues match this filter.")
    else:
        display_issues = issues.copy()
        if "error_type" in display_issues.columns:
            display_issues["error_type"] = display_issues["error_type"].map(_display_issue_label)
        st.dataframe(
            display_issues,
            width="stretch",
            hide_index=True,
            column_config={
                "severity": st.column_config.TextColumn("Severity"),
                "error_type": st.column_config.TextColumn("Issue"),
                "meter_id": st.column_config.TextColumn("Meter"),
                "reading_timestamp": st.column_config.TextColumn("Reading timestamp"),
                "source_file": st.column_config.TextColumn("Source file"),
                "error_detail": st.column_config.TextColumn("Detail", width="large"),
                "detected_at": st.column_config.DatetimeColumn("Detected", format="DD MMM YYYY · HH:mm"),
            },
        )


# ---------------------------------------------------------------------------
# Pipeline health
# ---------------------------------------------------------------------------

def render_pipeline_health(db_path_str: str, cache_key: int) -> None:
    _render_header("Pipeline Health")
    kpis = _cached_pipeline_health_kpis(db_path_str, cache_key)

    if kpis["last_run_status"] is None:
        st.info("No pipeline runs recorded yet.")
        return

    is_healthy = kpis["last_run_status"] == "SUCCESS"
    dot_class = "ok" if is_healthy else "bad"
    health_label = "Healthy" if is_healthy else "Attention needed"
    duration_txt = f'{kpis["duration_seconds"]:.2f} sec' if kpis["duration_seconds"] is not None else "N/A"

    st.markdown(
        f"""
        <div class="status-card">
            <div class="status-card-title">Current status</div>
            <div class="status-line"><span class="status-dot {dot_class}"></span>{health_label}</div>
            <div class="status-detail">Last run: {_status_html(kpis["last_run_status"])} · Duration: {duration_txt}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div style='margin-top:.9rem'></div>", unsafe_allow_html=True)
    _render_kpi_row(
        [
            ("Raw ingested", f'{kpis["raw_rows_ingested"]:,}', "", ""),
            ("Validated", f'{kpis["raw_rows_ingested"]:,}', "passed schema checks", ""),
            (
                "Accepted",
                f'{kpis["raw_rows_ingested"] - kpis["staging_rows_rejected"]:,}',
                "into staging",
                "good",
            ),
            (
                "Rejected",
                f'{kpis["staging_rows_rejected"]:,}',
                "failed validation",
                "bad" if kpis["staging_rows_rejected"] else "good",
            ),
            ("Published to analytics", f'{kpis["facts_inserted"]:,}', "facts inserted", "good"),
        ]
    )

    runs = _cached_pipeline_runs(db_path_str, cache_key)

    _section("Pipeline flow", "Row counts moving through each processing stage for the latest run.")
    accepted = kpis["raw_rows_ingested"] - kpis["staging_rows_rejected"]
    stage_cols = st.columns([1, 0.12, 1, 0.12, 1, 0.12, 1], gap="small")
    stage_defs = [
        ("SOURCE", kpis["raw_rows_ingested"], None),
        ("RAW", kpis["raw_rows_ingested"], None),
        ("STAGING", accepted, f'{kpis["staging_rows_rejected"]:,} rejected' if kpis["staging_rows_rejected"] else None),
        ("ANALYTICS", kpis["facts_inserted"], None),
    ]
    for i, (stage_label, value, note) in enumerate(stage_defs):
        with stage_cols[i * 2]:
            note_html = f'<div class="flow-stage-note">{note}</div>' if note else ""
            st.markdown(
                f"""
                <div class="flow-stage">
                    <div class="flow-stage-label">{stage_label}</div>
                    <div class="flow-stage-value">{value:,}</div>
                    {note_html}
                </div>
                """,
                unsafe_allow_html=True,
            )
    for i in (1, 3, 5):
        with stage_cols[i]:
            st.markdown('<div class="flow-arrow-h" style="padding-top:1.6rem;text-align:center">→</div>', unsafe_allow_html=True)

    _section("Processing history", "Throughput across recent pipeline runs.")
    if len(runs) <= 1:
        _chart_card_open("Pipeline stages", "Single run recorded — showing stage breakdown instead of a trend.")
        stages = [
            ("Raw ingested", int(kpis["raw_rows_ingested"])),
            ("Accepted", int(accepted)),
            ("Rejected", int(kpis["staging_rows_rejected"])),
            ("Published", int(kpis["facts_inserted"])),
        ]
        st.plotly_chart(charts.pipeline_stage_flow_chart(stages), width="stretch", config=PLOTLY_CONFIG)
        _chart_card_close()
    else:
        _chart_card_open("Run history", "Raw, accepted, rejected and published rows per run.")
        st.plotly_chart(charts.pipeline_run_metrics_chart(runs), width="stretch", config=PLOTLY_CONFIG)
        _chart_card_close()

    _section("Recent runs", "Operational audit trail.")
    display_runs = runs.copy()
    if "start_time" in display_runs.columns:
        display_runs["start_time"] = display_runs["start_time"].map(_format_timestamp)
    if "end_time" in display_runs.columns:
        display_runs["end_time"] = display_runs["end_time"].map(_format_timestamp)
    rename_map = {
        "run_id": "Run",
        "start_time": "Started",
        "end_time": "Ended",
        "status": "Status",
        "raw_rows_ingested": "Raw rows",
        "staging_rows_inserted": "Accepted",
        "staging_rows_rejected": "Rejected",
        "facts_inserted": "Published",
    }
    display_runs = display_runs.rename(columns=rename_map)
    st.dataframe(display_runs, width="stretch", hide_index=True)

    failed = _cached_failed_pipeline_runs(db_path_str, cache_key)
    if not failed.empty:
        _section("Failed runs", "Only failed executions appear here.")
        st.dataframe(failed.rename(columns=rename_map), width="stretch", hide_index=True)


# ---------------------------------------------------------------------------
# Data lineage
# ---------------------------------------------------------------------------

def render_data_lineage(db_path_str: str, cache_key: int) -> None:
    _render_header("Data Lineage")

    fact_ids = _cached_fact_id_options(db_path_str, cache_key)
    if not fact_ids:
        st.info("No fact rows available yet.")
        return

    fact_id = st.selectbox("Select fact record", options=fact_ids)
    lineage = _cached_lineage(db_path_str, cache_key, fact_id)
    if lineage is None:
        st.error("No lineage found for this fact record.")
        return

    _section(
        "End-to-end trace",
        "The same value followed from the analytics fact table back to the original source file.",
    )

    cols = st.columns([1, 0.12, 1, 0.12, 1, 0.12, 1], gap="small")

    with cols[0]:
        st.markdown(
            _lineage_card(
                "Fact",
                [
                    ("fact_id", lineage["fact_id"]),
                    ("meter", lineage["meter_id"]),
                    ("timestamp", lineage["reading_timestamp"]),
                    ("consumption", f'{lineage["consumption_liters"]:,.1f} L'),
                    ("threshold", "Exceeded" if lineage["is_suspicious"] else "Normal"),
                ],
            ),
            unsafe_allow_html=True,
        )

    for idx in (1, 3, 5):
        with cols[idx]:
            st.markdown('<div class="flow-arrow">→</div>', unsafe_allow_html=True)

    with cols[2]:
        st.markdown(
            _lineage_card(
                "Staging",
                [
                    ("stg_id", lineage["stg_id"]),
                    ("raw_id", lineage["stg_raw_id"]),
                    ("processed", lineage["processed_at"]),
                ],
            ),
            unsafe_allow_html=True,
        )

    with cols[4]:
        st.markdown(
            _lineage_card(
                "Raw",
                [
                    ("raw_id", lineage["raw_id"]),
                    ("source", lineage["source_file"]),
                    ("ingested", lineage["ingested_at"]),
                ],
            ),
            unsafe_allow_html=True,
        )

    with cols[6]:
        file_hash = lineage["file_hash"] or "—"
        st.markdown(
            _lineage_card(
                "Source file",
                [
                    ("file", lineage["file_name"]),
                    ("status", lineage["file_status"]),
                    ("SHA-256", f"{file_hash[:14]}…" if len(file_hash) > 14 else file_hash),
                ],
            ),
            unsafe_allow_html=True,
        )

    st.markdown("")
    with st.expander("Inspect raw payload"):
        st.code(lineage["raw_payload"] or "(empty)", language="json")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    _inject_css()

    db_path_str = str(dd.DEFAULT_DASHBOARD_DB_PATH)
    if "cache_key" not in st.session_state:
        st.session_state["cache_key"] = 0
    cache_key = st.session_state["cache_key"]

    if not dd.DEFAULT_DASHBOARD_DB_PATH.exists():
        st.error(
            f"Database not found at `{db_path_str}`. "
            "Run the pipeline or point DASHBOARD_DB_PATH at a populated database."
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
