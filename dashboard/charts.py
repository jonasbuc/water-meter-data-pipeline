"""
dashboard/charts.py
--------------------
Genbrugelige Plotly-chart-byggere. Holder `app.py` fokuseret på layout og
sidebar-logik, mens al chart-konfiguration (farver, titler, labels) samles
ét sted for konsistens - samme princip som at holde SQL ét sted i data.py.

Alle funktioner tager et pandas DataFrame og returnerer en Plotly Figure -
ingen af dem rører databasen.
"""

from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Restrained, professionelt farve-tema - undgår neon-farver og "gauge"-stil.
PRIMARY_COLOR = "#1f6feb"
SUSPICIOUS_COLOR = "#d1242f"
NORMAL_COLOR = "#1f6feb"
SEVERITY_COLORS = {"ERROR": "#d1242f", "WARNING": "#d4a72c"}

def _apply_default_layout(fig: go.Figure, title: Optional[str] = None,
                           xaxis_title: Optional[str] = None,
                           yaxis_title: Optional[str] = None) -> go.Figure:
    """
    Anvender det fælles, restrained layout-tema (hvid baggrund, kompakte
    margener, ensartet skriftstørrelse) på en figur. Skrevet med
    eksplicitte parametre i stedet for `fig.update_layout(**some_dict)`,
    fordi Plotly's type-stubs gør bred `**dict`-unpacking tvetydig for
    statiske type-checkere (falsk positiv, ikke en reel runtime-fejl).
    """
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=40, b=10),
        font=dict(size=13),
        title=title,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
    )
    return fig


def consumption_over_time_chart(df: pd.DataFrame) -> go.Figure:
    """Linjegraf: samlet forbrug pr. dag."""
    if df.empty:
        return _empty_figure("Intet forbrug at vise for de valgte filtre")
    fig = px.line(
        df, x="reading_date", y="total_liters",
        markers=True,
        labels={"reading_date": "Dato", "total_liters": "Forbrug (L)"},
    )
    fig.update_traces(line_color=PRIMARY_COLOR, hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f} L")
    return _apply_default_layout(fig, title="Water consumption over time")


def consumption_over_time_by_meter_chart(df: pd.DataFrame) -> go.Figure:
    """Linjegraf: forbrug pr. dag, opdelt pr. måler."""
    if df.empty:
        return _empty_figure("Intet forbrug at vise for de valgte filtre")
    fig = px.line(
        df, x="reading_date", y="total_liters", color="meter_id",
        markers=True,
        labels={"reading_date": "Dato", "total_liters": "Forbrug (L)", "meter_id": "Måler"},
    )
    return _apply_default_layout(fig, title="Water consumption over time (by meter)")


def top_meters_chart(df: pd.DataFrame) -> go.Figure:
    """Horisontal søjlediagram: top-N målere efter samlet forbrug."""
    if df.empty:
        return _empty_figure("Ingen målerdata for de valgte filtre")
    df_sorted = df.sort_values("total_liters", ascending=True)
    fig = px.bar(
        df_sorted, x="total_liters", y="meter_id", orientation="h",
        labels={"total_liters": "Samlet forbrug (L)", "meter_id": "Måler"},
        text="total_liters",
    )
    fig.update_traces(
        marker_color=PRIMARY_COLOR,
        texttemplate="%{text:,.0f} L",
        textposition="outside",
    )
    return _apply_default_layout(fig, title="Top meters by total consumption")


def status_breakdown_chart(df: pd.DataFrame) -> go.Figure:
    """Søjlediagram: antal målinger pr. status."""
    if df.empty:
        return _empty_figure("Ingen status-data for de valgte filtre")
    fig = px.bar(
        df, x="status", y="reading_count",
        labels={"status": "Status", "reading_count": "Antal målinger"},
        text="reading_count",
    )
    fig.update_traces(marker_color=PRIMARY_COLOR, textposition="outside")
    return _apply_default_layout(fig, title="Readings by status")


def meter_time_series_chart(df: pd.DataFrame, meter_id: str) -> go.Figure:
    """
    Linjegraf for én målers forbrug over tid, med mistænkelige målinger
    markeret som separate, fremhævede punkter (ikke bare en anden
    linjefarve, for tydelighed).
    """
    if df.empty:
        return _empty_figure(f"Ingen målinger for måler {meter_id}")
    df_sorted = df.sort_values("reading_timestamp")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_sorted["reading_timestamp"], y=df_sorted["consumption_liters"],
        mode="lines+markers", name="Consumption",
        line=dict(color=NORMAL_COLOR),
        hovertemplate="%{x|%Y-%m-%d %H:%M}<br>%{y:,.1f} L",
    ))
    suspicious_df = df_sorted[df_sorted["is_suspicious"] == 1]
    if not suspicious_df.empty:
        fig.add_trace(go.Scatter(
            x=suspicious_df["reading_timestamp"], y=suspicious_df["consumption_liters"],
            mode="markers", name="Suspicious reading",
            marker=dict(color=SUSPICIOUS_COLOR, size=11, symbol="diamond"),
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>%{y:,.1f} L (suspicious)",
        ))
    return _apply_default_layout(
        fig, title=f"Consumption over time — {meter_id}",
        xaxis_title="Timestamp", yaxis_title="Consumption (L)",
    )


def meter_temperature_chart(df: pd.DataFrame, meter_id: str) -> Optional[go.Figure]:
    """
    Separat temperatur-graf (IKKE dual-axis - se README/opgave-krav om at
    undgå forvirrende dobbelt-akse-grafer uden stærk grund). Returnerer
    None hvis temperatur mangler helt for denne måler.
    """
    temp_df = df.dropna(subset=["temperature"])
    if temp_df.empty:
        return None
    temp_df_sorted = temp_df.sort_values("reading_timestamp")
    fig = px.line(
        temp_df_sorted, x="reading_timestamp", y="temperature", markers=True,
        labels={"reading_timestamp": "Timestamp", "temperature": "Temperature (°C)"},
    )
    fig.update_traces(line_color="#9a6700")
    return _apply_default_layout(fig, title=f"Temperature over time — {meter_id}")


def quality_issues_by_type_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _empty_figure("Ingen datakvalitetsfejl registreret")
    df_sorted = df.sort_values("issue_count", ascending=True)
    fig = px.bar(
        df_sorted, x="issue_count", y="error_type", orientation="h",
        labels={"issue_count": "Antal", "error_type": "Fejltype"},
        text="issue_count",
    )
    fig.update_traces(marker_color=PRIMARY_COLOR, textposition="outside")
    return _apply_default_layout(fig, title="Issues by type")


def quality_severity_donut_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _empty_figure("Ingen datakvalitetsfejl registreret")
    colors = [SEVERITY_COLORS.get(s, "#8b949e") for s in df["severity"]]
    fig = go.Figure(data=[go.Pie(
        labels=df["severity"], values=df["issue_count"], hole=0.55,
        marker=dict(colors=colors),
    )])
    return _apply_default_layout(fig, title="Severity split")


def pipeline_run_metrics_chart(df: pd.DataFrame) -> go.Figure:
    """
    Søjlediagram over centrale rækketal pr. kørsel. Bevidst begrænset til
    et lille sæt sammenlignelige metrikker (rå/staging/fact) - blander IKKE
    fx duplicates_skipped ind, som ville gøre skalaen misvisende.
    """
    if df.empty:
        return _empty_figure("Ingen pipeline-kørsler registreret endnu")
    metrics = ["raw_rows_ingested", "staging_rows_inserted", "staging_rows_rejected", "facts_inserted"]
    plot_df = df[["run_id"] + metrics].melt(
        id_vars="run_id", var_name="metric", value_name="count"
    )
    fig = px.bar(
        plot_df, x="run_id", y="count", color="metric", barmode="group",
        labels={"run_id": "Run ID", "count": "Rows", "metric": "Metric"},
    )
    return _apply_default_layout(fig, title="Rows processed by pipeline run")


def _empty_figure(message: str) -> go.Figure:
    """Ensartet, læsbar "ingen data"-tilstand i stedet for en blank/broken chart."""
    fig = go.Figure()
    fig.add_annotation(
        text=message, xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False, font=dict(size=14, color="#57606a"),
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=40, b=10),
        font=dict(size=13),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig
