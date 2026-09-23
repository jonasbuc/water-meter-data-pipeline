"""
dashboard/charts.py
-------------------
Polerede, genbrugelige Plotly-chart builders til BI-dashboardet.

Målet er et roligt Power BI-lignende udtryk:
- ensartede akser, gridlines og hover labels
- tydelig visuel prioritering
- ingen neonfarver eller 3D/gauge-gimmicks
- anomalies/ERROR bruger accentfarver sparsomt
- funktionerne er rene: DataFrame ind -> Figure ud
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# ---------------------------------------------------------------------------
# Design tokens
# ---------------------------------------------------------------------------

NAVY = "#102A43"
BLUE = "#2F80ED"
BLUE_LIGHT = "#EAF2FF"
CYAN = "#56CCF2"
TEAL = "#2BB3A3"
GREEN = "#27AE60"
AMBER = "#D9A441"
RED = "#D95C5C"
PURPLE = "#7B61FF"

TEXT = "#1B2B3A"
MUTED = "#6B7C93"
GRID = "#EEF2F6"
BORDER = "#E1E8F0"
SURFACE = "#FFFFFF"
PLOT_BG = "rgba(0,0,0,0)"

SERIES = [BLUE, TEAL, PURPLE, AMBER, CYAN, GREEN]
SEVERITY_COLORS = {"ERROR": RED, "WARNING": AMBER}

# Titles are rendered by the surrounding card in app.py, not inside the
# figure itself, so charts never show a duplicated heading.


def _apply_default_layout(
    fig: go.Figure,
    *,
    title: Optional[str] = None,
    xaxis_title: Optional[str] = None,
    yaxis_title: Optional[str] = None,
    height: int = 340,
    showlegend: Optional[bool] = None,
) -> go.Figure:
    """Shared, restrained BI look for every figure. Backgrounds are
    transparent so the chart visually belongs to the surrounding card
    rather than owning its own surface."""
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=height,
        margin=dict(l=8, r=8, t=10, b=8),
        font=dict(
            family="Inter, Segoe UI, Arial, sans-serif",
            size=12,
            color=TEXT,
        ),
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        hoverlabel=dict(
            bgcolor="#FFFFFF",
            bordercolor=BORDER,
            font=dict(color=TEXT, size=12),
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            title_text="",
            font=dict(size=11, color=MUTED),
        ),
        showlegend=showlegend,
    )

    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        linecolor=BORDER,
        tickfont=dict(color=MUTED),
        title_font=dict(color=MUTED, size=11),
        automargin=True,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=GRID,
        gridwidth=1,
        zeroline=False,
        linecolor=BORDER,
        tickfont=dict(color=MUTED),
        title_font=dict(color=MUTED, size=11),
        automargin=True,
    )
    return fig


def consumption_over_time_chart(df: pd.DataFrame) -> go.Figure:
    """Samlet dagligt forbrug med diskret area-fill."""
    if df.empty:
        return _empty_figure("No consumption data for the selected filters")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["reading_date"],
            y=df["total_liters"],
            mode="lines",
            name="Consumption",
            line=dict(color=BLUE, width=2.5, shape="spline", smoothing=0.3),
            fill="tozeroy",
            fillcolor="rgba(47, 128, 237, 0.08)",
            hovertemplate="<b>%{x|%d %b %Y}</b><br>%{y:,.0f} L<extra></extra>",
        )
    )

    return _apply_default_layout(
        fig,
        yaxis_title="Litres",
        height=360,
        showlegend=False,
    )


def consumption_over_time_by_meter_chart(df: pd.DataFrame) -> go.Figure:
    """Dagligt forbrug opdelt pr. måler."""
    if df.empty:
        return _empty_figure("No consumption data for the selected filters")

    fig = px.line(
        df,
        x="reading_date",
        y="total_liters",
        color="meter_id",
        color_discrete_sequence=SERIES,
        markers=False,
        labels={
            "reading_date": "",
            "total_liters": "Litres",
            "meter_id": "Meter",
        },
    )
    fig.update_traces(
        line=dict(width=2.2),
        hovertemplate="<b>%{fullData.name}</b><br>%{x|%d %b %Y}<br>%{y:,.0f} L<extra></extra>",
    )

    return _apply_default_layout(
        fig,
        yaxis_title="Litres",
        height=360,
        showlegend=True,
    )


def top_meters_chart(df: pd.DataFrame) -> go.Figure:
    """Top-N målere med kompakt horisontal bar chart."""
    if df.empty:
        return _empty_figure("No meter data for the selected filters")

    plot_df = df.sort_values("total_liters", ascending=True).copy()
    max_val = plot_df["total_liters"].max()
    bar_colors = [BLUE if v < max_val else NAVY for v in plot_df["total_liters"]]

    fig = go.Figure(
        go.Bar(
            x=plot_df["total_liters"],
            y=plot_df["meter_id"],
            orientation="h",
            marker=dict(color=bar_colors, cornerradius=4),
            text=plot_df["total_liters"],
            texttemplate="%{text:,.0f} L",
            textposition="outside",
            textfont=dict(size=11, color=MUTED),
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>%{x:,.0f} L<extra></extra>",
        )
    )

    fig = _apply_default_layout(
        fig,
        xaxis_title="Litres",
        height=310,
        showlegend=False,
    )
    fig.update_yaxes(showgrid=False)
    fig.update_xaxes(showgrid=True, gridcolor=GRID)
    return fig


def status_breakdown_chart(df: pd.DataFrame) -> go.Figure:
    """Statusmix som donut; bedre egnet end bars når kategorierne udgør ét samlet antal."""
    if df.empty:
        return _empty_figure("No status data for the selected filters")

    colors = SERIES[: max(len(df), 1)]
    fig = go.Figure(
        data=[
            go.Pie(
                labels=df["status"],
                values=df["reading_count"],
                hole=0.72,
                sort=False,
                marker=dict(colors=colors, line=dict(color="#FFFFFF", width=2)),
                textinfo="percent",
                textfont=dict(size=11, color=TEXT),
                hovertemplate="<b>%{label}</b><br>%{value:,} readings<br>%{percent}<extra></extra>",
            )
        ]
    )

    total = int(df["reading_count"].sum())
    fig.add_annotation(
        x=0.5,
        y=0.5,
        text=f"<b>{total:,}</b><br><span style='font-size:10px;color:{MUTED}'>readings</span>",
        showarrow=False,
        align="center",
        font=dict(size=18, color=NAVY),
    )

    return _apply_default_layout(
        fig,
        height=300,
        showlegend=True,
    )


def meter_time_series_chart(df: pd.DataFrame, meter_id: str) -> go.Figure:
    """Forbrug for én måler, med anomalies som tydelige men kontrollerede highlights."""
    if df.empty:
        return _empty_figure(f"No readings for meter {meter_id}")

    plot_df = df.sort_values("reading_timestamp").copy()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=plot_df["reading_timestamp"],
            y=plot_df["consumption_liters"],
            mode="lines",
            name="Consumption",
            line=dict(color=BLUE, width=2.4),
            fill="tozeroy",
            fillcolor="rgba(47, 128, 237, 0.06)",
            hovertemplate="%{x|%d %b %Y %H:%M}<br><b>%{y:,.1f} L</b><extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=plot_df["reading_timestamp"],
            y=plot_df["consumption_liters"],
            mode="markers",
            name="Reading",
            marker=dict(size=5, color=BLUE, opacity=0.65),
            hovertemplate="%{x|%d %b %Y %H:%M}<br><b>%{y:,.1f} L</b><extra></extra>",
        )
    )

    suspicious = plot_df[plot_df["is_suspicious"] == 1]
    if not suspicious.empty:
        fig.add_trace(
            go.Scatter(
                x=suspicious["reading_timestamp"],
                y=suspicious["consumption_liters"],
                mode="markers",
                name="Threshold exceeded",
                marker=dict(
                    color=RED,
                    size=11,
                    symbol="diamond",
                    line=dict(color="#FFFFFF", width=2),
                ),
                hovertemplate=(
                    "<b>Threshold exceeded</b><br>"
                    "%{x|%d %b %Y %H:%M}<br>%{y:,.1f} L<extra></extra>"
                ),
            )
        )

    return _apply_default_layout(
        fig,
        xaxis_title="",
        yaxis_title="Litres",
        height=380,
        showlegend=True,
    )


def meter_temperature_chart(df: pd.DataFrame, meter_id: str) -> Optional[go.Figure]:
    """Separat temperaturserie — ingen dual-axis."""
    temp_df = df.dropna(subset=["temperature"]).sort_values("reading_timestamp")
    if temp_df.empty:
        return None

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=temp_df["reading_timestamp"],
            y=temp_df["temperature"],
            mode="lines",
            name="Temperature",
            line=dict(color=TEAL, width=2.2),
            fill="tozeroy",
            fillcolor="rgba(43, 179, 163, 0.07)",
            hovertemplate="%{x|%d %b %Y %H:%M}<br><b>%{y:.1f} °C</b><extra></extra>",
        )
    )
    return _apply_default_layout(
        fig,
        yaxis_title="°C",
        height=260,
        showlegend=False,
    )


def quality_issues_by_type_chart(df: pd.DataFrame) -> go.Figure:
    """Kvalitetsproblemer rangordnet efter antal."""
    if df.empty:
        return _empty_figure("No data quality issues recorded")

    plot_df = df.sort_values("issue_count", ascending=True).copy()

    fig = go.Figure(
        go.Bar(
            x=plot_df["issue_count"],
            y=plot_df["error_type"],
            orientation="h",
            marker=dict(color=RED, opacity=0.88, cornerradius=5),
            text=plot_df["issue_count"],
            textposition="outside",
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>%{x:,} issues<extra></extra>",
        )
    )

    fig = _apply_default_layout(
        fig,
        xaxis_title="Issues",
        height=300,
        showlegend=False,
    )
    fig.update_yaxes(showgrid=False)
    return fig


def quality_severity_donut_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _empty_figure("No data quality issues recorded")

    colors = [SEVERITY_COLORS.get(str(s), MUTED) for s in df["severity"]]
    total = int(df["issue_count"].sum())

    fig = go.Figure(
        data=[
            go.Pie(
                labels=df["severity"],
                values=df["issue_count"],
                hole=0.72,
                marker=dict(colors=colors, line=dict(color="#FFFFFF", width=2)),
                textinfo="none",
                hovertemplate="<b>%{label}</b><br>%{value:,} issues<br>%{percent}<extra></extra>",
            )
        ]
    )
    fig.add_annotation(
        x=0.5,
        y=0.5,
        text=f"<b>{total:,}</b><br><span style='font-size:10px;color:{MUTED}'>issues</span>",
        showarrow=False,
        font=dict(size=18, color=NAVY),
    )

    return _apply_default_layout(
        fig,
        height=300,
        showlegend=True,
    )


def pipeline_stage_flow_chart(stages: "list[tuple[str, int]]") -> go.Figure:
    """Compact horizontal stage-progression bars, used when there is only
    a single pipeline run and a run-history chart would not be meaningful."""
    if not stages:
        return _empty_figure("No pipeline runs recorded")

    labels = [s[0] for s in stages][::-1]
    values = [s[1] for s in stages][::-1]
    colors = [BLUE, TEAL, AMBER, PURPLE][: len(stages)][::-1]

    fig = go.Figure(
        go.Bar(
            x=values,
            y=labels,
            orientation="h",
            marker=dict(color=colors, cornerradius=4),
            text=values,
            texttemplate="%{text:,}",
            textposition="outside",
            cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>%{x:,} rows<extra></extra>",
        )
    )
    fig = _apply_default_layout(
        fig,
        xaxis_title="Rows",
        height=260,
        showlegend=False,
    )
    fig.update_yaxes(showgrid=False)
    return fig


def pipeline_run_metrics_chart(df: pd.DataFrame) -> go.Figure:
    """Run-history bars, only meaningful once more than one run exists."""
    if df.empty:
        return _empty_figure("No pipeline runs recorded")

    metrics = {
        "raw_rows_ingested": ("Raw ingested", BLUE),
        "staging_rows_inserted": ("Accepted", TEAL),
        "staging_rows_rejected": ("Rejected", RED),
        "facts_inserted": ("Published", PURPLE),
    }

    fig = go.Figure()
    for key, (label, color) in metrics.items():
        if key not in df.columns:
            continue
        fig.add_trace(
            go.Scatter(
                x=df["run_id"].astype(str),
                y=df[key],
                name=label,
                mode="lines+markers",
                line=dict(color=color, width=2.2),
                marker=dict(size=6, color=color),
                hovertemplate=f"<b>{label}</b><br>Run %{{x}}<br>%{{y:,}} rows<extra></extra>",
            )
        )

    return _apply_default_layout(
        fig,
        xaxis_title="Run",
        yaxis_title="Rows",
        height=320,
        showlegend=True,
    )


def _empty_figure(message: str) -> go.Figure:
    """Rolig tomtilstand, der stadig ligner resten af dashboardet."""
    fig = go.Figure()
    fig.add_annotation(
        text=f"<b>{message}</b>",
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.52,
        showarrow=False,
        font=dict(size=13, color=MUTED),
    )
    fig.add_annotation(
        text="Adjust the filters or run the pipeline to populate this view.",
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.42,
        showarrow=False,
        font=dict(size=11, color="#9AA8B6"),
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=260,
        margin=dict(l=18, r=18, t=18, b=18),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig
