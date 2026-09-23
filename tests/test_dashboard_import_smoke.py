"""
test_dashboard_import_smoke.py
---------------------------------
Minimal smoke test: beviser at dashboard/app.py (og dets afhængigheder
dashboard/data.py, dashboard/charts.py) kan importeres uden fejl - uden at
starte en rigtig Streamlit-server (det ville CI ikke skulle gøre, se
opgave-krav #24). Dette fanger fx import-fejl, forkerte modulnavne eller
manglende afhængigheder tidligt.
"""

import importlib


def test_dashboard_app_module_imports_successfully():
    module = importlib.import_module("dashboard.app")
    assert hasattr(module, "main")
    assert hasattr(module, "render_overview")
    assert hasattr(module, "render_meter_analysis")
    assert hasattr(module, "render_data_quality")
    assert hasattr(module, "render_pipeline_health")
    assert hasattr(module, "render_data_lineage")


def test_dashboard_data_and_charts_modules_import_successfully():
    data_module = importlib.import_module("dashboard.data")
    charts_module = importlib.import_module("dashboard.charts")
    assert hasattr(data_module, "DashboardFilters")
    assert hasattr(charts_module, "consumption_over_time_chart")
