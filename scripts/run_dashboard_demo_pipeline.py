"""
scripts/run_dashboard_demo_pipeline.py
-----------------------------------------
Kører den RIGTIGE, uændrede pipeline (ingestion -> validation ->
transformation) mod de syntetiske kilde-filer i data/dashboard_demo_raw/,
og skriver resultatet til en DEDIKERET database (data/dashboard_demo.db) -
adskilt fra både den lille eksisterende sample-database
(data/warehouse.db) og den generelle --demo-database (data/demo.db).

Dette script indeholder INGEN pipeline-logik selv - det er blot et tyndt
orkestrerings-lag der genbruger `src.database` og `src.pipeline` præcis
som `main.py` gør. Formålet er udelukkende at give BI-dashboardet en
rigere, men stadig 100% pipeline-genereret, datamængde at vise.

Kør (som modul, så `src`-pakken kan importeres uden PYTHONPATH-tricks):
    python scripts/generate_dashboard_demo_data.py   (hvis ikke allerede gjort)
    python -m scripts.run_dashboard_demo_pipeline
    streamlit run dashboard/app.py
"""

import logging
from pathlib import Path

from src.database import get_engine, init_db
from src.pipeline import run_pipeline

DASHBOARD_DEMO_RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "dashboard_demo_raw"
DASHBOARD_DEMO_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "dashboard_demo.db"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger(__name__)

    if not DASHBOARD_DEMO_RAW_DIR.exists() or not any(DASHBOARD_DEMO_RAW_DIR.iterdir()):
        logger.error(
            "Ingen syntetiske kilde-filer fundet i %s. Kør først: "
            "python scripts/generate_dashboard_demo_data.py",
            DASHBOARD_DEMO_RAW_DIR,
        )
        raise SystemExit(1)

    if DASHBOARD_DEMO_DB_PATH.exists():
        DASHBOARD_DEMO_DB_PATH.unlink()

    engine = get_engine(db_path=DASHBOARD_DEMO_DB_PATH)
    init_db(engine)

    logger.info("Kører pipeline mod %s -> %s", DASHBOARD_DEMO_RAW_DIR, DASHBOARD_DEMO_DB_PATH)
    summary = run_pipeline(engine, DASHBOARD_DEMO_RAW_DIR)

    print("\n--- Dashboard demo pipeline summary ---")
    for key, value in summary.items():
        print(f"{key}: {value}")
    print(f"\nDatabase klar til dashboardet: {DASHBOARD_DEMO_DB_PATH}")
    print("Peg dashboardet på den med miljøvariablen DASHBOARD_DB_PATH, fx:")
    print(f'  DASHBOARD_DB_PATH="{DASHBOARD_DEMO_DB_PATH}" streamlit run dashboard/app.py')


if __name__ == "__main__":
    main()
