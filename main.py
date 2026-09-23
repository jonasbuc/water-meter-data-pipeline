"""
main.py
-------
Indgangspunkt til at køre hele pipeline'en manuelt.

Kør med:  python main.py
"""

import logging
from pathlib import Path

from src.database import get_engine, init_db
from src.pipeline import run_pipeline

RAW_DATA_DIR = Path(__file__).resolve().parent / "data" / "raw"


def configure_logging() -> None:
    """
    Simpel logging-opsætning. I et rigtigt produktionssystem ville dette
    typisk gå til en fil eller et centralt logsystem (fx CloudWatch, ELK),
    men til dette projekt er stdout tilstrækkeligt og nemt at følge med i.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)

    engine = get_engine()
    init_db(engine)

    logger.info("Starter pipeline med kildemappe: %s", RAW_DATA_DIR)
    summary = run_pipeline(engine, RAW_DATA_DIR)

    print("\n--- Pipeline Summary ---")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
