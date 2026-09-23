"""
ingestion.py
------------
Formål: læse CSV/JSON-filer og indsætte dem UÆNDREDE i raw_meter_readings.

Designprincip: ingestion-laget laver INGEN forretningslogik.
- Ingen validering
- Ingen type-konvertering (alt gemmes som tekst)
- Ingen dedup

Hvorfor? Fordi hvis vi en dag opdager en fejl i vores validerings-logik,
skal vi kunne re-køre validering/transformation fra raw uden at have mistet
eller ændret noget fra kilden. Dette er "bevar rådata"-princippet.

Alternativ, vi IKKE vælger: parse og validere direkte ved ingestion.
Trade-off ved det: hurtigere pipeline, men vi mister evnen til at
'replaye' historikken, og fejl i logik kan betyde permanent datatab.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_csv_file(file_path: Path) -> pd.DataFrame:
    """Læser en CSV-fil ind som DataFrame. Alt læses som string (dtype=str),
    fordi raw-laget ikke skal type-konvertere - det sker i staging."""
    df = pd.read_csv(file_path, dtype=str)
    return df


def read_json_file(file_path: Path) -> pd.DataFrame:
    """Læser en JSON-fil (liste af objekter) ind som DataFrame, alt som string."""
    with open(file_path, "r") as f:
        records = json.load(f)
    df = pd.DataFrame(records)
    return df.astype(str)


def read_source_file(file_path: Path) -> pd.DataFrame:
    """Vælger parser ud fra filendelse."""
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return read_csv_file(file_path)
    elif suffix == ".json":
        return read_json_file(file_path)
    else:
        raise ValueError(f"Ukendt filtype: {suffix}")


# Forventede kildekolonner. Vi mapper eksplicit i stedet for at stole på at
# kildefilen altid har præcis de rigtige kolonnenavne/rækkefølge.
EXPECTED_COLUMNS = [
    "meter_id",
    "timestamp",
    "consumption_liters",
    "temperature",
    "status",
]


def ingest_file(engine: Engine, file_path: Path) -> int:
    """
    Læser en fil og indsætter alle rækker i raw_meter_readings.

    Returnerer antal indsatte rækker (= records_read for denne fil).

    Bemærk: ingestion er IKKE idempotent i sig selv - kører man samme fil
    to gange, får raw_meter_readings dubletter. Det er med vilje: raw er
    et append-only log af "hvad modtog vi hvornår". Idempotency håndhæves
    først i staging-laget (via UNIQUE constraint på meter_id+timestamp).
    """
    df = read_source_file(file_path)

    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Fil {file_path.name} mangler kolonner: {missing_cols}")

    ingested_at = _now_iso()
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "meter_id": row.get("meter_id"),
                "timestamp_raw": row.get("timestamp"),
                "consumption_liters": row.get("consumption_liters"),
                "temperature": row.get("temperature"),
                "status": row.get("status"),
                "source_file": file_path.name,
                "ingested_at": ingested_at,
            }
        )

    insert_sql = text(
        """
        INSERT INTO raw_meter_readings
            (meter_id, timestamp_raw, consumption_liters, temperature, status,
             source_file, ingested_at)
        VALUES
            (:meter_id, :timestamp_raw, :consumption_liters, :temperature, :status,
             :source_file, :ingested_at)
        """
    )

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    logger.info("Ingested %d rows from %s", len(rows), file_path.name)
    return len(rows)


def ingest_directory(engine: Engine, directory: Path) -> int:
    """Ingester alle .csv/.json filer i en mappe. Returnerer total antal rækker."""
    total = 0
    files: Iterable[Path] = sorted(
        list(directory.glob("*.csv")) + list(directory.glob("*.json"))
    )
    for file_path in files:
        total += ingest_file(engine, file_path)
    return total
