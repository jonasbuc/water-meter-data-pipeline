"""
scripts/generate_dashboard_demo_data.py
-----------------------------------------
Genererer SYNTETISK, DETERMINISTISK demonstrationsdata til BI-dashboardet.

VIGTIGT: dette er IKKE rigtige målinger fra noget forsyningsselskab. Det er
opdigtede, syntetiske tal designet til at gøre dashboardet visuelt
meningsfuldt (flere målere, flere dage, nogle datakvalitetsproblemer,
nogle mistænkelige målinger) - dokumenteret eksplicit her og i
kommentarerne nedenfor.

ARKITEKTUR-PRINCIP (vigtigt): dette script genererer KUN kilde-filer
(CSV/JSON) - det indsætter ALDRIG direkte i analytics-tabellerne. Den
rigtige vej er:

    syntetiske kilde-filer -> rigtig ingestion -> rigtig validation
    -> rigtig transformation -> dashboard

Kør:
    python scripts/generate_dashboard_demo_data.py
    python scripts/run_dashboard_demo_pipeline.py
    streamlit run dashboard/app.py

For ikke at forstyrre den lille, eksisterende sample-mappe (data/raw/),
skriver dette script til en DEDIKERET mappe:

    data/dashboard_demo_raw/

Determinisme: al "tilfældighed" bruger et FAST seed (random.seed(42)), så
gentagne kørsler producerer nøjagtig samme data - vigtigt for at CI og
demoer er reproducerbare.
"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 42
READINGS_PER_DAY = 4  # hver 6. time - nok til en læsbar tidsserie, ikke støjende
NUM_DAYS = 30

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "dashboard_demo_raw"

# Seks målere med forskellige, syntetiske forbrugsprofiler (baseline liter
# pr. aflæsning + variation) - dette giver dashboardet noget meningsfuldt
# at differentiere på Top Meters-grafen.
METER_PROFILES = {
    "DM-001": {"baseline": 40, "variation": 8},    # lille husstand
    "DM-002": {"baseline": 65, "variation": 12},   # gennemsnitlig husstand
    "DM-003": {"baseline": 30, "variation": 5},    # lille husstand
    "DM-004": {"baseline": 150, "variation": 25},  # erhverv/større forbrug
    "DM-005": {"baseline": 55, "variation": 10},   # gennemsnitlig husstand
    "DM-006": {"baseline": 90, "variation": 15},   # mellemstor husstand
}

START_DATE = datetime(2026, 8, 1)


def _generate_normal_reading(rng: random.Random, meter_id: str) -> float:
    profile = METER_PROFILES[meter_id]
    value = rng.normalvariate(profile["baseline"], profile["variation"])
    return max(round(value, 1), 0.1)


def _generate_temperature(rng: random.Random) -> float:
    """Simpel temperaturvariation - nogle målere mangler den bevidst (se
    OPTIONAL_COLUMNS i src/ingestion.py)."""
    return round(rng.normalvariate(12.0, 2.5), 1)


def generate_rows(rng: random.Random):
    """
    Genererer alle syntetiske rækker som en liste af dicts, klar til at
    blive skrevet ud som CSV/JSON i den kontrakt src/ingestion.py forventer
    (meter_id, timestamp, consumption_liters, temperature, status).

    Indbygger med VILJE nogle "designede" specialtilfælde, så
    dashboardets Data Quality- og Suspicious-sektioner har noget at vise:
    - to høj-forbrug mistænkelige målinger (over
      PipelineConfig.suspicious_threshold_liters = 2000 L)
    - nogle manglende temperatur-værdier
    - én duplikeret (meter_id, timestamp)-kombination i samme batch
    - ét ugyldigt timestamp
    - én negativ forbrugsværdi
    - manglende meter_id
    - én ikke-parsbar temperatur (WARNING, ikke rejection)
    """
    rows = []
    for day_offset in range(NUM_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        for meter_id in METER_PROFILES:
            for reading_index in range(READINGS_PER_DAY):
                timestamp = current_date + timedelta(hours=reading_index * 6)
                consumption = _generate_normal_reading(rng, meter_id)
                temperature = _generate_temperature(rng)
                # ~1 ud af 8 rækker dropper bevidst temperatur (optional-kolonne).
                include_temperature = rng.randint(1, 8) != 1
                rows.append({
                    "meter_id": meter_id,
                    "timestamp": timestamp.isoformat(),
                    "consumption_liters": consumption,
                    "temperature": temperature if include_temperature else "",
                    "status": "OK",
                })

    # --- Designede specialtilfælde (deterministiske, ikke tilfældige) ---

    rows.append({
        "meter_id": "DM-004", "timestamp": (START_DATE + timedelta(days=10, hours=6)).isoformat(),
        "consumption_liters": 3200.0, "temperature": 11.5, "status": "OK",
    })
    rows.append({
        "meter_id": "DM-002", "timestamp": (START_DATE + timedelta(days=20, hours=12)).isoformat(),
        "consumption_liters": 2650.0, "temperature": 13.0, "status": "OK",
    })

    duplicate_ts = (START_DATE + timedelta(days=5, hours=0)).isoformat()
    rows.append({
        "meter_id": "DM-001", "timestamp": duplicate_ts,
        "consumption_liters": 42.0, "temperature": 10.0, "status": "OK",
    })
    rows.append({
        "meter_id": "DM-001", "timestamp": duplicate_ts,
        "consumption_liters": 42.0, "temperature": 10.0, "status": "OK",
    })

    rows.append({
        "meter_id": "DM-003", "timestamp": "not-a-real-timestamp",
        "consumption_liters": 33.0, "temperature": 9.5, "status": "OK",
    })

    rows.append({
        "meter_id": "DM-005", "timestamp": (START_DATE + timedelta(days=7, hours=18)).isoformat(),
        "consumption_liters": -12.0, "temperature": 11.0, "status": "OK",
    })

    rows.append({
        "meter_id": "", "timestamp": (START_DATE + timedelta(days=8, hours=6)).isoformat(),
        "consumption_liters": 50.0, "temperature": 12.0, "status": "OK",
    })

    rows.append({
        "meter_id": "DM-006", "timestamp": (START_DATE + timedelta(days=9, hours=12)).isoformat(),
        "consumption_liters": 88.0, "temperature": "not-a-number", "status": "OK",
    })

    return rows


def write_csv(rows, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["meter_id", "timestamp", "consumption_liters", "temperature", "status"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(rows, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def main() -> None:
    rng = random.Random(SEED)
    rows = generate_rows(rng)

    # Split i to filer (CSV + JSON) for at demonstrere at ingestion
    # håndterer begge formater - samme mønster som de eksisterende
    # data/raw/-eksempler.
    midpoint = len(rows) // 2
    csv_rows, json_rows = rows[:midpoint], rows[midpoint:]

    csv_path = OUTPUT_DIR / "dashboard_demo_readings.csv"
    json_path = OUTPUT_DIR / "dashboard_demo_readings.json"

    write_csv(csv_rows, csv_path)
    write_json(json_rows, json_path)

    print("SYNTHETIC DEMONSTRATION DATA generated (NOT real utility readings):")
    print(f"  {csv_path}  ({len(csv_rows)} rows)")
    print(f"  {json_path} ({len(json_rows)} rows)")
    print(f"  Total rows: {len(rows)} across {len(METER_PROFILES)} meters, {NUM_DAYS} days")
    print()
    print("Next steps:")
    print("  python scripts/run_dashboard_demo_pipeline.py")
    print("  streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
