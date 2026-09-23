"""
config.py
---------
Formål: adskille KONFIGURATION/FORRETNINGSREGLER fra kode.

Hvorfor en lille dataclass i stedet for miljøvariabel-biblioteker eller
dependency injection-frameworks?
- Projektet har PRÆCIS ét konfigurationsparameter lige nu
  (suspicious_threshold_liters). En dataclass er den mindste mulige
  abstraktion der stadig gør pointen tydelig: "denne værdi er en
  forretningsbeslutning, ikke en implementeringsdetalje".
- main.py konstruerer standard-konfigurationen; tests kan sagtens
  konstruere deres egen PipelineConfig med en anden tærskelværdi, uden at
  skulle monkeypatche et modul-niveau-konstant.

VIGTIGT: SUSPICIOUS_THRESHOLD_LITERS (2000 liter) er en ILLUSTRATIV,
fast tærskelværdi til undervisningsformål - IKKE en rigtig
lækage-detektionsalgoritme. Et produktionssystem ville typisk bruge en
statistisk tilgang (fx X standardafvigelser over målerens eget historiske
gennemsnit, eller en tidsserie-baseret anomali-detektor).
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Forretningskonfiguration for pipeline'en. Hold denne fri for
    database-forbindelser eller andre tekniske detaljer - kun
    forretningsregler hører til her."""

    suspicious_threshold_liters: float = 2000.0


DEFAULT_CONFIG = PipelineConfig()
