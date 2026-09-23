# Vandmåler Data Pipeline

Et lille, men realistisk data engineering-projekt der demonstrerer kerne­koncepterne
i en dataspecialist-rolle: ingestion, validering, lagdelt datamodel, incremental
loading, idempotency, logging og analyseklare data til fx Power BI.

## Problem

Vi modtager rå målerdata fra vandmålere som CSV/JSON-filer. Data kan indeholde
fejl (manglende ID'er, ugyldige timestamps, negative målinger, dubletter), og
den skal gøres analyseklar uden at vi mister sporbarhed til kilden.

## Løsning

En simpel Python/SQL-datapipeline flytter data gennem tre lag:
**raw → staging → analytics**, med validering, deduplikering og logging
undervejs.

## Arkitektur

```
CSV/JSON
   │
   ▼
Ingestion (src/ingestion.py)
   │
   ▼
RAW           raw_meter_readings
   │
   ▼
Validation (src/validation.py)
   │
   ▼
STAGING       stg_meter_readings   +   data_quality_errors
   │
   ▼
Transformation (src/transformation.py)
   │
   ▼
ANALYTICS     dim_meter  +  fact_water_consumption
   │
   ▼
Power BI / SQL-analyse
```

Orkestreringen af alle trin sker i `src/pipeline.py`, som også skriver til
`pipeline_runs` for logging og incremental load.

## Projektstruktur

```
data/
  raw/                     # inputfiler (CSV/JSON)
  warehouse.db             # SQLite-database (genereres ved kørsel)

src/
  ingestion.py             # læser filer -> raw_meter_readings
  validation.py            # raw -> staging, med data quality checks
  transformation.py        # staging -> dim/fact
  database.py              # engine + schema-opsætning
  pipeline.py              # orkestrering, logging, incremental load

sql/
  schema.sql               # alle tabeller
  analytics_queries.sql    # 10 SQL-øvelser på fact/dim-modellen

tests/
  test_validation.py
  test_transformation.py
  test_pipeline_incremental.py

main.py                    # kør hele pipeline'en
```

## Sådan kører du det

```bash
python main.py              # kør pipeline
pytest                       # kør tests
```

## Design-beslutninger

### Hvorfor tre lag (raw / staging / analytics)?

- **Raw**: bevarer data præcis som modtaget (alt gemt som tekst). Hvis vi
  senere finder en bug i valideringslogikken, kan vi genbehandle historikken
  uden at have mistet noget. Raw er vores "forsikring".
- **Staging**: her håndhæves forretningsregler (typer, gyldighed, dubletter).
  Adskillelsen gør det muligt at teste validering isoleret fra transformation.
- **Analytics**: et kurateret star schema optimeret til BI-værktøjer og
  ad-hoc SQL-analyse.

### Hvorfor beholder vi rådata i stedet for kun at gemme det validerede?

Fordi valideringsregler kan ændre sig, eller vise sig at have fejl. Uden
rådata kan vi ikke gå tilbage og rette. Det er også nyttigt til debugging:
"hvorfor blev denne record afvist?" kræver at vi kan se den oprindelige,
ubehandlede værdi.

### Hvorfor er database constraints (UNIQUE, CHECK) vigtige?

Applikationskode kan have bugs, race conditions, eller blive omgået (fx et
script der skriver direkte til databasen). En `UNIQUE`-constraint på
`(meter_id, reading_timestamp)` er den sidste, garanterede forsvarslinje mod
dubletter — uanset hvad der sker i koden ovenover.

### Idempotency vs. duplicate detection vs. database constraints

| Begreb | Hvad det er | Eksempel her |
|---|---|---|
| Duplicate detection | Applikationslogik der *forsøger* at genkende gentagelser | `seen_keys_in_batch` i `validation.py` (fanger kun dubletter inden for samme batch) |
| Database constraint | Databasen *garanterer* uniqueness, uanset kode | `UNIQUE (meter_id, reading_timestamp)` |
| Idempotent processing | Hele operationen giver samme resultat, køres den 1 eller 10 gange | `INSERT OR IGNORE` + unik nøgle i både staging og fact |

Alle tre bruges sammen: duplicate detection er en optimering/kvalitetssignal,
constraint er sikkerhedsnettet, og idempotent processing er den overordnede
egenskab vi designer efter.

### Hvorfor incremental loading?

Uden det ville hver pipeline-kørsel genbehandle *hele* datasættet — dyrt i
tid og ressourcer, og det skalerer dårligt efterhånden som historikken vokser.

**Løsningen her**: `pipeline_runs` gemmer det højeste `raw_id` og `stg_id`
der er behandlet i seneste succesfulde kørsel. Næste kørsel henter kun
rækker med højere ID.

**Hvorfor ID og ikke kilde-timestamp som vandmærke?**
Timestamps fra målere kan ankomme forsinket eller i vilkårlig rækkefølge
(ur-skævheder, netværksforsinkelse). Hvis vi bruger `MAX(reading_timestamp)`
som vandmærke, og en forsinket record ankommer *efter* vi har rykket
vandmærket forbi dens timestamp, springes den permanent over. Et
database-genereret, strengt stigende ID (`raw_id`/`stg_id`) har ikke dette
problem, fordi det afspejler *ankomstrækkefølge* i systemet, ikke kildens
egen tidsangivelse.

**Problemer incremental loading selv kan skabe:**
- Hvis en kørsel fejler midtvejs, skal vandmærket kun opdateres ved succes
  (ellers mister vi data permanent) — derfor opdaterer vi kun
  `pipeline_runs` med `status = 'SUCCESS'` som gyldigt udgangspunkt.
- Sen-ankommende data (en fil der "burde" være behandlet tidligere) kan i
  værste fald aldrig blive samlet op, hvis den ankommer med et *raw_id* der
  ligger under vandmærket. I dette projekt er det ikke et problem, fordi
  ingestion altid tilføjer nye raw_id'er sekventielt — men i mere komplekse
  systemer er "late-arriving data" et kendt, svært problem.

### Hvorfor gemmer vi datakvalitetsfejl i stedet for bare at droppe dem?

Princippet: **"Bad data should be observable, not silently discarded."**
Hvis afviste records bare forsvinder, kan vi ikke svare på spørgsmål som
"hvor mange records fejler, og hvorfor?" — hvilket ofte er lige så vigtigt
som selve dataen, fordi det afslører problemer hos datakilden.

### Hvorfor logging og `pipeline_runs`?

Uden logging er en fejlet eller langsom pipeline en sort boks. `pipeline_runs`
giver et minimalt, forespørgelsesbart audit trail: hvornår kørte vi, hvor
meget data, og lykkedes det. Det er samtidig grundlaget for incremental load.

### Hvorfor fact/dimension-model (star schema) i analytics-laget?

- `dim_meter` beskriver *hvem* måleren er (stabile attributter, kan
  udvides med lokation osv. uden at ændre fact-tabellen).
- `fact_water_consumption` beskriver *hvad der skete* — én række pr. måling.

Denne adskillelse gør det let at tilføje flere dimensioner senere (fx
`dim_date`), holder fact-tabellen "tynd" og hurtig at aggregere, og er det
mønster BI-værktøjer som Power BI er bygget til at arbejde effektivt med
(simple joins, klare relationer, hurtige aggregeringer).

## Brug i Power BI

`data/warehouse.db` kan tilsluttes direkte (via en SQLite-ODBC-driver) eller
eksporteres til en SQL Server-database ved at genbruge `sql/schema.sql`
(kun connection-strengen i `src/database.py` skal ændres). I Power BI
importeres `dim_meter` og `fact_water_consumption`, og der oprettes en
relation `dim_meter.meter_key = fact_water_consumption.meter_key` — det
klassiske star schema-setup.

## SQL-øvelser

Se `sql/analytics_queries.sql` for 10 forespørgsler du kan træne på:
daglige totaler, top-10 målere, gennemsnit, window functions (`LAG`,
`AVG OVER`), JOIN mellem fact/dim, og data quality-overblik.
