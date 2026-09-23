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
  ingestion.py             # læser filer -> raw_meter_readings (file-level idempotent)
  validation.py            # raw -> staging, med data quality checks (ERROR/WARNING)
  transformation.py        # staging -> dim/fact (event vs. processing time)
  database.py              # engine + schema-opsætning + PRAGMA foreign_keys=ON
  pipeline.py               # orkestrering, logging, incremental load, recovery

sql/
  schema.sql               # alle tabeller
  analytics_queries.sql    # SQL-øvelser + lineage/debugging + pipeline health

tests/
  test_validation.py
  test_transformation.py
  test_pipeline_incremental.py
  test_lineage_and_constraints.py

.github/workflows/tests.yml # CI: kører pytest på push/PR

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

Se `sql/analytics_queries.sql` for forespørgsler du kan træne på:
daglige totaler, top-10 målere, gennemsnit, window functions (`LAG`,
`AVG OVER`), JOIN mellem fact/dim, data quality-overblik, pipeline health,
og en lineage/debugging-forespørgsel (se "Data lineage" nedenfor).

## Duplicate handling: hvorfor DUPLICATE_EXISTING ikke er en data-kvalitetsfejl

`validation.py` skelner mellem to slags dubletter:

- **DUPLICATE_IN_BATCH**: samme `(meter_id, reading_timestamp)` optræder
  to gange i SAMME fil/batch. Det er en reel datakvalitetsfejl - kilden
  har sendt samme måling to gange - og logges i `data_quality_errors`
  med `severity=ERROR`.
- **DUPLICATE_EXISTING**: rækken findes allerede i staging fra en TIDLIGERE
  kørsel. Det er IKKE en fejl i dataen - det er forventet, normal adfærd
  når fx watermarks overlapper, eller en fil delvist var behandlet før en
  fejl. Den tælles i `duplicates_skipped` i `pipeline_runs`, men havner
  bevidst IKKE i `data_quality_errors`, fordi den ikke fortæller noget om
  datakvalitet - kun at pipelinen allerede har set den.

**Konceptet**: en dublet kan være gyldigt input der allerede er behandlet;
det er ikke nødvendigvis korrupt data. At blande de to sammen i samme
fejl-tabel ville gøre `data_quality_errors` mindre nyttig til at finde
*reelle* problemer hos datakilden.

## Concepts demonstrated

Kort, i almindeligt sprog - de begreber dette projekt er bygget til at vise:

- **RAW / STAGING / ANALYTICS**: tre lag med stigende grad af "tillid" og
  struktur. Raw er ukurateret, staging er valideret, analytics er
  BI-klar (star schema).
- **ETL vs. ELT**: dette projekt er ETL (Extract-Transform-Load) - data
  transformeres i Python/SQL FØR den lander i det endelige analytics-lag,
  ikke bagefter i selve BI-værktøjet.
- **Incremental loading**: hvert pipeline-trin behandler kun rækker nyere
  end sidste succesfulde kørsel (via `raw_id`/`stg_id`), i stedet for at
  genbehandle alt hver gang.
- **Watermarks**: det gemte "hvor langt er vi nået"-punkt
  (`last_processed_raw_id`/`stg_id`). Opdateres kun ved SUCCESS.
- **Late-arriving data**: data der ankommer/behandles efter data der
  logisk hører til SENERE i tid. Håndteres ved at bruge system-ID'er som
  vandmærke (ikke kilde-timestamps) og MIN/MAX-semantik i `dim_meter`.
- **Idempotency**: at køre samme operation flere gange giver samme
  resultat. Vi har BÅDE fil-niveau (samme fil-hash indlæses kun én gang)
  og record-niveau (UNIQUE constraints + eksplicit dublet-tjek) idempotency.
- **Fil-niveau vs. record-niveau idempotency**: fil-niveau forhindrer at
  en hel fil genindlæses; record-niveau forhindrer at en enkelt måling
  duplikeres på tværs af filer/kørsler. De løser forskellige problemer og
  er begge nødvendige.
- **Database constraints**: `UNIQUE`, `CHECK` og `FOREIGN KEY` er den
  sidste, garanterede forsvarslinje mod dårlig data - uafhængig af om
  applikationskoden husker at tjekke selv.
- **Data lineage**: evnen til at spore en analytics-række tilbage til
  præcis den kilde-fil og raw-række den stammer fra (`fact.source_stg_id`
  -> `stg.raw_id` -> `raw.raw_id`/`source_file`).
- **Data-quality errors vs. warnings**: ERROR betyder rækken blev afvist;
  WARNING betyder rækken blev accepteret, men har en kvalitetsbemærkning
  (fx en ikke-parsbar temperatur, hvor kernemålingen stadig er gyldig).
- **Event time vs. processing time**: event time er hvornår noget SKETE
  (kildens `reading_timestamp`); processing time er hvornår VI behandlede
  det. `dim_meter.first_reading_at`/`last_reading_at` er event time;
  `created_at`/`updated_at` er processing time.
- **Transaktioner / atomicitet**: hvert logisk pipeline-trin (raw->staging,
  staging->analytics) sker i én transaktion, så det enten lykkes helt
  eller rulles helt tilbage.
- **Star schema**: `dim_meter` + `fact_water_consumption` - få joins,
  hurtige aggregeringer, det mønster BI-værktøjer er bygget til.
- **Surrogate vs. natural keys**: `meter_key` (surrogate, database-genereret)
  vs. `meter_id` (natural/business key fra kildesystemet). Surrogate keys
  er stabile selv hvis kildesystemets ID-format ændrer sig.
- **Observability**: `pipeline_runs` og `data_quality_errors` gør det
  muligt at svare på "hvad skete der, og hvorfor" uden at grave i logfiler.
- **Power BI som konsumtionslag**: analytics-laget (`dim_meter` +
  `fact_water_consumption`) er designet til at blive importeret direkte
  i et BI-værktøj med en simpel star schema-relation.

## SQLite → SQL Server mapping

Arkitekturen og SQL-koncepterne (lag, star schema, incremental load,
lineage, constraints) overfører sig direkte til SQL Server. Men det er
IKKE sandt at man bare kan skifte connection-string - en del
SQLite-specifik syntaks og connection-adfærd skal tilpasses:

| SQLite | SQL Server | Bemærkning |
|---|---|---|
| `INTEGER PRIMARY KEY AUTOINCREMENT` | `INT IDENTITY(1,1) PRIMARY KEY` | Forskellig syntaks for auto-genererede nøgler |
| `INSERT OR IGNORE` | `INSERT ... WHERE NOT EXISTS (...)` eller `MERGE` (med forsigtighed - MERGE har kendte race-condition-faldgruber) | SQL Server har ikke en direkte "ignore duplicate"-klausul |
| `LIMIT n` | `TOP (n)` eller `OFFSET ... FETCH NEXT n ROWS ONLY` | Anden paginering-syntaks |
| `DATE(timestamp_col)` | `CAST(timestamp_col AS DATE)` eller `CONVERT(date, ...)` | Anden dato-trunkeringsfunktion |
| `PRAGMA foreign_keys=ON` (per connection) | FK håndhæves altid, ingen pragma nødvendig | SQL Server håndhæver FK som standard |
| `TEXT`-kolonne med ISO8601-timestamp | `DATETIME2` / `DATETIMEOFFSET` | SQL Server har native dato/tid-typer i stedet for tekststrenge |
| In-process fil (`sqlite:///path.db`) | Klient/server-forbindelse (`mssql+pyodbc://...`) | Connection pooling, netværk og autentificering fungerer anderledes |

Resten af koden (Python-logikken, transaktionsgrænser, validerings- og
transformationsregler) ville forblive stort set uændret - kun SQL-dialekten
og connection-håndteringen skal tilpasses.

## Questions I should be able to answer

1. **Hvorfor bruger du ikke `reading_timestamp` som incremental watermark?**
   Fordi kilde-timestamps kan ankomme forsinket eller i vilkårlig rækkefølge
   (ur-skævheder, netværksforsinkelse). Et database-genereret, monotont
   stigende ID (`raw_id`/`stg_id`) afspejler ankomstrækkefølge, ikke
   kildens egen tidsangivelse, så sent-ankommende data aldrig springes over.

2. **Hvad sker der hvis samme fil ankommer to gange?**
   Ingestion beregner SHA-256 af filens indhold og slår op i
   `ingested_files`. Er hashen allerede markeret `PROCESSED`, springes
   filen over - raw vokser ikke. Ændret indhold (ny hash) behandles som
   en ny version.

3. **Hvad sker der hvis transformation fejler EFTER at staging er committet?**
   Staging-transaktionen er allerede permanent (den var sin egen atomiske
   enhed). Kørslen markeres `FAILED`, og `last_processed_stg_id` opdateres
   IKKE. Næste kørsel henter derfor de samme staging-rækker igen og
   transformerer dem - uden at duplikere dem, fordi fact har en
   `UNIQUE(meter_key, reading_timestamp)`-constraint.

4. **Hvorfor har du brug for en UNIQUE-constraint, hvis applikationskoden allerede opdager dubletter?**
   Applikationskode kan have bugs, blive omgået, eller ramme race
   conditions. Constrainten er den sidste, GARANTEREDE forsvarslinje -
   uafhængig af om koden ovenover opfører sig korrekt.

5. **Hvorfor beholder du rådata i stedet for kun det validerede?**
   Valideringsregler kan ændre sig eller vise sig fejlbehæftede. Uden
   rådata kan man ikke genbehandle historikken eller debugge hvorfor en
   specifik række blev afvist.

6. **Hvad er forskellen på event time og processing time?**
   Event time er hvornår noget faktisk SKETE ifølge kilden
   (`reading_timestamp`); processing time er hvornår PIPELINEN rørte
   ved det (`created_at`/`updated_at`). De kan være vidt forskellige,
   fx ved efterslæb eller sent-ankommende data.

7. **Hvordan kan du spore en Power BI-værdi tilbage til kilden?**
   Følg lineage-kæden: `fact.source_stg_id -> stg.stg_id`, og
   `stg.raw_id -> raw.raw_id`, som har `source_file` og `raw_payload`.
   Se lineage/debugging-forespørgslen i `sql/analytics_queries.sql`.

8. **Hvorfor bruge et star schema i stedet for at forespørge staging direkte?**
   Star schema giver stabile surrogate keys, færre/simplere joins, og et
   kurateret, forudsigeligt lag BI-værktøjer er optimeret til at
   aggregere hurtigt over - uden at BI-rapporter skal kende til
   validerings-detaljer i staging.

9. **Hvordan ville denne arkitektur ændre sig på SQL Server?**
   Lagene, star schemaet, incremental load og lineage forbliver de samme.
   Kun SQL-dialekt-detaljer skifter (se "SQLite → SQL Server mapping"),
   samt connection-håndtering (klient/server i stedet for fil).

10. **Hvorfor er `duplicates_skipped` ikke en fejl i `data_quality_errors`?**
    Fordi en dublet ift. en TIDLIGERE kørsel ikke fortæller noget om
    dataens kvalitet - kun at pipelinen allerede har set den. Det er
    operationel metadata, adskilt fra `DUPLICATE_IN_BATCH`, som ER en
    reel datakvalitetsfejl (kilden sendte samme måling to gange i én fil).

11. **Hvorfor har `data_quality_errors` en `severity`-kolonne?**
    For at skelne mellem rækker der blev AFVIST (`ERROR`, findes ikke i
    staging) og rækker der blev ACCEPTERET men har en kvalitetsbemærkning
    (`WARNING`, fx en ikke-parsbar temperatur hvor kernemålingen er gyldig).

12. **Hvad beviser din test af foreign keys egentlig?**
    At `PRAGMA foreign_keys=ON` reelt håndhæves af SQLite-forbindelsen -
    ikke bare at `FOREIGN KEY` står i `schema.sql`. Testen forsøger at
    indsætte en fact-række med et ugyldigt `meter_key` og forventer at
    SQLite kaster en `IntegrityError`.
