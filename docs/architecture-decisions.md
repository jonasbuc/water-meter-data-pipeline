# Architecture Decision Records

Korte ADR'er for de vigtigste design-beslutninger i dette projekt. Formatet
er bevidst minimalt (Context / Decision / Trade-off) - nok til at kunne
forsvare en beslutning i en samtale, uden bureaukrati.

---

## ADR-001: Tre lag - RAW / STAGING / ANALYTICS

**Context**: Data fra vandmålere kan indeholde fejl, og valideringsregler
kan vise sig at være forkerte eller ufuldstændige efter de er skrevet.

**Decision**: Data flyder gennem tre adskilte lag med stigende grad af
tillid: raw (ukurateret, alt som tekst), staging (valideret, typet),
analytics (star schema, BI-klar).

**Trade-off**: Mere kode og flere tabeller end en simpel "load direkte i
fact-tabellen"-løsning. Til gengæld: mulighed for at genbehandle historik
efter en bugfix i valideringslogikken, og fuld sporbarhed (lineage) fra
BI-rapport til kilde-byte.

---

## ADR-002: System-genererede ID'er som incremental watermark (ikke kilde-timestamp)

**Context**: Incremental loading kræver et "vandmærke" der fortæller
pipelinen hvor langt den er nået.

**Decision**: Brug monotont stigende `raw_id`/`stg_id` (database-genereret)
i stedet for kildens `reading_timestamp`.

**Trade-off**: Kræver at ID'er reelt er strengt stigende i
ankomstrækkefølge (sandt her, fordi ingestion er den eneste writer). Til
gengæld undgås det klassiske problem med sent-ankommende data, der ellers
permanent ville blive sprunget over hvis vandmærket var en kilde-timestamp.

---

## ADR-003: Fil-niveau idempotency via indholds-hash

**Context**: Samme fil kan blive leveret/afhentet flere gange (fx en
genkørt cron-opgave, eller en fil der ved en fejl kopieres to gange).

**Decision**: Beregn SHA-256 af filens rå indhold og gem i
`ingested_files.file_hash` (UNIQUE). Er hashen allerede `PROCESSED`,
springes filen over.

**Trade-off**: Kræver at læse hele filen for at hashe den, selv for filer
vi ender med at springe over. Det er en fast, lille pris (I/O), og en
optimering (precheck på filnavn før hash) undgår dette for filer der med
sikkerhed allerede er markeret `PROCESSED`.

---

## ADR-004: Per-stage transaktionsgrænser (ikke én stor transaktion for hele pipelinen)

**Context**: Pipelinen har tre logiske trin (ingestion, validation,
transformation). Et trin kan fejle midtvejs (fx strømafbrydelse, bug).

**Decision**: Hvert trin er sin egen atomiske transaktion. Ingestion af én
fil er ÉN transaktion (claim + insert + marker processeret). Validation af
én batch er ÉN transaktion. Transformation af én batch er ÉN transaktion.

**Trade-off**: En fejl i transformation efterlader staging som already
committed (det er med vilje - staging-arbejdet var gyldigt og skal ikke
laves om). Alternativet (én kæmpe transaktion for hele pipelinen) ville
gøre retries dyrere (alt arbejde tabt ved enhver fejl) og holde
databaselåse åbne unødvendigt længe.

---

## ADR-005: Star schema i analytics-laget

**Context**: Analytics-laget skal være nemt at forespørge fra BI-værktøjer
som Power BI.

**Decision**: `dim_meter` (attributter om måleren) + `fact_water_consumption`
(én række pr. måling) - klassisk star schema.

**Trade-off**: Kræver en ekstra transformation fra staging til fact/dim
(surrogate keys, upsert-logik), i stedet for at BI-værktøjet forespørger
staging direkte. Til gengæld: stabile surrogate keys, simple joins, og et
mønster BI-værktøjer er optimeret til.

---

## ADR-006: Lineage via tekniske fremmednøgler, ikke en separat lineage-tabel

**Context**: Det skal være muligt at spore en analytics-værdi tilbage til
den præcise kilde-fil og raw-række.

**Decision**: `fact.source_stg_id -> stg.stg_id`, `stg.raw_id -> raw.raw_id`,
og `raw.source_file`/`raw.raw_payload` er nok til fuld lineage - ingen
separat lineage-metadata-tabel.

**Trade-off**: Kræver et par ekstra joins for at spore lineage, i stedet
for at slå direkte op i en dedikeret tabel. Til gengæld: ingen risiko for
at lineage-tabellen kommer ud af sync med de faktiske data, fordi lineage
ER fremmednøgle-kæden, ikke en separat afledt struktur.

---

## ADR-007: SQLite til demo/udvikling, SQL Server konceptuelt til produktion

**Context**: Projektet skal være let at køre lokalt uden en database-server,
men skal demonstrere begreber der overfører sig til en "rigtig" produktions-
database.

**Decision**: SQLite bruges til udvikling/demo/tests (in-memory til tests,
fil-baseret til demo). `README.md` dokumenterer eksplicit hvilke
SQL-dialekt-forskelle (ikke bare connection-string) der skal håndteres ved
en SQL Server-migrering.

**Trade-off**: Nogle SQLite-specifikke konstruktioner
(`INSERT OR IGNORE`, `PRAGMA foreign_keys=ON`, `AUTOINCREMENT`) skal
oversættes manuelt ved migrering. Til gengæld: nul opsætning for at køre
eller teste projektet.

---

## ADR-008: Interval-forbrug er den antagede datamodel (ikke kumulativ måler-tælleraflæsning)

**Context**: Vandmålerdata kan modelleres på to fundamentalt forskellige
måder: (a) hver record ER forbruget i en periode ("interval"-model), eller
(b) hver record er en akkumuleret tæller-aflæsning ("kumulativ"-model),
hvor forbrug = differencen mellem to på hinanden følgende aflæsninger.

**Decision**: Dette projekt antager INTERVAL-modellen: `consumption_liters`
er allerede forbruget i den pågældende periode, og kan derfor lægges
sammen direkte med `SUM()`.

**Trade-off**: Hvis kildedata reelt er kumulative tælleraflæsninger, ville
`SUM(consumption_liters)` give et grotesk forkert (alt for stort) resultat,
og man skulle i stedet bruge `consumption = current_reading - LAG(reading)
OVER (PARTITION BY meter_id ORDER BY reading_timestamp)` - med ekstra
kompleksitet for målerskift, nulstillinger og "counter rollover". Se
README-afsnittet "Domæneantagelse: interval- vs. kumulativt forbrug" for
et konkret SQL-eksempel på begge tilgange.
