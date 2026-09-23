-- =========================================================
-- SCHEMA: Vandmåler Data Pipeline (REFERENCE ONLY)
-- Lag: RAW -> STAGING -> ANALYTICS
--
-- VIGTIGT: Denne fil køres IKKE længere af koden. Den er et
-- menneskelæsbart snapshot af det fulde, aktuelle skema. Den autoritative
-- kilde til sandhed er migrations/*.sql, anvendt via src/database.py
-- (apply_migrations). Hold denne fil synkroniseret manuelt når du
-- tilføjer en ny migration, men lad ALDRIG denne fil og migrations/
-- modsige hinanden - se docs/architecture-decisions.md.
--
-- Designet til SQLite, men bevidst skrevet så det er let at forstå
-- forskellene til SQL Server (se README.md -> "SQLite -> SQL Server
-- mapping" for hvorfor det IKKE kun er et connection-string-skift).
-- =========================================================

-- ---------------------------------------------------------
-- FILE-LEVEL IDEMPOTENCY
-- Formål: forhindre at den SAMME fysiske fil (indhold) bliver
-- indlæst i raw_meter_readings flere gange.
--
-- Vi bruger SHA-256 af filens rå bytes som nøgle:
--   - samme filnavn + samme indhold  -> springes over (samme hash)
--   - samme filnavn + ændret indhold -> ny version, behandles som nyt
--   - forskelligt filnavn + identisk indhold -> også sprunget over,
--     fordi vi bevidst vælger CONTENT-baseret idempotency frem for
--     filnavn-baseret idempotency. Trade-off: to legitimt forskellige
--     kilder der tilfældigvis sender byte-identisk indhold under
--     forskellige navne vil kollidere. For dette projekt (ét feed,
--     én kilde) er det en fornuftig og enkel antagelse.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingested_files (
    file_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name           TEXT NOT NULL,
    file_hash           TEXT NOT NULL UNIQUE,   -- content-based idempotency-nøgle
    first_seen_at       TEXT NOT NULL,
    processed_at        TEXT,
    row_count           INTEGER DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED'))
);

-- ---------------------------------------------------------
-- RAW LAYER
-- Formål: gemme data så tæt på kilden som muligt.
-- Ingen constraints på forretningslogik her (fx ingen CHECK på
-- negative værdier) - vi vil kunne gemme "beskidt" data som det er.
--
-- Bemærk (præcisering af RAW-semantik): pandas' CSV/JSON-parsing er
-- IKKE byte-for-byte identisk med kildefilen (tomme felter -> NaN/None,
-- JSON-typer normaliseres til str). raw_payload gemmer derfor en JSON-
-- repræsentation af den oprindelige kilderække, så vi altid kan se
-- præcis hvad der stod i den enkelte celle. Kildefilen selv er den
-- egentlige immutable "sandhed" - raw_meter_readings er en forespørgelig
-- landing-repræsentation af den - ikke en byte-kopi.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_meter_readings (
    raw_id              INTEGER PRIMARY KEY AUTOINCREMENT, -- SQL Server: IDENTITY(1,1)
    file_id             INTEGER,  -- reference til ingested_files
    meter_id            TEXT,
    timestamp_raw       TEXT,     -- gemt som TEXT/rå streng, ikke parset endnu
    consumption_liters  TEXT,     -- bevidst TEXT: kilden kan sende ugyldige værdier
    temperature         TEXT,
    status              TEXT,
    raw_payload         TEXT,     -- JSON af den oprindelige kilderække
    source_file         TEXT NOT NULL,   -- hvilken fil kom denne række fra?
    ingested_at         TEXT NOT NULL,   -- hvornår blev den indlæst (UTC ISO8601)

    FOREIGN KEY (file_id) REFERENCES ingested_files (file_id)
);

-- ---------------------------------------------------------
-- STAGING LAYER
-- Formål: kun gyldige, normaliserede, deduplikerede records.
-- Her håndhæver vi forretningsregler via constraints.
--
-- raw_id giver LINEAGE: hver staging-række peger tilbage på den
-- præcise raw-række den blev udledt af.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_meter_readings (
    stg_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id              INTEGER NOT NULL,  -- lineage: hvilken raw-række stammer dette fra?
    meter_id            TEXT NOT NULL,
    reading_timestamp   TEXT NOT NULL,     -- ISO8601, parset og valideret
    consumption_liters  REAL NOT NULL CHECK (consumption_liters >= 0),
    temperature         REAL,
    status              TEXT,
    source_file         TEXT NOT NULL,
    processed_at        TEXT NOT NULL,

    -- Idempotency: samme måler kan ikke have to målinger på samme tidspunkt.
    -- Dette er en DATABASE CONSTRAINT - den sidste forsvarslinje mod dubletter,
    -- uafhængig af om applikationskoden husker at tjekke selv.
    UNIQUE (meter_id, reading_timestamp),
    FOREIGN KEY (raw_id) REFERENCES raw_meter_readings (raw_id)
);

-- ---------------------------------------------------------
-- DATA QUALITY ERRORS / WARNINGS
-- Formål: "Bad data should be observable, not silently discarded."
--
-- severity skelner mellem:
--   ERROR   -> rækken blev AFVIST (findes ikke i staging)
--   WARNING -> rækken blev ACCEPTERET, men har en kvalitetsbemærkning
--              (fx ikke-parsbar temperatur, hvor kernemålingen er gyldig)
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_quality_errors (
    error_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id              INTEGER,          -- reference til raw_meter_readings, hvis kendt
    meter_id            TEXT,
    reading_timestamp   TEXT,
    error_type          TEXT NOT NULL,    -- fx 'MISSING_METER_ID', 'NEGATIVE_CONSUMPTION'
    severity            TEXT NOT NULL DEFAULT 'ERROR'
        CHECK (severity IN ('ERROR', 'WARNING')),
    error_detail        TEXT,
    source_file         TEXT,
    detected_at         TEXT NOT NULL,
    FOREIGN KEY (raw_id) REFERENCES raw_meter_readings (raw_id)
);

-- ---------------------------------------------------------
-- ANALYTICS LAYER: DIMENSION
-- Formål: beskriver "hvem" en måler er - stabile attributter.
--
-- EVENT TIME vs PROCESSING TIME (se README):
--   first_reading_at / last_reading_at -> udledt af reading_timestamp
--     (hvornår skete målingen ifølge KILDEN). MIN/MAX-semantik, så
--     sent-ankommende historiske målinger stadig opdaterer korrekt.
--   created_at / updated_at -> hvornår PIPELINEN rørte ved rækken
--     (system-/processeringstid).
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_meter (
    meter_key           INTEGER PRIMARY KEY AUTOINCREMENT, -- surrogate key
    meter_id            TEXT NOT NULL UNIQUE,              -- natural/business key fra kildesystemet
    first_reading_at     TEXT NOT NULL,   -- event time (MIN over reading_timestamp)
    last_reading_at      TEXT NOT NULL,   -- event time (MAX over reading_timestamp)
    created_at           TEXT NOT NULL,   -- processing time: da rækken først blev oprettet
    updated_at           TEXT NOT NULL    -- processing time: da rækken sidst blev rørt
);

-- ---------------------------------------------------------
-- ANALYTICS LAYER: FACT
-- Formål: én række per måling, klar til aggregering/analyse.
--
-- source_stg_id giver LINEAGE videre fra fact -> staging -> raw:
-- fact.source_stg_id -> stg_meter_readings.stg_id -> raw_meter_readings.
-- Se sql/analytics_queries.sql for et lineage/debugging-eksempel.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_water_consumption (
    fact_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    meter_key           INTEGER NOT NULL,
    source_stg_id       INTEGER NOT NULL,  -- lineage: hvilken staging-række stammer dette fra?
    reading_timestamp   TEXT NOT NULL,
    consumption_liters  REAL NOT NULL,
    temperature         REAL,
    status              TEXT,
    is_suspicious       INTEGER NOT NULL DEFAULT 0
        CHECK (is_suspicious IN (0, 1)),  -- 0/1 boolean (SQL Server: BIT)

    FOREIGN KEY (meter_key) REFERENCES dim_meter (meter_key),
    FOREIGN KEY (source_stg_id) REFERENCES stg_meter_readings (stg_id),
    UNIQUE (meter_key, reading_timestamp)  -- idempotency også her
);

-- ---------------------------------------------------------
-- PIPELINE LOGGING
-- Formål: observability - hvornår kørte pipeline'en, og hvad skete der.
-- Hvert tal skal kunne forklares entydigt (se README).
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time              TEXT NOT NULL,
    end_time                TEXT,
    status                  TEXT NOT NULL DEFAULT 'RUNNING'
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),

    files_discovered        INTEGER DEFAULT 0,  -- filer fundet i kildemappen denne kørsel
    files_ingested          INTEGER DEFAULT 0,  -- filer der reelt blev læst ind (ny hash)
    files_skipped           INTEGER DEFAULT 0,  -- filer sprunget over (allerede set indhold)

    raw_rows_ingested       INTEGER DEFAULT 0,  -- rækker skrevet til raw_meter_readings
    raw_rows_validated      INTEGER DEFAULT 0,  -- raw-rækker behandlet af validation

    staging_rows_inserted   INTEGER DEFAULT 0,  -- nye rækker i stg_meter_readings
    staging_rows_rejected   INTEGER DEFAULT 0,  -- rækker afvist til data_quality_errors (ERROR)
    duplicates_skipped      INTEGER DEFAULT 0,  -- rækker der allerede fandtes (ikke en fejl)

    facts_inserted          INTEGER DEFAULT 0,  -- nye rækker i fact_water_consumption

    -- Incremental load "watermarks": vi bruger monotont stigende,
    -- database-genererede ID'er (ikke kilde-timestamps) som vandmærke.
    -- Se forklaring i src/pipeline.py for hvorfor.
    last_processed_raw_id  INTEGER DEFAULT 0,
    last_processed_stg_id  INTEGER DEFAULT 0,

    -- FAILED-run diagnostik (migration 003): et kort operationelt
    -- sammendrag af HVOR og HVORFOR en kørsel fejlede. Fulde stack traces
    -- hører til i Python-loggeren, ikke i databasen.
    failed_stage            TEXT,   -- INGESTION / VALIDATION / TRANSFORMATION / FINALIZATION
    error_type               TEXT,   -- exception class name, fx 'ValueError'
    error_message            TEXT    -- kort, menneskelæsbar fejlbesked
);

-- ---------------------------------------------------------
-- INDEXES (migration 002)
-- Se docs/architecture-decisions.md og sql/query_plan_examples.sql for
-- hvilke queries hver af disse understøtter, og hvorfor vi IKKE
-- indekserer alt.
-- ---------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_raw_meter_readings_file_id
    ON raw_meter_readings (file_id);
CREATE INDEX IF NOT EXISTS idx_stg_meter_readings_raw_id
    ON stg_meter_readings (raw_id);
CREATE INDEX IF NOT EXISTS idx_fact_water_consumption_source_stg_id
    ON fact_water_consumption (source_stg_id);
CREATE INDEX IF NOT EXISTS idx_fact_water_consumption_reading_timestamp
    ON fact_water_consumption (reading_timestamp);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON pipeline_runs (status);
