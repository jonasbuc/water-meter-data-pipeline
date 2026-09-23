-- =========================================================
-- SCHEMA: Vandmåler Data Pipeline
-- Lag: RAW -> STAGING -> ANALYTICS
-- Designet til SQLite, men bevidst skrevet så det er let at
-- portere til SQL Server (kommentarer markerer forskelle).
-- =========================================================

-- ---------------------------------------------------------
-- RAW LAYER
-- Formål: gemme data så tæt på kilden som muligt.
-- Ingen constraints på forretningslogik her (fx ingen CHECK på
-- negative værdier) - vi vil kunne gemme "beskidt" data som det er.
-- Vi tilføjer kun metadata om HVOR og HVORNÅR det kom fra.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_meter_readings (
    raw_id              INTEGER PRIMARY KEY AUTOINCREMENT, -- SQL Server: IDENTITY(1,1)
    meter_id            TEXT,
    timestamp_raw       TEXT,     -- gemt som TEXT/rå streng, ikke parset endnu
    consumption_liters  TEXT,     -- bevidst TEXT: kilden kan sende ugyldige værdier
    temperature         TEXT,
    status              TEXT,
    source_file         TEXT NOT NULL,   -- hvilken fil kom denne række fra?
    ingested_at         TEXT NOT NULL    -- hvornår blev den indlæst (UTC ISO8601)
);

-- ---------------------------------------------------------
-- STAGING LAYER
-- Formål: kun gyldige, normaliserede, deduplikerede records.
-- Her håndhæver vi forretningsregler via constraints.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_meter_readings (
    stg_id              INTEGER PRIMARY KEY AUTOINCREMENT,
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
    UNIQUE (meter_id, reading_timestamp)
);

-- ---------------------------------------------------------
-- DATA QUALITY ERRORS
-- Formål: "Bad data should be observable, not silently discarded."
-- Alt der afvises fra raw -> staging valideringen, logges her
-- i stedet for bare at forsvinde.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_quality_errors (
    error_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id              INTEGER,          -- reference til raw_meter_readings, hvis kendt
    meter_id            TEXT,
    reading_timestamp   TEXT,
    error_type          TEXT NOT NULL,    -- fx 'MISSING_METER_ID', 'NEGATIVE_CONSUMPTION'
    error_detail        TEXT,
    source_file         TEXT,
    detected_at         TEXT NOT NULL,
    FOREIGN KEY (raw_id) REFERENCES raw_meter_readings (raw_id)
);

-- ---------------------------------------------------------
-- ANALYTICS LAYER: DIMENSION
-- Formål: beskriver "hvem" en måler er - stabile attributter.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_meter (
    meter_key           INTEGER PRIMARY KEY AUTOINCREMENT, -- surrogate key
    meter_id            TEXT NOT NULL UNIQUE,              -- natural/business key fra kildesystemet
    first_seen_at       TEXT NOT NULL,
    last_seen_at        TEXT NOT NULL
);

-- ---------------------------------------------------------
-- ANALYTICS LAYER: FACT
-- Formål: én række per måling, klar til aggregering/analyse.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_water_consumption (
    fact_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    meter_key           INTEGER NOT NULL,
    reading_timestamp   TEXT NOT NULL,
    consumption_liters  REAL NOT NULL,
    temperature         REAL,
    status              TEXT,
    is_suspicious       INTEGER NOT NULL DEFAULT 0,  -- 0/1 boolean (SQL Server: BIT)

    FOREIGN KEY (meter_key) REFERENCES dim_meter (meter_key),
    UNIQUE (meter_key, reading_timestamp)  -- idempotency også her
);

-- ---------------------------------------------------------
-- PIPELINE LOGGING
-- Formål: observability - hvornår kørte pipeline'en, og hvad skete der.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time          TEXT NOT NULL,
    end_time            TEXT,
    records_read        INTEGER DEFAULT 0,
    records_inserted    INTEGER DEFAULT 0,
    records_rejected    INTEGER DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'RUNNING',  -- RUNNING / SUCCESS / FAILED

    -- Incremental load "watermarks": vi bruger monotont stigende,
    -- database-genererede ID'er (ikke kilde-timestamps) som vandmærke.
    -- Se forklaring i src/pipeline.py for hvorfor.
    last_processed_raw_id  INTEGER DEFAULT 0,
    last_processed_stg_id  INTEGER DEFAULT 0
);
