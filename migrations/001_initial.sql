-- =========================================================
-- MIGRATION 001: initial schema
-- Lag: RAW -> STAGING -> ANALYTICS
-- Denne fil repræsenterer den fulde, konsoliderede skema-tilstand fra
-- det første review-forløb (fil-idempotency, lineage, severity,
-- event/processing time var alle allerede indført inden migrations blev
-- tilføjet). Fremadrettet sker ALLE skema-ændringer som NYE migrationsfiler
-- - denne fil ændres ALDRIG efter den er "udgivet" (samme regel som man
-- ville følge med rigtige migrationsværktøjer som Alembic/Flyway).
--
-- CHECK-constraints for enum-lignende kolonner (status/severity/boolean)
-- er inkluderet direkte her, fordi SQLite ikke understøtter
-- "ALTER TABLE ... ADD CONSTRAINT" - at tilføje en CHECK-constraint på en
-- eksisterende tabel kræver en tabel-ombygning (create-copy-drop-rename).
-- Se docs/architecture-decisions.md for hvorfor vi undgår det her.
-- =========================================================

CREATE TABLE IF NOT EXISTS ingested_files (
    file_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name           TEXT NOT NULL,
    file_hash           TEXT NOT NULL UNIQUE,
    first_seen_at       TEXT NOT NULL,
    processed_at        TEXT,
    row_count           INTEGER DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED'))
);

CREATE TABLE IF NOT EXISTS raw_meter_readings (
    raw_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id             INTEGER,
    meter_id            TEXT,
    timestamp_raw       TEXT,
    consumption_liters  TEXT,
    temperature         TEXT,
    status              TEXT,
    raw_payload         TEXT,
    source_file         TEXT NOT NULL,
    ingested_at         TEXT NOT NULL,

    FOREIGN KEY (file_id) REFERENCES ingested_files (file_id)
);

CREATE TABLE IF NOT EXISTS stg_meter_readings (
    stg_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id              INTEGER NOT NULL,
    meter_id            TEXT NOT NULL,
    reading_timestamp   TEXT NOT NULL,
    consumption_liters  REAL NOT NULL CHECK (consumption_liters >= 0),
    temperature         REAL,
    status              TEXT,
    source_file         TEXT NOT NULL,
    processed_at        TEXT NOT NULL,

    UNIQUE (meter_id, reading_timestamp),
    FOREIGN KEY (raw_id) REFERENCES raw_meter_readings (raw_id)
);

CREATE TABLE IF NOT EXISTS data_quality_errors (
    error_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id              INTEGER,
    meter_id            TEXT,
    reading_timestamp   TEXT,
    error_type          TEXT NOT NULL,
    severity            TEXT NOT NULL DEFAULT 'ERROR'
        CHECK (severity IN ('ERROR', 'WARNING')),
    error_detail        TEXT,
    source_file         TEXT,
    detected_at         TEXT NOT NULL,
    FOREIGN KEY (raw_id) REFERENCES raw_meter_readings (raw_id)
);

CREATE TABLE IF NOT EXISTS dim_meter (
    meter_key           INTEGER PRIMARY KEY AUTOINCREMENT,
    meter_id            TEXT NOT NULL UNIQUE,
    first_reading_at    TEXT NOT NULL,
    last_reading_at     TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_water_consumption (
    fact_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    meter_key           INTEGER NOT NULL,
    source_stg_id       INTEGER NOT NULL,
    reading_timestamp   TEXT NOT NULL,
    consumption_liters  REAL NOT NULL,
    temperature         REAL,
    status              TEXT,
    is_suspicious       INTEGER NOT NULL DEFAULT 0
        CHECK (is_suspicious IN (0, 1)),

    FOREIGN KEY (meter_key) REFERENCES dim_meter (meter_key),
    FOREIGN KEY (source_stg_id) REFERENCES stg_meter_readings (stg_id),
    UNIQUE (meter_key, reading_timestamp)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time              TEXT NOT NULL,
    end_time                TEXT,
    status                  TEXT NOT NULL DEFAULT 'RUNNING'
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),

    files_discovered        INTEGER DEFAULT 0,
    files_ingested          INTEGER DEFAULT 0,
    files_skipped           INTEGER DEFAULT 0,

    raw_rows_ingested       INTEGER DEFAULT 0,
    raw_rows_validated      INTEGER DEFAULT 0,

    staging_rows_inserted   INTEGER DEFAULT 0,
    staging_rows_rejected   INTEGER DEFAULT 0,
    duplicates_skipped      INTEGER DEFAULT 0,

    facts_inserted          INTEGER DEFAULT 0,

    last_processed_raw_id  INTEGER DEFAULT 0,
    last_processed_stg_id  INTEGER DEFAULT 0
);
