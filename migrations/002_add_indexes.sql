-- =========================================================
-- MIGRATION 002: intentional indexing strategy
--
-- Vi indekserer KUN kolonner der reelt bruges i JOIN/WHERE/ORDER BY i
-- koden eller sql/analytics_queries.sql - ikke "alt for en sikkerheds
-- skyld". Se sql/query_plan_examples.sql for EXPLAIN QUERY PLAN-beviser,
-- og docs/architecture-decisions.md for trade-off-diskussionen
-- ("hvorfor ikke bare indeksere alt?").
-- =========================================================

-- raw_meter_readings.file_id: bruges hvis man vil slå op "hvilke raw-rækker
-- kom fra denne fil" (fx debugging af en enkelt fil-ingestion).
CREATE INDEX IF NOT EXISTS idx_raw_meter_readings_file_id
    ON raw_meter_readings (file_id);

-- stg_meter_readings.raw_id: understøtter lineage-joinet
-- (raw JOIN stg ON stg.raw_id = raw.raw_id), som bruges i lineage-queryen
-- i sql/analytics_queries.sql og i test_lineage_and_constraints.py.
CREATE INDEX IF NOT EXISTS idx_stg_meter_readings_raw_id
    ON stg_meter_readings (raw_id);

-- fact_water_consumption.source_stg_id: understøtter det andet led af
-- lineage-kæden (stg JOIN fact ON fact.source_stg_id = stg.stg_id).
CREATE INDEX IF NOT EXISTS idx_fact_water_consumption_source_stg_id
    ON fact_water_consumption (source_stg_id);

-- fact_water_consumption.reading_timestamp: understøtter tidsbaserede
-- analytics-forespørgsler (DATE(reading_timestamp) GROUP BY, window
-- functions ORDER BY reading_timestamp) i analytics_queries.sql.
CREATE INDEX IF NOT EXISTS idx_fact_water_consumption_reading_timestamp
    ON fact_water_consumption (reading_timestamp);

-- pipeline_runs.status: understøtter "seneste succesfulde kørsel"-opslaget
-- i src/pipeline.py (_get_last_processed_ids), som filtrerer på
-- status = 'SUCCESS' og sorterer på run_id.
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON pipeline_runs (status);

-- BEMÆRK hvad vi bevidst IKKE indekserer:
-- - stg_meter_readings(meter_id, reading_timestamp) har allerede en
--   UNIQUE-constraint, som SQLite automatisk bakker med et unikt index.
--   Endnu et index her ville være rent overhead (dobbelt vedligehold).
-- - dim_meter.meter_id har tilsvarende allerede UNIQUE.
-- - Vi indekserer ikke lav-kardinalitets-kolonner som is_suspicious eller
--   severity alene, fordi de sjældent forespørges isoleret nok til at et
--   index slår en fuld tabel-scan på denne datastørrelse.
