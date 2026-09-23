-- =========================================================
-- query_plan_examples.sql
--
-- Formål: dokumentere at indexes i migrations/002_add_indexes.sql
-- rent faktisk bliver brugt af SQLite's query planner, ikke bare
-- "findes i skemaet". Kør disse i sqlite3 CLI mod data/warehouse.db:
--
--   sqlite3 data/warehouse.db < sql/query_plan_examples.sql
--
-- EXPLAIN QUERY PLAN viser HVORDAN SQLite vil udføre en forespørgsel -
-- "SCAN" betyder fuld tabel-scan (dyrt, vokser lineært med tabellens
-- størrelse); "SEARCH ... USING INDEX" betyder et indeks blev brugt
-- (typisk O(log n) opslag).
-- =========================================================

-- 1) Lineage-join: raw -> stg
-- Understøttes af idx_stg_meter_readings_raw_id.
-- Uden indekset ville SQLite scanne HELE stg_meter_readings for hvert
-- raw_id vi leder efter.
EXPLAIN QUERY PLAN
SELECT r.raw_id, r.source_file, s.stg_id
FROM raw_meter_readings r
JOIN stg_meter_readings s ON s.raw_id = r.raw_id
WHERE r.raw_id = 1;

-- 2) Lineage-join: stg -> fact
-- Understøttes af idx_fact_water_consumption_source_stg_id.
EXPLAIN QUERY PLAN
SELECT f.fact_id, f.meter_key
FROM stg_meter_readings s
JOIN fact_water_consumption f ON f.source_stg_id = s.stg_id
WHERE s.stg_id = 1;

-- 3) Tidsbaseret analytics-forespørgsel (fx daglige totaler)
-- Understøttes af idx_fact_water_consumption_reading_timestamp.
EXPLAIN QUERY PLAN
SELECT DATE(reading_timestamp) AS day, SUM(consumption_liters)
FROM fact_water_consumption
WHERE reading_timestamp >= '2026-09-01'
GROUP BY day;

-- 4) Seneste succesfulde kørsel (bruges af incremental load-logikken)
-- Understøttes af idx_pipeline_runs_status.
EXPLAIN QUERY PLAN
SELECT last_processed_raw_id, last_processed_stg_id
FROM pipeline_runs
WHERE status = 'SUCCESS'
ORDER BY run_id DESC
LIMIT 1;

-- 5) MODEXEMPEL: en forespørgsel der IKKE er indekseret, med vilje.
-- data_quality_errors.error_type har lav kardinalitet (få distinkte
-- værdier) og forespørges sjældent isoleret uden dato-filter - derfor
-- er en fuld scan her et bevidst, acceptabelt valg (se
-- docs/architecture-decisions.md, ADR om indexing).
EXPLAIN QUERY PLAN
SELECT error_type, COUNT(*)
FROM data_quality_errors
GROUP BY error_type;
