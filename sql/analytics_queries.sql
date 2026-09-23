-- =========================================================
-- ANALYTICS QUERIES: øvelser til at træne SQL på fact/dim-modellen
-- Hver forespørgsel er beskrevet i README.md med forklaring.
-- =========================================================

-- 1. Samlet vandforbrug pr. dag
SELECT
    DATE(f.reading_timestamp) AS reading_date,
    SUM(f.consumption_liters) AS total_liters
FROM fact_water_consumption f
GROUP BY DATE(f.reading_timestamp)
ORDER BY reading_date;


-- 2. De 10 målere med størst samlet forbrug
SELECT
    d.meter_id,
    SUM(f.consumption_liters) AS total_liters
FROM fact_water_consumption f
JOIN dim_meter d ON d.meter_key = f.meter_key
GROUP BY d.meter_id
ORDER BY total_liters DESC
LIMIT 10;


-- 3. Gennemsnitligt forbrug pr. måler
SELECT
    d.meter_id,
    AVG(f.consumption_liters) AS avg_liters,
    COUNT(*) AS number_of_readings
FROM fact_water_consumption f
JOIN dim_meter d ON d.meter_key = f.meter_key
GROUP BY d.meter_id
ORDER BY avg_liters DESC;


-- 4. Målere hvor en given måling ligger markant over målerens eget
--    gennemsnit (window function: AVG OVER PARTITION BY)
SELECT *
FROM (
    SELECT
        d.meter_id,
        f.reading_timestamp,
        f.consumption_liters,
        AVG(f.consumption_liters) OVER (PARTITION BY d.meter_id) AS meter_avg
    FROM fact_water_consumption f
    JOIN dim_meter d ON d.meter_key = f.meter_key
) sub
WHERE sub.consumption_liters > sub.meter_avg * 2
ORDER BY sub.meter_id, sub.reading_timestamp;


-- 5. Records i staging der mangler en tilhørende dimension i analytics
--    (kan ske hvis transformation endnu ikke er kørt for de nyeste rækker)
SELECT s.*
FROM stg_meter_readings s
LEFT JOIN dim_meter d ON d.meter_id = s.meter_id
WHERE d.meter_key IS NULL;


-- 6. Analyse med GROUP BY: antal suspicious readings pr. måler
SELECT
    d.meter_id,
    COUNT(*) AS suspicious_count
FROM fact_water_consumption f
JOIN dim_meter d ON d.meter_key = f.meter_key
WHERE f.is_suspicious = 1
GROUP BY d.meter_id
ORDER BY suspicious_count DESC;


-- 7. JOIN mellem fact og dimension: fuld liste af målinger med målernavn
SELECT
    d.meter_id,
    f.reading_timestamp,
    f.consumption_liters,
    f.status,
    f.is_suspicious
FROM fact_water_consumption f
JOIN dim_meter d ON d.meter_key = f.meter_key
ORDER BY d.meter_id, f.reading_timestamp;


-- 8. Window function: LAG - forskel i forbrug ift. forrige måling for
--    samme måler (udvikling over tid)
SELECT
    d.meter_id,
    f.reading_timestamp,
    f.consumption_liters,
    LAG(f.consumption_liters) OVER (
        PARTITION BY d.meter_id ORDER BY f.reading_timestamp
    ) AS previous_reading,
    f.consumption_liters - LAG(f.consumption_liters) OVER (
        PARTITION BY d.meter_id ORDER BY f.reading_timestamp
    ) AS delta_liters
FROM fact_water_consumption f
JOIN dim_meter d ON d.meter_key = f.meter_key
ORDER BY d.meter_id, f.reading_timestamp;


-- 9. Data quality overblik: fejltyper og antal, nyeste først
SELECT
    error_type,
    COUNT(*) AS error_count,
    MAX(detected_at) AS last_seen
FROM data_quality_errors
GROUP BY error_type
ORDER BY error_count DESC;


-- 10. Pipeline health: seneste 5 kørsler
SELECT
    run_id, start_time, end_time, status,
    records_read, records_inserted, records_rejected
FROM pipeline_runs
ORDER BY run_id DESC
LIMIT 5;
