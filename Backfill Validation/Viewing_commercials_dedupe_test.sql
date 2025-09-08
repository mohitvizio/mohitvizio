-- Databricks notebook source
SELECT * FROM dev.detection.viewing_commercials_firehose LIMIT 100;

-- COMMAND ----------

SELECT MIN(session_start), MAX(session_end) FROM unit_tests.viewing_commercials_firehose_dedup;

-- COMMAND ----------

SELECT DATE_TRUNC('HOUR', session_start), fk_commercial_source_id, SPLIT_PART(tvog_num, '_', 2) AS client_name
, COUNT(*) AS ttl_sessions, COUNT(DISTINCT fk_tvid) AS tv_coumt, SUM(session_duration)/3600.0 AS ttl_duration
FROM unit_tests.viewing_commercials_firehose_dedup
WHERE SPLIT_PART(tvog_num, '_', 2) IN ('kinetiq', 'ispot', 'springserve-prod')
GROUP BY 1,2,3;

-- COMMAND ----------

SELECT fk_tvid, session_duration, session_start, session_end, fk_commercial_id, SPLIT_PART(tvog_num, '_', 2) AS client_name, external_id
FROM unit_tests.viewing_commercials_firehose_dedup
WHERE SPLIT_PART(tvog_num, '_', 2) IN ('kinetiq', 'ispot', 'springserve-prod')
ORDER BY fk_tvid, client_name, session_start
LIMIT 1000;

-- COMMAND ----------

SELECT fk_tvid, session_duration, session_start, session_end, fk_commercial_id, tvog_num, external_id
FROM unit_tests.viewing_commercials_firehose_dedup
WHERE SPLIT_PART(tvog_num, '_', 2) IN ('kinetiq', 'ispot', 'springserve-prod')
ORDER BY fk_tvid, session_start
LIMIT 1000;

-- COMMAND ----------


SELECT * FROM prod.staging.logo_detection LIMIT 100

-- COMMAND ----------

SELECT * FROM detection.clients WHERE client_name = 'kinetiq'

-- COMMAND ----------

SELECT prev_vizio_epg_station IS NOT NULL, COUNT(*)
FROM prod.detection.viewing_commercials_firehose vc
JOIN prod.detection.commercial_id_external_firehose ce ON vc.fk_commercial_id = ce.fk_commercial_id
AND ce.fk_client_id = 753
WHERE partition_key = '2024-09-17'
AND session_start >= '2024-09-17 00:00:00'
AND session_start < '2024-09-18 00:00:00'
GROUP BY 1

-- COMMAND ----------

SELECT prev_vizio_epg_station IS NOT NULL, COUNT(*)
FROM dev.detection.viewing_commercials_firehose vc
JOIN prod.detection.commercial_id_external_firehose ce ON vc.fk_commercial_id = ce.fk_commercial_id
AND ce.fk_client_id = 753
WHERE partition_key = '2024-09-17'
AND session_start >= '2024-09-17 00:00:00'
AND session_start < '2024-09-18 00:00:00'
GROUP BY 1

-- COMMAND ----------

    SELECT blocked_apps.app_name
    FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_activity_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = 'nielsen'
    WHERE override.app_name IS NULL

-- COMMAND ----------

    SELECT blocked_apps.app_name
    FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = 'nielsen'
    WHERE override.app_name IS NULL

-- COMMAND ----------

WITH tm AS (
  SELECT *
  FROM detection.time_minute tm
  WHERE tm.minute_start >= '2024-09-19 13:00:00'::TIMESTAMP
  AND tm.minute_start <= '2024-09-23 13:00:00'::TIMESTAMP
)
SELECT tm.minute_start
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM detection.viewing_commercials_firehose_dedup_cfe_merge vc
JOIN tm
  ON tm.minute_start >= vc.session_start
WHERE vc.session_start >= '2024-09-19 13:00:00'::TIMESTAMP
    AND vc.session_end < '2024-09-23 13:00:00'::TIMESTAMP
    AND vc.fk_zoo_id = 17
    AND vc.session_duration > 0
    AND DATEDIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
    AND vc.partition_key >= '2024-09-19'
    AND vc.partition_key <= '2024-09-23'
GROUP BY 1

-- COMMAND ----------

-- WITH tm AS (
--   SELECT *
--   FROM detection.time_minute tm
--   WHERE tm.minute_start >= '2024-09-19 13:00:00'::TIMESTAMP
--   AND tm.minute_start <= '2024-09-23 13:00:00'::TIMESTAMP
-- )
SELECT tm.minute_start
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM detection.viewing_commercials_firehose vc
JOIN tm
  ON tm.minute_start >= vc.session_start
WHERE vc.session_start >= '2024-09-19 13:00:00'::TIMESTAMP
    AND vc.session_end < '2024-09-23 13:00:00'::TIMESTAMP
    AND vc.fk_zoo_id = 17
    AND vc.session_duration > 0
    AND DATEDIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
    AND vc.partition_key >= '2024-09-19'
    AND vc.partition_key <= '2024-09-23'
GROUP BY 1

-- COMMAND ----------

-- WITH tm AS (
--   SELECT *
--   FROM detection.time_minute tm
--   WHERE tm.minute_start >= '2024-09-19 13:00:00'::TIMESTAMP
--   AND tm.minute_start <= '2024-09-23 13:00:00'::TIMESTAMP
-- )
-- SELECT DATE_TRUNC('HOUR', session_start)
SELECT DATE_PART('HOUR', session_start) AS session_hour
, DATE_PART('DOW', session_start) AS day_of_week
, DATE(session_start) AS session_day
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
, COUNT(*) AS session_count
FROM detection.viewing_commercials_firehose vc
-- JOIN tm
--   ON tm.minute_start >= vc.session_start
WHERE vc.session_start >= '2024-09-10 00:00:00'::TIMESTAMP
  AND vc.session_end < '2024-09-25 00:00:00'::TIMESTAMP
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
--   AND DATEDIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key >= '2024-09-10'
  AND vc.partition_key <= '2024-09-24'
GROUP BY 1,2,3

-- COMMAND ----------

SELECT * FROM prod.detection.station_distribution_obfuscation_overwrite
