-- Databricks notebook source
SELECT DATE(create_timestamp), input_device, COUNT(DISTINCT fk_tvid)
FROM dev.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2

-- COMMAND ----------

SELECT DATE(create_timestamp), input_device, COUNT(DISTINCT fk_tvid)
FROM prod.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2

-- COMMAND ----------

SELECT DATE(e.create_timestamp)
, e.input_device AS ex_input
, n.input_device AS new_input
, COUNT(DISTINCT e.fk_tvid)
FROM prod.detection.tv_input_stats_firehose AS e
JOIN dev.detection.tv_input_stats_firehose AS n
  ON e.fk_tvid = n.fk_tvid
 AND e.create_timestamp = n.create_timestamp
 AND e.category <=> n.category
  AND e.fk_input_source_id = n.fk_input_source_id
WHERE e.total_duration >0
AND n.total_duration >0
AND e.create_timestamp = '2024-08-12'
AND n.create_timestamp = '2024-08-12'
AND COALESCE(e.input_device, '') != COALESCE(n.input_device, '')
GROUP BY 1, 2, 3

-- COMMAND ----------

SELECT e.*
FROM prod.detection.tv_input_stats_firehose AS e
JOIN dev.detection.tv_input_stats_firehose AS n
  ON e.fk_tvid = n.fk_tvid
 AND e.create_timestamp = n.create_timestamp
 AND e.category <=> n.category
 AND e.fk_input_source_id = n.fk_input_source_id
WHERE e.total_duration >0
AND n.total_duration >0
AND e.create_timestamp = '2024-08-12'
AND n.create_timestamp = '2024-08-12'
AND e.input_device = 'OTA'
AND n.input_device IS NULL
-- AND COALESCE(e.input_device, '') != COALESCE(n.input_device, '')
ORDER BY e.fk_tvid
LIMIT 100

-- COMMAND ----------

SELECT n.*
FROM dev.detection.tv_input_stats_firehose AS n
WHERE n.total_duration >0
AND n.create_timestamp = '2024-08-12'
-- AND n.input_device IS NULL
AND fk_tvid=3341389
-- AND COALESCE(e.input_device, '') != COALESCE(n.input_device, '')
-- ORDER BY e.fk_tvid
-- LIMIT 100

-- COMMAND ----------

SELECT percent_local_frames IS NULL, COUNT(*)
FROM prod.detection.tv_input_stats_firehose AS n
WHERE n.total_duration >0
AND n.create_timestamp = '2024-08-12'
-- AND (percent_local_frames < .99 OR percent_local_frames IS NULL OR category NOT IN ('SD TV','HD TV', 'OTHER'))
AND n.subcategory = 'OTA'
GROUP BY 1

-- COMMAND ----------

SELECT percent_local_frames IS NULL, COUNT(*)
FROM dev.detection.tv_input_stats_firehose AS n
WHERE n.total_duration >0
AND n.create_timestamp = '2024-08-12'
-- AND (percent_local_frames < .99 OR percent_local_frames IS NULL OR category NOT IN ('SD TV','HD TV', 'OTHER'))
-- AND n.input_device = 'OTA'
GROUP BY 1

-- COMMAND ----------

SELECT DATE(create_timestamp), input_device, 'Existing' AS table_name,  COUNT(DISTINCT fk_tvid) AS tv_count, SUM(total_duration)/3600.0 AS ttl_duration
FROM prod.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2
UNION
SELECT DATE(create_timestamp), input_device, 'New' AS table_name,  COUNT(DISTINCT fk_tvid) AS tv_count, SUM(total_duration)/3600.0 AS ttl_duration
FROM dev.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2

-- COMMAND ----------

SELECT DATE(create_timestamp), category, 'Existing' AS table_name, COUNT(DISTINCT fk_tvid) AS tv_count, SUM(total_duration)/3600.0 AS ttl_duration
FROM prod.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2
UNION
SELECT DATE(create_timestamp), category, 'New' AS table_name, COUNT(DISTINCT fk_tvid) AS tv_count, SUM(total_duration)/3600.0 AS ttl_duration
FROM dev.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2

-- COMMAND ----------

SELECT DATE(create_timestamp), subcategory, 'Existing' AS table_name, COUNT(DISTINCT fk_tvid) AS tv_count, SUM(total_duration)/3600.0 AS ttl_duration
FROM prod.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2
UNION
SELECT DATE(create_timestamp), subcategory, 'New' AS table_name, COUNT(DISTINCT fk_tvid) AS tv_count, SUM(total_duration)/3600.0 AS ttl_duration
FROM dev.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2

-- COMMAND ----------

SELECT DATE(create_timestamp), input_device_source, 'Existing' AS table_name, COUNT(DISTINCT fk_tvid) AS tv_count, SUM(total_duration)/3600.0 AS ttl_duration
FROM prod.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2
UNION
SELECT DATE(create_timestamp), input_device_source, 'New' AS table_name, COUNT(DISTINCT fk_tvid) AS tv_count, SUM(total_duration)/3600.0 AS ttl_duration
FROM dev.detection.tv_input_stats_firehose
WHERE total_duration >0
AND create_timestamp = '2024-08-12'
GROUP BY 1, 2

-- COMMAND ----------

WITH epg_station_deduped AS (
              SELECT *
              FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY vendor_name, station_id ORDER BY created_at DESC) AS rn
                  FROM prod.detection.epg_station) ism
              WHERE ism.rn = 1
            )
SELECT vendor_name, COUNT(*), COUNT(DISTINCT station_id) FROM epg_station_deduped GROUP BY 1

-- COMMAND ----------


SELECT vendor_name, COUNT(*), COUNT(DISTINCT station_id) FROM prod.detection.epg_station GROUP BY 1

-- COMMAND ----------

SELECT 0.000 >= 0.99
