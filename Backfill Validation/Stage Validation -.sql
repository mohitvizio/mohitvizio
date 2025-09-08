-- Databricks notebook source
SELECT DATE_TRUNC('DAY', session_start)
, CASE WHEN fk_station_id IS NOT NULL AND tms_station_id IS NOT NULL THEN 'TIVO + TMS not null'
       WHEN fk_station_id IS NOT NULL AND tms_station_id IS NULL THEN 'TIVO not null, TMS null'
       WHEN fk_station_id IS NULL AND tms_station_id IS NOT NULL THEN 'TIVO null, TMS not null'
       WHEN fk_station_id IS NULL AND tms_station_id IS NULL THEN 'TIVO + TMS null' END AS station_id_check
, COUNT(*)
FROM stage.detection.viewing_content_firehose
WHERE session_start >= '2024-08-08T20:00:00'
AND partition_key >= '2024-08-08'
GROUP BY 1, 2

-- COMMAND ----------

SELECT DATE(session_start)
, CASE WHEN fk_station_id IS NOT NULL AND tms_station_id IS NOT NULL THEN 'TIVO + TMS not null'
       WHEN fk_station_id IS NULL AND tms_station_id IS NOT NULL THEN 'TIVO not null, TMS null'
       WHEN fk_station_id IS NOT NULL AND tms_station_id IS NULL THEN 'TIVO null, TMS not null'
       WHEN fk_station_id IS NULL AND tms_station_id IS NULL THEN 'TIVO + TMS null' END AS station_id_check
, COUNT(*)
FROM dev.detection.viewing_content_firehose
WHERE session_start >= '2024-08-08T20:00:00.000'
AND partition_key >= '2024-08-08'
GROUP BY 1, 2

-- COMMAND ----------

SELECT DATE(session_start)
, tms_station_id
, COUNT(*)
FROM stage.detection.viewing_content_firehose
WHERE session_start >= '2024-08-08T20:00:00.000'
AND partition_key >= '2024-08-08'
AND fk_station_id IS NULL AND tms_station_id IS NOT NULL
GROUP BY 1, 2

-- COMMAND ----------

SELECT st.station_name, COUNT(*)
FROM stage.detection.viewing_content_firehose vc
JOIN stage.detection.epg_station st
  ON st.station_id = vc.tms_station_id
  AND st.vendor_name = 'TIVO'
WHERE session_start >= '2024-08-08T20:00:00.000'
AND partition_key >= '2024-08-08'
AND fk_station_id IS NULL AND tms_station_id IS NOT NULL
GROUP BY 1
-- ORDER BY fk_tvid, session_start
-- LIMIT 1000

-- COMMAND ----------

SELECT *
FROM stage.detection.viewing_content_firehose
WHERE session_start >= '2024-08-08T20:00:00.000'
AND partition_key >= '2024-08-08'
AND fk_station_id IS NULL AND tms_station_id IS NOT NULL
ORDER BY fk_tvid, session_start
LIMIT 1000

-- COMMAND ----------

SELECT st.station_name, COUNT(*)
FROM dev.detection.viewing_content_firehose vc
JOIN stage.detection.epg_station st
  ON st.station_id = vc.tms_station_id
  AND st.vendor_name = 'TIVO'
WHERE session_start >= '2024-08-08T20:00:00.000'
AND partition_key >= '2024-08-08'
AND fk_station_id IS NULL AND tms_station_id IS NOT NULL
GROUP BY 1
-- ORDER BY fk_tvid, session_start
-- LIMIT 1000

-- COMMAND ----------

SELECT DATE_TRUNC('DAY', session_start)
, CASE WHEN fk_show_id IS NOT NULL AND tms_show_id IS NOT NULL THEN 'TIVO + TMS not null'
       WHEN fk_show_id IS NOT NULL AND tms_show_id IS NULL THEN 'TIVO not null, TMS null'
       WHEN fk_show_id IS NULL     AND tms_show_id IS NOT NULL THEN 'TIVO null, TMS not null'
       WHEN fk_show_id IS NULL     AND tms_show_id IS NULL THEN 'TIVO + TMS null' END AS show_id_check
, COUNT(*)
FROM stage.detection.viewing_content_firehose
WHERE session_start >= '2024-08-08T20:00:00.000'
AND partition_key >= '2024-08-08'
GROUP BY 1, 2

-- COMMAND ----------

SELECT DATE_TRUNC('DAY', session_start)
, CASE WHEN fk_show_id IS NOT NULL AND tms_show_id IS NOT NULL THEN 'TIVO + TMS not null'
       WHEN fk_show_id IS NOT NULL AND tms_show_id IS NULL THEN 'TIVO not null, TMS null'
       WHEN fk_show_id IS NULL     AND tms_show_id IS NOT NULL THEN 'TIVO null, TMS not null'
       WHEN fk_show_id IS NULL     AND tms_show_id IS NULL THEN 'TIVO + TMS null' END AS show_id_check
, COUNT(*)
FROM stage.detection.viewing_content_firehose
WHERE session_start >= '2024-08-08T20:00:00.000'
AND partition_key >= '2024-08-08'
GROUP BY 1, 2

-- COMMAND ----------

SELECT *
FROM stage.detection.viewing_content_firehose
WHERE session_start >= '2024-08-08T20:00:00'
AND partition_key >= '2024-08-08'
AND fk_show_id IS NOT NULL AND tms_show_id IS NULL
ORDER BY fk_tvid, session_start
LIMIT 1000

-- COMMAND ----------

SELECT fk_station_id, COUNT(*)
FROM stage.detection.viewing_content_firehose
WHERE session_start >= '2024-08-08T20:00:00.000'
AND partition_key >= '2024-08-08'
AND fk_show_id IS NOT NULL AND tms_show_id IS NULL
GROUP BY 1
ORDER BY 2 DESC
-- ORDER BY fk_tvid, session_start
-- LIMIT 1000

-- COMMAND ----------


SELECT sch.fk_station_id
, MAX(sch.airdate)
, COUNT(*) as num_airings
, COUNT(DISTINCT sch.fk_show_id) as num_shows
FROM prod.detection.epg_schedule_latest sch
JOIN prod.detection.inscape_station_map ism
  ON ism.mapped_vendor = 'TMS'
 AND ism.mapped_vendor_station_id = sch.fk_station_id
-- WHERE sch.airdate >= '2024-07-05'
GROUP BY 1
