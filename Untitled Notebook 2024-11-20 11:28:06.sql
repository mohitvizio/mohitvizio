-- Databricks notebook source
SELECT * FROM detection.epg_schedule sch
JOIN detection.inscape_station_map ism
 ON ism.mapped_vendor_station_id = sch.fk_station_id
 WHERE fk_show_id = 2189877 AND airdate >= '2024-11-02'

-- COMMAND ----------

SELECT * FROM detection.epg_show WHERE database_key = '1061095499'

-- COMMAND ----------

SELECT * FROM detection.logo_detection LIMIT 100

-- COMMAND ----------

WITH old_show_picks AS (
  SELECT fk_tvid, session_start, session_end, fk_show_id, airdate, GREATEST(media_time_start, media_time_end) AS max_mt, fk_station_id
  , NVL(fk_dma_id, 178) AS dma_id
  --, media_time_start, media_time_end
  FROM prod.detection.viewing_content_firehose
  WHERE fk_zoo_id = 17
    AND session_start >= CURRENT_DATE - 1
    AND is_live = FALSE
    AND fk_content_id != 3468026
    AND fk_station_id IS NOT NULL
    AND fk_show_id IS NOT NULL
    AND airdate IS NOT NULL
    AND media_time_start IS NOT NULL
    AND TIMESTAMPDIFF(HOUR, airdate, session_start) > 72
    AND session_duration > 0
  GROUP BY ALL
)
, sch AS (
  SELECT sch.fk_show_id, sch.airdate, sch.duration, sch.fk_station_id, CASE WHEN st.local_or_national = 'National' THEN 0 ELSE NVL(st.fk_dma_id, 0) END AS dma_id
  FROM detection.epg_schedule sch
  JOIN detection.inscape_station_map ism
    ON ism.mapped_vendor_station_id = sch.fk_station_id
   AND ism.mapped_vendor = 'TIVO'
  JOIN detection.epg_station st
    ON st.station_id = ism.mapped_vendor_station_id
   AND st.vendor_name = 'TIVO'
  WHERE sch.vendor_name = 'TIVO'
    AND sch.airdate > CURRENT_DATE - 6
    AND sch.airdate <= CURRENT_DATE + 1
  GROUP BY ALL
)
SELECT COUNT(DISTINCT fk_tvid||'_'||session_start)
FROM old_show_picks vc
JOIN sch
  ON sch.fk_show_id = vc.fk_show_id
 AND sch.airdate > vc.airdate
 AND sch.airdate <= vc.session_start
 AND TIMESTAMPDIFF(HOUR, sch.airdate, vc.session_start) <= 72
 AND vc.max_mt <= sch.duration
 AND vc.fk_station_id = sch.fk_station_id
 AND (vc.dma_id = sch.dma_id OR sch.dma_id = 0)

-- COMMAND ----------

SELECT COUNT(DISTINCT fk_tvid||'_'||session_start)
FROM prod.detection.viewing_content_firehose
WHERE fk_zoo_id = 17
  AND session_start >= CURRENT_DATE - 1
  AND is_live = FALSE
  AND fk_content_id != 3468026
  AND fk_station_id IS NOT NULL
  AND fk_show_id IS NOT NULL
  AND airdate IS NOT NULL
  AND media_time_start IS NOT NULL
  AND TIMESTAMPDIFF(HOUR, airdate, session_start) > 72
  AND session_duration > 0

-- COMMAND ----------

SELECT session_hour
, device
, COUNT(DISTINCT fk_tvid) AS tv_count
FROM (
SELECT DATE_TRUNC('HOUR', vc.session_start) AS session_hour
, vc.fk_tvid
, lookup.device
FROM detection.viewing_content_firehose vc
LEFT JOIN detection.logo_detection ld
  ON vc.session_start >= ld.view_ts
 AND vc.session_start < ld.next_match_ts
 AND vc.fk_tvid = ld.tvid
 AND vc.fk_input_source_id = ld.fk_input_source_id
LEFT JOIN detection.logo_detection_lookup lookup
  ON ld.corrected_logo_id = lookup.id
WHERE vc.session_start >= '2024-11-19 00:00:00'::TIMESTAMP
  AND ld.next_match_ts >= '2024-11-19 00:00:00'::TIMESTAMP
  AND vc.partition_key >= '2024-11-19'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
GROUP BY 1, 2, 3)
GROUP BY 1, 2

-- COMMAND ----------

SELECT session_hour
, device_count
, COUNT(DISTINCT fk_tvid) AS tv_count
FROM (
  SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
  , vc.fk_tvid
  , COUNT(DISTINCT lookup.device) AS device_count
  FROM detection.viewing_content_firehose vc
  LEFT JOIN detection.logo_detection ld
    ON vc.session_start >= ld.view_ts
    AND vc.session_start < ld.next_match_ts
    AND vc.fk_tvid = ld.tvid
    AND vc.fk_input_source_id = ld.fk_input_source_id
  LEFT JOIN detection.logo_detection_lookup lookup
    ON ld.corrected_logo_id = lookup.id
  WHERE vc.session_start >= '2024-11-19 00:00:00'::TIMESTAMP
    AND ld.next_match_ts >= '2024-11-19 00:00:00'::TIMESTAMP
    AND vc.partition_key >= '2024-11-19'
    AND vc.fk_zoo_id = 17
    AND vc.session_duration > 0
    -- AND lookup.device != 'Unknown'
  GROUP BY 1, 2)
GROUP BY 1, 2

-- COMMAND ----------

SELECT DATE(match_ts), COUNT(*), AVG(keypoints_matched*1.0) AS avg_match, AVG(keypoints_sent) AS avg_sent
FROM detection.logo_detection
WHERE match_ts >= CURRENT_DATE - INTERVAL 10 DAY
GROUP BY 1

-- COMMAND ----------

SELECT 'DP4', COUNT(*) FROM prod.detection.viewing_content_firehose
WHERE fk_content_id IS NULL
  AND session_start >= DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 24 HOURS
  AND session_start < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 1 HOURS
  AND fk_zoo_id = 17
UNION
SELECT 'DP5', COUNT(*)
FROM qa.detection.viewing_content_firehose
WHERE fk_content_id IS NULL
  AND session_start >= DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 24 HOURS
  AND session_start < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 1 HOURS
  AND fk_zoo_id = 17
