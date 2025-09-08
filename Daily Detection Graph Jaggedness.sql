-- Databricks notebook source
-- DBTITLE 1,All Chipsets
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,M Series
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset = 'MSERIES'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,5581p
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset = '5581p'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,5597
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset = '5597'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,NVT72690
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset = 'NVT72690'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,5691
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset = '5691'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,5583
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset = '5583'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,SIGMA_SX6
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset = 'SIGMA_SX6'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,SIGMA_SX7C
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset = 'SIGMA_SX7C'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,5581z
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset = '5581z'
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,Good Chipsets
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026 OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL OR vizio_epg_airing IS NOT NULL THEN 'Detected Session' ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset IN ('MSERIES','5597', '5581p')
GROUP BY 1, 2

-- COMMAND ----------

-- DBTITLE 1,Bad Chipsets
WITH tm AS (
  SELECT *
  FROM prod.detection.time_minute
  WHERE minute_start >= '2024-09-17 00:00:00'
    AND minute_start <= '2024-09-18 00:00:00'
)
SELECT tm.minute_start
, CASE WHEN vc.fk_content_id != 3468026
         OR COALESCE(tuner_channel_id, tuner_program_id) IS NOT NULL
         OR vizio_epg_airing IS NOT NULL THEN 'Detected Session'
       ELSE 'Null Session' END AS session_type
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
FROM prod.detection.viewing_content_firehose vc
JOIN detection.tv
  ON tv.tvid = vc.fk_tvid
JOIN tm
  ON tm.minute_start >= vc.session_start
 AND tm.minute_start < vc.session_end
WHERE vc.session_start >= '2024-09-17 00:00:00'
  AND vc.session_end < '2024-09-18 00:00:00'
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND DATE_DIFF(SECOND, GREATEST(vc.session_start, tm.minute_start), LEAST(tm.minute_stop, vc.session_end)) > 0
  AND vc.partition_key = '2024-09-17'
  AND tv.chipset IN ('SIGMA_SX7C', 'SIGMA_SX6', '5581z', '5691', '5583')
GROUP BY 1, 2

-- COMMAND ----------

SELECT chipset, COUNT(DISTINCT token)
FROM detection.tv
GROUP BY 1
ORDER BY 2 DESC
