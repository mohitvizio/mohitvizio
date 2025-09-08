-- Databricks notebook source
-- MAGIC %md
-- MAGIC Step one to initialize a table for a client with 10M TVS

-- COMMAND ----------

CREATE TABLE dev.mohit_gangwani.ten_mil_sample_test AS
WITH latest_tvid AS (
  SELECT tvid, token
  , ROW_NUMBER() OVER (PARTITION BY token ORDER BY joined_date DESC) AS rn
  FROM detection.tv
  WHERE tv.oem = 'VIZIO'
),
one_year_active AS (
  SELECT fk_tvid
  FROM detection.tv_activity ta
  WHERE ta.session_end >= CURRENT_DATE - 365
    AND TIMESTAMPDIFF(SECOND, ta.session_start, ta.session_end) > 0
  GROUP BY 1
)
SELECT ld.tvid
FROM latest_tvid ld
JOIN one_year_active ta
  ON ta.fk_tvid = ld.tvid
JOIN prod.detection.tv_zoo_latest_daily tv_zoo
  ON tv_zoo.tvid = ld.tvid
 AND tv_zoo.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
JOIN prod.detection.tv_settings_latest_daily AS tv_settings
  ON ld.tvid = tv_settings.tvid
 AND UPPER(tv_settings.country_name) = 'USA'
JOIN prod.detection.tv_populations AS u
  ON ld.tvid = u.fk_tvid
JOIN prod.detection.populations AS pop
  ON u.fk_population_id = pop.population_id 
 AND pop.population_name = 'opted_in'
JOIN prod.detection.tv_geolocation_latest_daily tv_geo
  ON ld.tvid = tv_geo.tvid
 AND UPPER(tv_geo.country_code) = 'US'
WHERE ld.rn = 1
  AND MOD(ld.tvid, 23) < 10 -- This would be one option
  -- AND MOD(tvid, 23) >= 13 -- This would be another option
ORDER BY tvid DESC
LIMIT 10000000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Step 2 to find out how many TVs we would have on a daily basis for this month

-- COMMAND ----------

-- DBTITLE 1,Daily TV Count
select date_Trunc('month', session_start), count(distinct fk_tvid)
from prod.detection.viewing_content_firehose vc 
-- join dev.mohit_gangwani.ten_mil_sample_test tv
--   on tv.tvid = vc.fk_tvid
where session_start >= '2024-10-01'
  AND partition_key >= '2024-10-01'
  AND partition_key < '2024-11-01'
  AND fk_zoo_id = 17
  AND MOD(fk_tvid, 100) <= 47
group by 1

-- COMMAND ----------

SELECT a.name AS report_name
, a.customer_id
, a.frequency
, a.destination AS dest_a
, a.silence_alarms
, a.additionalfields
, c.name AS report_type
, b.name AS customer_name
, b.destination AS dest_b
, b.created
, b.modified
FROM dev.mohit_gangwani.rm_reports_dbricks a
JOIN dev.mohit_gangwani.rm_report_types c
  ON a.reporttype = c.id
JOIN dev.mohit_gangwani.rm_customers b
  ON a.customer_id = b.id
WHERE frequency = 'recurring'
  AND enabled = true
  AND b.name != 'cognet'

-- COMMAND ----------

SELECT TIMESTAMPDIFF(DAY, vc.airdate, vc.session_start) AS airdate_to_session_start_diff
, vc.is_live
, vc.file_ingested
, COUNT(*)
, COUNT(DISTINCT fk_show_id)
FROM detection.viewing_content_firehose vc
WHERE vc.partition_key >= '2024-11-01'
  AND vc.fk_zoo_id = 17
  AND vc.airdate IS NOT NULL
  AND vc.fk_show_id IS NOT NULL
  AND vc.fk_station_id IS NOT NULL
  AND vc.media_time_start IS NOT NULL
GROUP BY 1, 2, 3

-- COMMAND ----------

SELECT * FROM detection.epg_show WHERE database_key = 450450

-- COMMAND ----------

SELECT sch.fk_show_id, sch.airdate, sch.duration, st.station_name, sh.database_key, sh.title, sh.epi_title
FROM detection.epg_schedule sch
JOIN detection.epg_station st
  ON st.station_id = sch.fk_station_id
 AND st.vendor_name = 'TIVO'
JOIN detection.epg_show sh
  ON sh.show_id = sch.fk_show_id
 AND sh.vendor_name = 'TIVO'
WHERE sch.vendor_name = 'TIVO'
  AND sh.database_key = 604722020
  AND sch.airdate >= '2024-04-01'
  AND st.attributed = 'TRUE'
GROUP BY ALL
ORDER BY 2

-- COMMAND ----------

SELECT *
FROM dev.public.fileingest_cidmap
WHERE contenttype = 'content'
  AND reportingid LIKE '%450450%'
  -- AND status = 'DETECTION_COMPLETED'
LIMIT 10

-- COMMAND ----------

SELECT DATE_TRUNC('HOUR', vc.ts_start) AS session_hour
, COUNT(Distinct vc.tvid) AS total_tvs
, SUM(TIMESTAMPDIFF(SECOND, vc.ts_start, vc.ts_end))/3600.0 AS total_duration
, COUNT(*) AS session_count
FROM prod.staging.vizio_attrcomm_firehose vc
WHERE ts_start >= DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 3 HOURS
  AND ts_start < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 2 HOURS
GROUP BY 1

-- COMMAND ----------

SELECT DATE_TRUNC('HOUR', vc.ts_start) AS session_hour
, COUNT(Distinct vc.tvid) AS total_tvs
, SUM(TIMESTAMPDIFF(SECOND, vc.ts_start, vc.ts_end))/3600.0 AS total_duration
, COUNT(*) AS session_count
FROM prod.cooker.vizio_attrcomm_firehose vc
WHERE ts_start >= DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 3 HOURS
  AND ts_start < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 2 HOURS
GROUP BY 1
