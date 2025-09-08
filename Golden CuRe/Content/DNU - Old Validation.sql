-- Databricks notebook source
(SELECT 'Matching Rows' AS table_name
 , DATE(exs_report.ts_start) AS report_date
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM dev.mohit_gangwani.content_with_null_adelaide_existing AS exs_report
 JOIN dev.mohit_gangwani.content_with_null_adelaide_new AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.ts_end > exs_report.ts_start
 AND new_report.ts_end > new_report.ts_start
 GROUP BY 2
)
UNION
(SELECT 'Existing CuRe' AS table_name
 , DATE(ts_start) AS report_date
 , COUNT(*) AS total_matching_rows
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM dev.mohit_gangwani.content_with_null_adelaide_existing AS exs_report
 WHERE exs_report.ts_end > exs_report.ts_start
 GROUP BY 2
)
UNION
(SELECT 'Golden CuRe' AS table_name
 , DATE(ts_start) AS report_date
 , COUNT(*) AS total_matching_rows
 , COUNT(DISTINCT new_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM dev.mohit_gangwani.content_with_null_adelaide_new AS new_report
 WHERE new_report.ts_end > new_report.ts_start
 GROUP BY 2
)
ORDER BY 2, 1

-- COMMAND ----------


SELECT num_rows, COUNT(*)
FROM (
  SELECT tvid, ts_start, ts_end, ip, COUNT(*) AS num_rows
  FROM dev.mohit_gangwani.content_with_null_adelaide_new
  WHERE ts_end > ts_start
  GROUP BY 1, 2, 3, 4
)
GROUP BY 1

-- COMMAND ----------


SELECT a.*, b.num_rows
FROM dev.mohit_gangwani.content_with_null_adelaide_new a
JOIN (
  SELECT tvid, ts_start, ts_end, ip, COUNT(*) AS num_rows
  FROM dev.mohit_gangwani.content_with_null_adelaide_new
  WHERE ts_end > ts_start
  GROUP BY 1, 2, 3, 4
) b
ON a.tvid = b.tvid
AND a.ts_start = b.ts_start
AND a.ts_end = b.ts_end
AND a.ip = b.ip
AND b.num_rows > 1
ORDER BY a.tvid, a.ts_start
LIMIT 100

-- COMMAND ----------

SELECT *
FROM dev.mohit_gangwani.content_with_null_adelaide_new
LIMIT 100

-- COMMAND ----------

SELECT DATE(exs_report.ts_start) AS report_date
, CASE WHEN NVL(exs_report.input_category, '') = NVL(new_report.input_category, '') THEN 'Match'
       ELSE 'No Match'
  END AS input_category_match
, CASE WHEN NVL(exs_report.input_device, '') = NVL(new_report.input_device, '') THEN 'Match'
       ELSE 'No Match'
  END AS input_device_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs
FROM dev.mohit_gangwani.content_with_null_adelaide_existing AS exs_report
JOIN dev.mohit_gangwani.content_with_null_adelaide_new AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
GROUP BY 1,2,3
ORDER BY 1,2,3

-- COMMAND ----------

SELECT DATE(exs_report.ts_start) AS report_date
, CASE WHEN NVL(exs_report.zipcode, '') = NVL(new_report.zipcode, '') THEN 'Match'
       ELSE 'No Match'
  END AS zipcode_match
, CASE WHEN NVL(exs_report.dma, '') = NVL(new_report.dma, '') THEN 'Match'
       ELSE 'No Match'
  END AS dma_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs
FROM dev.mohit_gangwani.content_with_null_adelaide_existing AS exs_report
JOIN dev.mohit_gangwani.content_with_null_adelaide_new AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.ts_end > exs_report.ts_start
 AND new_report.ts_end > new_report.ts_start
GROUP BY 1,2,3
ORDER BY 1,2,3

-- COMMAND ----------

SELECT DATE(exs_report.ts_start) AS report_date
, CASE WHEN exs_report.air_date IS NOT NULL THEN
           CASE WHEN exs_report.air_date = new_report.air_date THEN '1 - Match'
                       WHEN exs_report.air_date != new_report.air_date THEN '3 - No Match'
                       WHEN new_report.air_date IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.air_date IS NULL AND new_report.air_date IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS airdate_match
, CASE WHEN exs_report.episode_id IS NOT NULL THEN
           CASE WHEN exs_report.episode_id = new_report.episode_id THEN '1 - Match'
                       WHEN exs_report.episode_id != new_report.episode_id THEN '3 - No Match'
                       WHEN new_report.episode_id IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.episode_id IS NULL AND new_report.episode_id IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS episode_id_match
, CASE WHEN exs_report.channel_callsign IS NOT NULL THEN
           CASE WHEN exs_report.channel_callsign = new_report.channel_callsign THEN '1 - Match'
                       WHEN exs_report.channel_callsign != new_report.channel_callsign THEN '3 - No Match'
                       WHEN new_report.channel_callsign IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.channel_callsign IS NULL AND new_report.channel_callsign IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS channel_callsign_match
, CASE WHEN exs_report.live IS NOT NULL THEN
           CASE WHEN exs_report.live = new_report.live THEN '1 - Match'
                       WHEN exs_report.live != new_report.live THEN '3 - No Match'
                       WHEN new_report.live IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.live IS NULL AND new_report.live IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS live_match
, CASE WHEN exs_report.show_title  IS NOT NULL THEN
           CASE WHEN exs_report.show_title = new_report.show_title THEN '1 - Match'
                       WHEN exs_report.show_title != new_report.show_title THEN '3 - No Match'
                       WHEN new_report.show_title IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.show_title IS NULL AND new_report.show_title IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS show_title_match
, CASE WHEN exs_report.channel_affiliate IS NOT NULL THEN
           CASE WHEN exs_report.channel_affiliate = new_report.channel_affiliate THEN '1 - Match'
                       WHEN exs_report.channel_affiliate != new_report.channel_affiliate THEN '3 - No Match'
                       WHEN new_report.channel_affiliate IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.channel_affiliate IS NULL AND new_report.channel_affiliate IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS channel_affiliate_match
, COUNT(*)*1.0 AS session_count
FROM dev.mohit_gangwani.content_with_null_adelaide_existing AS exs_report
JOIN dev.mohit_gangwani.content_with_null_adelaide_new AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.ts_end > exs_report.ts_start
 AND new_report.ts_end > new_report.ts_start
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1,2,3,4,5,6,7

-- COMMAND ----------

WITH bad_sessions AS (
  SELECT exs_report.tvid, exs_report.ts_start, exs_report.ts_end, exs_report.ip
  FROM dev.mohit_gangwani.content_with_null_adelaide_existing AS exs_report
  JOIN dev.mohit_gangwani.content_with_null_adelaide_new AS new_report
    ON exs_report.tvid = new_report.tvid
   AND exs_report.ts_start = new_report.ts_start
   AND exs_report.ts_end = new_report.ts_end
   AND exs_report.ip <=> new_report.ip
   AND COALESCE(exs_report.channel_callsign, 'x') != COALESCE(new_report.channel_callsign, 'x')
  WHERE exs_report.ts_end > exs_report.ts_start
    AND new_report.ts_end > new_report.ts_start
  GROUP BY 1,2,3,4
)
SELECT exs_report.*, 'Existing' AS report_type
FROM dev.mohit_gangwani.content_with_null_adelaide_existing AS exs_report
JOIN bad_sessions bs
  ON bs.tvid = exs_report.tvid
 AND bs.ts_start = exs_report.ts_start
 AND bs.ts_end = exs_report.ts_end
 AND bs.ip <=> exs_report.ip
UNION
(SELECT new_report.*, 'New' AS report_type
FROM dev.mohit_gangwani.content_with_null_adelaide_new AS new_report
JOIN bad_sessions bs
  ON bs.tvid = new_report.tvid
 AND bs.ts_start = new_report.ts_start
 AND bs.ts_end = new_report.ts_end
 AND bs.ip <=> new_report.ip)
ORDER BY tvid, ts_start, report_type
LIMIT 1000

-- COMMAND ----------

SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Adimpact'
FROM dev.mohit_gangwani.content_adimpact_new
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Adelaide'
FROM dev.mohit_gangwani.content_with_null_adelaide_new
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Cognet'
FROM dev.mohit_gangwani.content_with_null_cognet_new
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Altice'
FROM dev.mohit_gangwani.content_with_null_altice_new
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date_tms IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live_tms = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Discovery'
FROM dev.mohit_gangwani.content_discovery_new
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Nielsen'
FROM dev.mohit_gangwani.content_with_null_nielsen_new
WHERE ts_end > ts_start


-- COMMAND ----------

SELECT * FROM detection.inscape_station_map WHERE inscape_call_sign = 'FILE'

-- COMMAND ----------

SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Adimpact'
FROM dev.mohit_gangwani.content_adimpact_existing
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Adelaide'
FROM dev.mohit_gangwani.content_with_null_adelaide_existing
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Cognet'
FROM dev.mohit_gangwani.content_with_null_cognet_existing
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Altice'
FROM dev.mohit_gangwani.content_with_null_altice_existing
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date_tms IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live_tms = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Discovery'
FROM dev.mohit_gangwani.content_discovery_existing
WHERE ts_end > ts_start
UNION
SELECT COUNT(*) AS total_sessions
, SUM(CASE WHEN air_date IS NULL THEN 1 ELSE 0 END) AS null_session_count
, SUM(CASE WHEN input_category = 'APPS' THEN 1 ELSE 0 END) AS app_session_count
, SUM(CASE WHEN live = 't' THEN 1 ELSE 0 END) AS app_session_count
, 'Nielsen'
FROM dev.mohit_gangwani.content_with_null_nielsen_existing
WHERE ts_end > ts_start


-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_adelaide_mm_sessions;
CREATE TABLE dev.mohit_gangwani.content_with_null_adelaide_mm_sessions AS
WITH bad_sessions AS (
SELECT exs_report.tvid AS fk_tvid, exs_report.ts_start, exs_report.ts_end
  FROM dev.mohit_gangwani.content_with_null_adelaide_existing AS exs_report
  JOIN dev.mohit_gangwani.content_with_null_adelaide_new AS new_report
    ON exs_report.tvid = new_report.tvid
   AND exs_report.ts_start = new_report.ts_start
   AND exs_report.ts_end = new_report.ts_end
   AND exs_report.ip <=> new_report.ip
   AND COALESCE(exs_report.channel_callsign, 'x') != COALESCE(new_report.channel_callsign, 'x')
  WHERE exs_report.ts_end > exs_report.ts_start
    AND new_report.ts_end > new_report.ts_start
  GROUP BY 1,2,3)
SELECT c.*
FROM detection.viewing_content_firehose c
JOIN detection.tv
  ON tv.tvid = c.fk_tvid
JOIN bad_sessions bs
  ON bs.fk_tvid = COALESCE(tv.long_tvid, tv.vizio_tvid)
 AND bs.ts_end = c.session_end
 AND bs.ts_start = c.session_start
WHERE c.session_start >= '2024-12-15T04:00:00'::timestamp
    AND c.session_start < '2024-12-15T06:00:00'::timestamp;

-- COMMAND ----------

SELECT COUNT(*) FROM dev.mohit_gangwani.content_with_null_adelaide_mm_sessions

-- COMMAND ----------

SELECT a.*
FROM dev.mohit_gangwani.cure_viewing_content_first_iteration a
JOIN dev.mohit_gangwani.content_with_null_adelaide_mm_sessions b
  ON a.fk_tvid = b.fk_tvid
 AND a.session_start = b.session_start
 AND a.session_end = b.session_end
 AND a.client_id_not_null LIKE '%|adelaide|%'
 LIMIT 100

-- COMMAND ----------

SELECT a.client_id_not_null, COUNT(*)
FROM dev.mohit_gangwani.cure_viewing_content_first_iteration a
JOIN dev.mohit_gangwani.content_with_null_adelaide_mm_sessions b
  ON a.fk_tvid = b.fk_tvid
 AND a.session_start = b.session_start
 AND a.session_end = b.session_end
GROUP BY 1

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_adelaide_trouble_shooting;
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_with_null_adelaide_trouble_shooting (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.content_with_null_adelaide_trouble_shooting (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
WITH activity_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_activity_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = 'adelaide'
    WHERE override.app_name IS NULL
)
, viewing_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = 'adelaide'
    WHERE override.app_name IS NULL
)
, station_distribution_blacklist AS (
    WITH agg AS (
        SELECT vendor_station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
        FROM prod.detection.station_distribution_obfuscation_overwrite
        GROUP BY 1, 2)
    SELECT vendor_station_id AS station_id, vendor_name
    FROM agg
    WHERE cl_list NOT ILIKE '%adelaide%'
)
, inscape_station_map_dedupe AS (
    SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
    FROM (
        SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
        FROM detection.inscape_station_map) ism
    WHERE ism.rn = 1
)
SELECT /*+ BROADCAST(cid) */  DISTINCT
    COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
    '', 
    NULLIF(location.zipcode, ''), 
    REPLACE(dma.dma_name, ',', ''), 
    NULLIF(CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') OR 'TIVO' != 'TMS' THEN 'Chan b' 
            ELSE vizio_program.program_tms_id
        END
    ELSE
        CASE WHEN acrb.app_name IS NOT NULL THEN 'ACR B'
            WHEN (cl.client_id is not null) THEN 'Client ID'
        ELSE
            CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN 'Nielsen BLK'
                WHEN station_blacklist.station_id IS NOT NULL THEN 'STTN BLCKLIST'
                WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
            ELSE show.database_key
        END
    END
    END,''), 
    CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN 'Chan b'  
            WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
                THEN vizio_program.series_aggregate_title
            ELSE vizio_program.title END
    WHEN acrb.app_name IS NOT NULL THEN 'ACR B'
    WHEN (cl.client_id is not null) THEN 'Client ID'
    WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN 'Nielsen BLK'
    WHEN station_blacklist.station_id IS NOT NULL THEN 'STTN BLCKLIST'
    ELSE CASE WHEN c.file_ingested THEN 'File Ingested' 
        WHEN 'adelaide' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
        ELSE REPLACE(show.title, ',', '') END 
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(c.airdate, c.tms_airdate)
        WHEN (cl.client_id is not null) THEN NULL
        WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
        WHEN 'adelaide' != 'nielsen' THEN COALESCE(c.airdate, c.tms_airdate)
        WHEN 'adelaide' = 'nielsen' THEN c.airdate
        ELSE NULL
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN 'ACR B'
        WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
        WHEN (cl.client_id is not null) THEN 'Client ID'
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN 'Nielsen B'
        WHEN station_obfs.station_id IS NOT NULL THEN 'Station OBfs'
        WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
        WHEN 'adelaide' != 'nielsen' THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
        WHEN 'adelaide' = 'nielsen' AND c.airdate IS NOT NULL THEN map.inscape_call_sign
        ELSE 'Other'
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL 
        WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
        WHEN (cl.client_id is not null) THEN NUll
        WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
        WHEN 'adelaide' != 'nielsen' THEN LEAST(c.media_time_start, c.runtime)
        WHEN 'adelaide' = 'nielsen' AND c.airdate IS NULL THEN NULL
        ELSE LEAST(c.media_time_start, c.runtime)
    END, 
    c.session_start, 
    c.session_end, 
    CASE WHEN c.vizio_epg_station IS NOT NULL
    THEN CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN 'OBFUSCATED' 
        ELSE vizio_station.name END
    WHEN acrb.app_name IS NOT NULL THEN NULL
    WHEN (cl.client_id IS NOT NULL) THEN NULL
    ELSE
        CASE 
            WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
            WHEN station_obfs.station_id IS NOT NULL THEN NULL
            WHEN 'adelaide' = 'nielsen' AND c.airdate IS NULL THEN NULL
            WHEN c.fk_station_id IS NOT NULL THEN
                CASE WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
                     WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
                           OR LOWER(station.station_affil) LIKE '%independent%'
                           OR LOWER(station.station_affil) LIKE '%low power%')
                        THEN station.station_affil ELSE NULL END
            WHEN c.fk_station_id IS NULL AND c.tms_station_id IS NOT NULL THEN
                CASE WHEN (backup_station.inscape_station_name IS NOT NULL) THEN backup_station.inscape_station_name
                     WHEN (LOWER(backup_station.station_affil) LIKE '%affiliate%'
                          OR LOWER(backup_station.station_affil) LIKE '%independent%'
                          OR LOWER(backup_station.station_affil) LIKE '%low power%')
                        THEN backup_station.station_affil ELSE NULL END END
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN 't'
        WHEN (cl.client_id IS NOT NULL) THEN NULL
        WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL
        WHEN 'adelaide' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
        WHEN 'adelaide' = 'nielsen' AND c.airdate IS NULL THEN NULL
    ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
    END, 
    ip.ip_address, 
    tvis.category, 
    tvis.input_device, 
    CASE WHEN UPPER(tvis.category) = 'APPS' THEN
      CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
         WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
          WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
          WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
         WHEN lower(tis.app_name) = 'unknown' THEN NULL
          ELSE tis.app_name END
    WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
        THEN 'vMVPD'
    END
FROM
    dev.mohit_gangwani.content_with_null_adelaide_mm_sessions AS c
    JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    JOIN prod.detection.tv AS tv ON c.session_start >= '2024-12-15T04:00:00'::timestamp
        AND c.session_start < '2024-12-15T06:00:00'::timestamp
        AND c.fk_tvid = tv.tvid 
        AND tv.oem = 'VIZIO'   
    JOIN prod.detection.tv_settings AS tv_settings
        ON c.session_start >= tv_settings.create_timestamp
        AND c.session_start < tv_settings.next_create_timestamp
        AND tv_settings.create_timestamp <= '2024-12-15T06:00:00'::timestamp
        AND tv_settings.next_create_timestamp >= '2024-12-15T04:00:00'::timestamp
        AND c.fk_tvid = tv_settings.fk_tvid
    JOIN prod.detection.settings AS settings
        ON tv_settings.fk_settings_id = settings.settings_id
        AND UPPER(settings.country_name) = 'USA'
    JOIN prod.detection.tv_populations AS u
        ON c.fk_tvid = u.fk_tvid 
    JOIN prod.detection.populations AS pop
        ON u.fk_population_id = pop.population_id 
        AND pop.population_name = 'opted_in'
    JOIN prod.detection.location AS location
        ON c.fk_location_id = location.location_id
        AND UPPER(location.country_code) = 'US'
    LEFT OUTER JOIN prod.detection.dma AS dma
        ON c.fk_dma_id = dma.dma_id
    LEFT OUTER JOIN prod.detection.input_source inps 
        ON c.fk_input_source_id = inps.input_source_id
    LEFT OUTER JOIN inscape_station_map_dedupe AS map
        ON map.mapped_vendor_station_id = c.fk_station_id
        AND map.mapped_vendor = 'TIVO'
    LEFT OUTER JOIN prod.detection.epg_station AS station
        ON station.station_id = c.fk_station_id
        AND station.vendor_name = 'TIVO'
    LEFT OUTER JOIN prod.detection.epg_show AS show
        ON show.show_id = c.fk_show_id
        AND show.vendor_name = 'TIVO'
    LEFT OUTER JOIN prod.detection.epg_show AS backup_show
        ON backup_show.show_id = c.tms_show_id
        AND c.fk_show_id IS NULL
       AND backup_show.vendor_name = 'TMS'
       AND 'adelaide' != 'nielsen'
    LEFT OUTER JOIN prod.detection.epg_station AS backup_station
        ON backup_station.station_id = c.tms_station_id
        AND c.fk_station_id IS NULL
        AND backup_station.vendor_name = 'TMS'
        AND 'adelaide' != 'nielsen'
    LEFT OUTER JOIN inscape_station_map_dedupe AS backup_map
        ON backup_map.mapped_vendor_station_id = c.tms_station_id
        AND c.fk_station_id IS NULL
        AND backup_map.mapped_vendor = 'TMS'
        AND 'adelaide' != 'nielsen'
    LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
        ON c.fk_station_id = station_blacklist.station_id
        AND station_blacklist.vendor_name = map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
        ON c.fk_station_id = station_obfs.vendor_station_id
        AND station_obfs.vendor_name = map.mapped_vendor  
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
        ON CAST(c.vizio_epg_station AS STRING) = CAST(vizio_station.station_id AS STRING)
    LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
        ON CAST(c.vizio_epg_program AS STRING) = CAST(vizio_program.program_aggregate_id AS STRING)
        AND c.vizio_epg_program != '0'
    JOIN prod.detection.content_ids_firehose AS cid
        ON cid.content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
        ON m.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cl
        ON m.fk_client_id = cl.client_id
        AND cl.client_name <> 'adelaide'
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
        ON md.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cli
        ON md.fk_client_id = cli.client_id
        AND cli.client_name = 'adelaide'
    JOIN prod.detection.tv_input_stats_firehose  tvis 
        ON c.session_start >= tvis.create_timestamp
        AND c.session_start < tvis.next_create_timestamp
        AND tvis.create_timestamp <= '2024-12-15T06:00:00'::timestamp
        AND tvis.next_create_timestamp >= '2024-12-15T04:00:00'::timestamp
        AND  c.fk_tvid = tvis.fk_tvid
        AND  c.fk_input_source_id = tvis.fk_input_source_id
    LEFT OUTER JOIN prod.detection.tv_inputsource tis
        ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
        AND c.session_start < (tis.next_create_timestamp::double)::timestamp
        AND c.fk_tvid = tis.fk_tvid
        AND c.fk_input_source_id = tis.fk_input_source_id
        AND tis.create_timestamp <= ('2024-12-15T06:00:00'::timestamp::double)::timestamp 
        AND tis.next_create_timestamp >= ('2024-12-15T04:00:00'::timestamp::double)::timestamp 
    LEFT OUTER JOIN activity_obfuscation AS appb
        ON tis.app_name = appb.app_name
    LEFT OUTER JOIN viewing_obfuscation AS acrb 
        ON tis.app_name = acrb.app_name 
    LEFT OUTER JOIN 
        prod.detection.free_channels_distribution_blacklist chanb 
        ON vizio_station.name = chanb.channel_name
    LEFT OUTER JOIN
        prod.detection.tv_ip_address AS ip
        ON c.session_start >= ip.create_timestamp
        AND c.session_start < ip.next_create_timestamp
        AND ip.create_timestamp <= '2024-12-15T06:00:00'::timestamp
        AND ip.next_create_timestamp >= '2024-12-15T04:00:00'::timestamp
        AND tv.tvid = ip.fk_tvid
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
        ON nielsen_blacklist.station_id = COALESCE(map.inscape_station_id, backup_map.inscape_station_id)
        AND c.session_start >= nielsen_blacklist.blacklist_start 
        AND c.session_start < nielsen_blacklist.blacklist_end
        AND 'adelaide' != 'nielsen'  
    LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
        ON map.inscape_station_id = rep_local.station_id 
        AND COALESCE(c.airdate, c.tms_airdate) = rep_local.airdate
        AND COALESCE(c.fk_show_id, c.tms_show_id) = rep_local.fk_show_id
        AND c.fk_dma_id = rep_local.dma_id
        AND 'adelaide' != 'nielsen'
    LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
        ON map.inscape_station_id = rep_nyc_nat.station_id 
        AND COALESCE(c.airdate, c.tms_airdate) = rep_nyc_nat.airdate
        AND COALESCE(c.fk_show_id, c.tms_show_id) = rep_nyc_nat.fk_show_id 
        AND 'adelaide' != 'nielsen'
    WHERE
        c.session_start >= '2024-12-15T04:00:00'::timestamp
    AND c.session_start < '2024-12-15T06:00:00'::timestamp
    AND CASE c.file_ingested
        WHEN true THEN
            CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT_PART(cid.content_cid, '_', 3), '') IS NULL
            WHEN true THEN SPLIT(cid.content_cid, '_')[1]
            ELSE NULL
            END
        ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport')
        END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)

-- COMMAND ----------

SELECT channel_callsign,
    COUNT(*)
    FROM dev.mohit_gangwani.content_with_null_adelaide_trouble_shooting
    GROUP BY 1

-- COMMAND ----------


