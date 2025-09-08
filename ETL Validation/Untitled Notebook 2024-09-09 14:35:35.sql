-- Databricks notebook source
SELECT DATE_TRUNC('HOUR', vc.session_start), COUNT(*)
FROM dev.samples.viewing_content_firehose_sample AS vc
GROUP BY 1

-- COMMAND ----------

(SELECT DATE_TRUNC('DAY', vc.session_start), 'SAMPLE TABLE' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM dev.samples.viewing_content_firehose_sample AS vc
WHERE fk_zoo_id = 17
GROUP BY 1)
UNION
(SELECT DATE_TRUNC('DAY', vc.session_start), 'Prod TABLE' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM (
  SELECT *
	     , ROW_NUMBER() OVER (PARTITION BY fk_tvid, session_start, session_end, fk_content_id ORDER BY fk_tvid, session_start, session_end, fk_content_id) AS ms
	FROM
		prod.detection.viewing_content_firehose
		WHERE session_start >= '2024-03-26T08:00:00'::TIMESTAMP
      AND fk_zoo_id = 17
      AND session_start < '2024-09-12T08:00:00'::TIMESTAMP
      AND abs(mod(hash(fk_tvid, "md5"),100)) = 1) AS vc
WHERE ms = 1
GROUP BY 1)

-- COMMAND ----------

(SELECT DATE_TRUNC('HOUR', vc.session_start), 'SAMPLE TABLE' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM dev.samples.viewing_content_firehose_sample AS vc
WHERE fk_zoo_id = 17
  AND fk_content_id != 3468026
GROUP BY 1)
UNION
(SELECT DATE_TRUNC('HOUR', vc.session_start), 'Prod TABLE' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM prod.detection.viewing_content_firehose AS vc
WHERE session_start >= '2024-09-08T08:00:00'::TIMESTAMP
  AND fk_zoo_id = 17
  AND session_start < '2024-09-09T08:00:00'::TIMESTAMP
  AND abs(mod(hash(fk_tvid, "md5"),100)) = 1
  AND fk_content_id != 3468026
GROUP BY 1)

-- COMMAND ----------

SELECT COUNT(*)
FROM dev.samples.viewing_content_firehose_sample vc
JOIN prod.detection.content_id_external_firehose cief
  ON cief.fk_content_id = vc.fk_content_id
WHERE session_start >= CURRENT_DATE -1
-- AND tms_show_id IS NOT NULL
LIMIT 100

-- COMMAND ----------

SELECT * FROM prod.detection.content_id_external_firehose
WHERE fk_content_id = 2454664

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.viewing_content_input_stats_firehose_sample;
CREATE TABLE dev.mohit_gangwani.viewing_content_input_stats_firehose_sample AS
SELECT 
	vc.fk_tvid,
	vc.fk_show_id,
	vc.fk_station_id,
	vc.airdate,
	vc.session_start,
	vc.session_end,
	vc.session_duration,
	vc.media_time_start,
	vc.media_time_end,
	vc.runtime,
	vc.fk_frame_id,
	vc.fk_dma_id,
	vc.fk_zoo_id,
	vc.fk_content_id,
	vc.fk_input_source_id,
	vc.fk_location_id,
	vc.fk_schedule_id,
	vc.timezone,
	vc.confidence,
	vc.ump_id,
	vc.is_live,
	vc.batch_size,
	vc.file_ingested,
    vc.created_at,
	vc.audio_contri,
	vc.video_contri,
	vc.viewing_type,
	vc.local_session_start,
	vc.local_session_end,
	vc.vizio_epg_airing,
	vc.vizio_epg_station,
	vc.vizio_epg_program,
	vc.tuner_channel_id,
	vc.tuner_schedule_id,
	vc.tuner_program_id,
    vc.tms_station_id,
    vc.tms_show_id,
	vc.tms_airdate,
    vc.tms_schedule_id,
    vc.tms_tuner_channel_id,
	vc.tms_tuner_schedule_id,
	vc.tms_tuner_program_id,
	sh.title,
	sh.epi_title,
	sh.genre,
	sh.database_key,
    st.station_name ,
    st.station_affil,
    tvis.input_number,
    tvis.category,
    tvis.subcategory,
    tvis.detection_rate,
    tvis.percent_hd_frames,
    tvis.percent_sd_frames,
    tvis.percent_other_frames,
    tvis.total_duration,
    tvis.create_timestamp,
    tvis.next_create_timestamp,
    tvis.percent_local_frames,
    tvis.input_device,
    tvis.input_device_source,
    tvis.input_device_type,
    vc.partition_key
FROM dev.samples.viewing_content_firehose_sample vc
LEFT OUTER JOIN prod.detection.epg_station AS st 
	ON st.vendor_name = 'TMS'
	AND st.station_id = vc.tms_station_id
LEFT OUTER JOIN prod.detection.epg_show AS sh 
	ON sh.vendor_name = 'TMS'
    AND vc.tms_show_id = sh.show_id
LEFT JOIN prod.detection.tv_input_stats_firehose tvis 
	ON date_trunc('week',vc.session_start) = tvis.create_timestamp
	AND tvis.fk_tvid = vc.fk_tvid
	AND tvis.fk_input_source_id = vc.fk_input_source_id
  -- AND tvis.total_duration > 0
WHERE vc.session_start >= '2024-07-08T08:00:00'::TIMESTAMP
  AND vc.session_start < '2024-07-09T08:00:00'::TIMESTAMP

-- COMMAND ----------

(SELECT DATE(session_start) AS session_day, 'New Sample Table' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM dev.samples.viewing_content_input_stats_firehose_sample AS vc
-- WHERE session_start >= '2024-09-08T08:00:00'::TIMESTAMP
--   AND fk_zoo_id = 17
--   AND session_start < '2024-09-09T08:00:00'::TIMESTAMP
  -- AND tms_station_id IS NOT NULL AND fk_station_id IS NULL
GROUP BY 1)
UNION
(SELECT DATE(session_start) AS session_day, 'VC Sample Table' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM dev.samples.viewing_content_firehose_sample AS vc
-- WHERE session_start >= '2024-09-08T08:00:00'::TIMESTAMP
--   AND fk_zoo_id = 17
--   AND session_start < '2024-09-09T08:00:00'::TIMESTAMP
  -- AND tms_station_id IS NOT NULL AND fk_station_id IS NULL
GROUP BY 1)

-- COMMAND ----------

(SELECT DATE(session_start) AS session_day, 'New Sample Table' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM dev.samples.viewing_content_input_stats_firehose_sample AS vc
WHERE fk_station_id IS NOT NULL
GROUP BY 1)
UNION
(SELECT DATE(session_start) AS session_day, 'Self Created Table' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM dev.samples.viewing_content_firehose_sample AS vc
WHERE fk_station_id IS NOT NULL
  -- AND tms_station_id IS NOT NULL AND fk_station_id IS NULL
GROUP BY 1)

-- COMMAND ----------

(SELECT input_device, 'New Sample Table' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM dev.samples.viewing_content_input_stats_firehose_sample AS vc
WHERE session_start >= '2024-09-08T08:00:00'::TIMESTAMP
  AND fk_zoo_id = 17
  AND session_start < '2024-09-09T08:00:00'::TIMESTAMP
  -- AND tms_station_id IS NOT NULL AND fk_station_id IS NULL
GROUP BY 1)
UNION
(SELECT input_device, 'Self Created Table' AS table_name, COUNT(*) AS session_count, COUNT(DISTINCT fk_tvid) AS total_tvs, SUM(session_duration)/3600.0 AS ttl_duration
FROM dev.mohit_gangwani.viewing_content_input_stats_firehose_sample AS vc
WHERE session_start >= '2024-09-08T08:00:00'::TIMESTAMP
  AND fk_zoo_id = 17
  AND session_start < '2024-09-09T08:00:00'::TIMESTAMP
  -- AND tms_station_id IS NOT NULL AND fk_station_id IS NULL
GROUP BY 1)

-- COMMAND ----------


