# Databricks notebook source
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")

start_time = dbutils.widgets.get("start_date")
end_time = dbutils.widgets.get("end_date")

print(f"Running Content Hourly Report from {start_time} to {end_time}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM prod.detection.viewing_content_firehose
# MAGIC WHERE fk_tvid = 133735360
# MAGIC AND session_start = '2024-07-03T02:03:40'
# MAGIC AND partition_key = '2024-07-03'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT COUNT(*), SUM(gc.ttl_sessions)
# MAGIC SELECT vc.*
# MAGIC FROM dev.mohit_gangwani.content_with_null_cognet_existing_table_backfill_test AS vc
# MAGIC JOIN (
# MAGIC   SELECT tvid, ts_start, ts_end, ip, COUNT(*) AS ttl_sessions
# MAGIC   FROM dev.mohit_gangwani.content_with_null_cognet_existing_table_backfill_test AS new_report
# MAGIC   -- FROM dev.mohit_gangwani.content_with_null_cognet_existing_table_backfill_test AS existing_table
# MAGIC   GROUP BY 1, 2, 3, 4
# MAGIC ) AS gc
# MAGIC ON vc.tvid = gc.tvid AND gc.ts_start = vc.ts_start AND gc.ip = vc.ip AND gc.ts_end = vc.ts_end
# MAGIC WHERE gc.ttl_sessions > 1
# MAGIC ORDER BY vc.tvid, vc.ts_start
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_cognet_existing_table_backfill_test;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_with_null_cognet_existing_table_backfill_test (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC INSERT INTO dev.mohit_gangwani.content_with_null_cognet_existing_table_backfill_test (
# MAGIC     tvid, 
# MAGIC     hash, 
# MAGIC     zipcode, 
# MAGIC     dma, 
# MAGIC     episode_id, 
# MAGIC     show_title, 
# MAGIC     air_date, 
# MAGIC     channel_callsign, 
# MAGIC     mt_start, 
# MAGIC     ts_start, 
# MAGIC     ts_end, 
# MAGIC     channel_affiliate, 
# MAGIC     live, 
# MAGIC     ip, 
# MAGIC     input_category, 
# MAGIC     input_device, 
# MAGIC     app_service
# MAGIC )
# MAGIC WITH activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'cognet'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'cognet'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%cognet%'
# MAGIC )
# MAGIC SELECT /*+ BROADCAST(cid) */  DISTINCT
# MAGIC     COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
# MAGIC     '', 
# MAGIC     NULLIF(location.zipcode, ''), 
# MAGIC     REPLACE(dma.dma_name, ',', ''), 
# MAGIC     NULLIF(CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TIVO' != 'TMS' THEN NULL 
# MAGIC             ELSE vizio_program.program_tms_id
# MAGIC         END
# MAGIC     ELSE
# MAGIC         CASE WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC             WHEN (cl.client_id is not null) THEN NULL
# MAGIC         ELSE
# MAGIC             CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC                 WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC                 WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key
# MAGIC         END
# MAGIC     END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC             ELSE vizio_program.title END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id is not null) THEN NULL
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'cognet' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
# MAGIC         WHEN (cl.client_id is not null) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC         WHEN 'cognet' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
# MAGIC         ELSE schedule.airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'cognet' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         ELSE map.inscape_call_sign 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'cognet' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         ELSE LEAST(c.media_time_start, schedule.duration) 
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL
# MAGIC     THEN CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED' 
# MAGIC         ELSE vizio_station.name END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL 
# MAGIC             WHEN 'cognet' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC             WHEN 'cognet' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC             WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC             WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC                 OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC                 OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC             THEN station.station_affil ELSE NULL END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC         WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL 
# MAGIC         WHEN 'cognet' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN
# MAGIC       CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC          WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC           WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC           WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC          WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC           ELSE tis.app_name END
# MAGIC     WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
# MAGIC         THEN 'vMVPD'
# MAGIC     END
# MAGIC FROM
# MAGIC     prod.detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-07-11T00:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC     JOIN prod.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     JOIN prod.detection.tv_populations AS u
# MAGIC         ON c.fk_tvid = u.fk_tvid 
# MAGIC     JOIN prod.detection.populations AS pop
# MAGIC         ON u.fk_population_id = pop.population_id 
# MAGIC         AND pop.population_name = 'opted_in'
# MAGIC     JOIN prod.detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     LEFT OUTER JOIN prod.detection.dma AS dma
# MAGIC         ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.input_source inps 
# MAGIC         ON c.fk_input_source_id = inps.input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.inscape_station_map AS map
# MAGIC         ON map.inscape_station_id = c.fk_station_id
# MAGIC         AND map.mapped_vendor = 'TIVO'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS station
# MAGIC         ON map.mapped_vendor_station_id = station.station_id 
# MAGIC         AND station.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
# MAGIC         ON c.fk_station_id = station_blacklist.station_id
# MAGIC         AND station_blacklist.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
# MAGIC         ON c.fk_station_id = station_obfs.station_id
# MAGIC         AND station_obfs.vendor_name = map.mapped_vendor  
# MAGIC     LEFT OUTER JOIN prod.detection.epg_schedule_latest AS schedule
# MAGIC         ON map.mapped_vendor_station_id = schedule.fk_station_id
# MAGIC         AND schedule.vendor_name = map.mapped_vendor
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) >= schedule.airdate  
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) <  schedule.airdate_end
# MAGIC         AND schedule.airdate >= '2024-07-01T00:00:00'::timestamp - interval '60' day
# MAGIC         AND schedule.airdate <= '2024-07-12T00:00:00'::timestamp
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS show
# MAGIC         ON schedule.fk_show_id = show.show_id
# MAGIC         AND show.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = c.fk_show_id 
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
# MAGIC         ON c.vizio_epg_station = vizio_station.station_id
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
# MAGIC         ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
# MAGIC         AND c.vizio_epg_program != '0'
# MAGIC     JOIN prod.detection.content_ids_firehose AS cid
# MAGIC         ON cid.content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
# MAGIC         ON m.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC         AND cl.client_name <> 'cognet'
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'cognet'
# MAGIC     JOIN prod.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-07-11T00:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-07-01T00:00:00'::timestamp::double)::timestamp 
# MAGIC     LEFT OUTER JOIN activity_obfuscation AS appb
# MAGIC         ON tis.app_name = appb.app_name
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb 
# MAGIC         ON tis.app_name = acrb.app_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist chanb 
# MAGIC         ON vizio_station.name = chanb.channel_name
# MAGIC     LEFT OUTER JOIN
# MAGIC         prod.detection.tv_ip_address AS ip
# MAGIC         ON c.session_start >= ip.create_timestamp
# MAGIC         AND c.session_start < ip.next_create_timestamp
# MAGIC         AND ip.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'cognet' != 'nielsen'  
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND c.fk_show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND c.fk_show_id  = rep_nyc_nat.fk_show_id 
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-07-01T00:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-07-11T00:00:00'::timestamp
# MAGIC     AND c.partition_key >= '2024-07-01'
# MAGIC     AND c.partition_key <= '2024-07-11'
# MAGIC     AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign);

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_cognet_new_table_backfill_test;
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_with_null_cognet_new_table_backfill_test (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer,
# MAGIC     ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.content_with_null_cognet_new_table_backfill_test (
# MAGIC     tvid, 
# MAGIC     hash, 
# MAGIC     zipcode, 
# MAGIC     dma, 
# MAGIC     episode_id, 
# MAGIC     show_title, 
# MAGIC     air_date, 
# MAGIC     channel_callsign, 
# MAGIC     mt_start, 
# MAGIC     ts_start, 
# MAGIC     ts_end, 
# MAGIC     channel_affiliate, 
# MAGIC     live, 
# MAGIC     ip, 
# MAGIC     input_category, 
# MAGIC     input_device, 
# MAGIC     app_service
# MAGIC )
# MAGIC WITH activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'cognet'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'cognet'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT vendor_station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT vendor_station_id AS station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%cognet%'
# MAGIC )
# MAGIC , inscape_station_map_dedupe AS (
# MAGIC     SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
# MAGIC     FROM (
# MAGIC         SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
# MAGIC         FROM detection.inscape_station_map) ism
# MAGIC     WHERE ism.rn = 1
# MAGIC )
# MAGIC SELECT /*+ BROADCAST(cid) */  DISTINCT
# MAGIC     COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
# MAGIC     '', 
# MAGIC     NULLIF(location.zipcode, ''), 
# MAGIC     REPLACE(dma.dma_name, ',', ''), 
# MAGIC     NULLIF(CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TIVO' != 'TMS' THEN NULL 
# MAGIC             ELSE vizio_program.program_tms_id
# MAGIC         END
# MAGIC     ELSE
# MAGIC         CASE WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC             WHEN (cl.client_id is not null) THEN NULL
# MAGIC         ELSE
# MAGIC             CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC                 WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC                 WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key
# MAGIC         END
# MAGIC     END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC             ELSE vizio_program.title END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id is not null) THEN NULL
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'nielsen' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(c.airdate, c.tms_airdate)
# MAGIC         WHEN (cl.client_id is not null) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC         WHEN 'cognet' != 'nielsen' THEN COALESCE(c.airdate, c.tms_airdate)
# MAGIC         WHEN 'cognet' = 'nielsen' THEN c.airdate
# MAGIC         ELSE NULL
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'cognet' != 'nielsen' THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC         WHEN 'cognet' = 'nielsen' AND c.airdate IS NOT NULL THEN map.inscape_call_sign
# MAGIC     END,
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN 'cognet' != 'nielsen' THEN LEAST(c.media_time_start, c.runtime)
# MAGIC         WHEN 'cognet' = 'nielsen' AND c.airdate IS NULL THEN NULL
# MAGIC         ELSE LEAST(c.media_time_start, c.runtime)
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL
# MAGIC     THEN CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED' 
# MAGIC         ELSE vizio_station.name END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC             WHEN 'cognet' = 'nielsen' AND c.airdate IS NULL THEN NULL
# MAGIC             WHEN c.fk_station_id IS NOT NULL THEN
# MAGIC                 CASE WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC                      WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC                            OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC                            OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC                         THEN station.station_affil ELSE NULL END
# MAGIC             WHEN c.fk_station_id IS NULL AND c.tms_station_id IS NOT NULL THEN
# MAGIC                 CASE WHEN (backup_station.inscape_station_name IS NOT NULL) THEN backup_station.inscape_station_name
# MAGIC                      WHEN (LOWER(backup_station.station_affil) LIKE '%affiliate%'
# MAGIC                           OR LOWER(backup_station.station_affil) LIKE '%independent%'
# MAGIC                           OR LOWER(backup_station.station_affil) LIKE '%low power%')
# MAGIC                         THEN backup_station.station_affil ELSE NULL END END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC         WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
# MAGIC         WHEN 'cognet' = 'nielsen' AND c.airdate IS NULL THEN NULL
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN
# MAGIC       CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC          WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC           WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC           WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC          WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC           ELSE tis.app_name END
# MAGIC     WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
# MAGIC         THEN 'vMVPD'
# MAGIC     END
# MAGIC FROM
# MAGIC     dev_temp.detection.viewing_content_firehose_historic_0731 AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-07-11T00:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC     JOIN prod.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     JOIN prod.detection.tv_populations AS u
# MAGIC         ON c.fk_tvid = u.fk_tvid 
# MAGIC     JOIN prod.detection.populations AS pop
# MAGIC         ON u.fk_population_id = pop.population_id 
# MAGIC         AND pop.population_name = 'opted_in'
# MAGIC     JOIN prod.detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     LEFT OUTER JOIN prod.detection.dma AS dma
# MAGIC         ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.input_source inps 
# MAGIC         ON c.fk_input_source_id = inps.input_source_id
# MAGIC -- Historical change start
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS map
# MAGIC         ON map.mapped_vendor_station_id = c.fk_station_id
# MAGIC         AND map.mapped_vendor = 'TIVO'
# MAGIC     LEFT OUTER JOIN detection.epg_station AS station
# MAGIC         ON station.station_id = c.fk_station_id
# MAGIC         AND station.vendor_name = 'TIVO'
# MAGIC     LEFT OUTER JOIN detection.epg_show AS show
# MAGIC         ON show.show_id = c.fk_show_id
# MAGIC         AND show.vendor_name = 'TIVO'
# MAGIC     -- Backup values
# MAGIC     LEFT OUTER JOIN detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = c.tms_show_id
# MAGIC        AND c.fk_show_id IS NULL
# MAGIC        AND backup_show.vendor_name = 'TMS'
# MAGIC        AND 'cognet' != 'nielsen'
# MAGIC     LEFT OUTER JOIN detection.epg_station AS backup_station
# MAGIC         ON backup_station.station_id = c.tms_station_id
# MAGIC        AND c.fk_station_id IS NULL
# MAGIC        AND backup_station.vendor_name = 'TMS'
# MAGIC        AND 'cognet' != 'nielsen'
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS backup_map
# MAGIC         ON backup_map.mapped_vendor_station_id = c.tms_station_id
# MAGIC         AND c.fk_station_id IS NULL
# MAGIC         AND backup_map.mapped_vendor = 'TMS'
# MAGIC         AND 'cognet' != 'nielsen'
# MAGIC -- Historical change End
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
# MAGIC         ON c.fk_station_id = station_blacklist.station_id
# MAGIC         AND station_blacklist.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
# MAGIC         ON c.fk_station_id = station_obfs.vendor_station_id
# MAGIC         AND station_obfs.vendor_name = map.mapped_vendor  
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
# MAGIC         ON c.vizio_epg_station = vizio_station.station_id
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
# MAGIC         ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
# MAGIC         AND c.vizio_epg_program != '0'
# MAGIC     JOIN prod.detection.content_ids_firehose AS cid
# MAGIC         ON cid.content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
# MAGIC         ON m.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC         AND cl.client_name <> 'cognet'
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'cognet'
# MAGIC     JOIN prod.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-07-11T00:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-07-01T00:00:00'::timestamp::double)::timestamp 
# MAGIC     LEFT OUTER JOIN activity_obfuscation AS appb
# MAGIC         ON tis.app_name = appb.app_name
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb 
# MAGIC         ON tis.app_name = acrb.app_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist chanb 
# MAGIC         ON vizio_station.name = chanb.channel_name
# MAGIC     LEFT OUTER JOIN
# MAGIC         prod.detection.tv_ip_address AS ip
# MAGIC         ON c.session_start >= ip.create_timestamp
# MAGIC         AND c.session_start < ip.next_create_timestamp
# MAGIC         AND ip.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'cognet' != 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND c.fk_show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC         AND 'cognet' != 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND c.fk_show_id  = rep_nyc_nat.fk_show_id 
# MAGIC         AND 'cognet' != 'nielsen'
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-07-01T00:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-07-11T00:00:00'::timestamp
# MAGIC     AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_nielsen_existing_table_backfill_test;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_with_null_nielsen_existing_table_backfill_test (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC INSERT INTO dev.mohit_gangwani.content_with_null_nielsen_existing_table_backfill_test (
# MAGIC     tvid, 
# MAGIC     hash, 
# MAGIC     zipcode, 
# MAGIC     dma, 
# MAGIC     episode_id, 
# MAGIC     show_title, 
# MAGIC     air_date, 
# MAGIC     channel_callsign, 
# MAGIC     mt_start, 
# MAGIC     ts_start, 
# MAGIC     ts_end, 
# MAGIC     channel_affiliate, 
# MAGIC     live, 
# MAGIC     ip, 
# MAGIC     input_category, 
# MAGIC     input_device, 
# MAGIC     app_service
# MAGIC )
# MAGIC WITH activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'nielsen'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'nielsen'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%nielsen%'
# MAGIC )
# MAGIC SELECT /*+ BROADCAST(cid) */  DISTINCT
# MAGIC     COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
# MAGIC     '', 
# MAGIC     NULLIF(location.zipcode, ''), 
# MAGIC     REPLACE(dma.dma_name, ',', ''), 
# MAGIC     NULLIF(CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TMS' != 'TMS' THEN NULL 
# MAGIC             ELSE vizio_program.program_tms_id
# MAGIC         END
# MAGIC     ELSE
# MAGIC         CASE WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC             WHEN (cl.client_id is not null) THEN NULL
# MAGIC         ELSE
# MAGIC             CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC                 WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC                 WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key
# MAGIC         END
# MAGIC     END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC             ELSE vizio_program.title END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id is not null) THEN NULL
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'nielsen' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
# MAGIC         WHEN (cl.client_id is not null) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC         WHEN 'nielsen' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
# MAGIC         ELSE schedule.airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'nielsen' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'nielsen' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         ELSE map.inscape_call_sign 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC         WHEN 'nielsen' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'nielsen' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         ELSE LEAST(c.media_time_start, schedule.duration) 
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL
# MAGIC     THEN CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED' 
# MAGIC         ELSE vizio_station.name END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL 
# MAGIC             WHEN 'nielsen' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC             WHEN 'nielsen' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC             WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC             WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC                 OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC                 OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC             THEN station.station_affil ELSE NULL END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC         WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN 'nielsen' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL 
# MAGIC         WHEN 'nielsen' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN
# MAGIC       CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC          WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC           WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC           WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC          WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC           ELSE tis.app_name END
# MAGIC     WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
# MAGIC         THEN 'vMVPD'
# MAGIC     END
# MAGIC FROM
# MAGIC     detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-07-11T00:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC     JOIN prod.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     JOIN prod.detection.tv_populations AS u
# MAGIC         ON c.fk_tvid = u.fk_tvid 
# MAGIC     JOIN prod.detection.populations AS pop
# MAGIC         ON u.fk_population_id = pop.population_id 
# MAGIC         AND pop.population_name = 'opted_in'
# MAGIC     JOIN prod.detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     LEFT OUTER JOIN prod.detection.dma AS dma
# MAGIC         ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.input_source inps 
# MAGIC         ON c.fk_input_source_id = inps.input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.inscape_station_map AS map
# MAGIC         ON map.inscape_station_id = c.fk_station_id
# MAGIC         AND map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS station
# MAGIC         ON map.mapped_vendor_station_id = station.station_id 
# MAGIC         AND station.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
# MAGIC         ON c.fk_station_id = station_blacklist.station_id
# MAGIC         AND station_blacklist.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
# MAGIC         ON c.fk_station_id = station_obfs.station_id
# MAGIC         AND station_obfs.vendor_name = map.mapped_vendor  
# MAGIC     LEFT OUTER JOIN prod.detection.epg_schedule_latest AS schedule
# MAGIC         ON map.mapped_vendor_station_id = schedule.fk_station_id
# MAGIC         AND schedule.vendor_name = map.mapped_vendor
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) >= schedule.airdate  
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) <  schedule.airdate_end
# MAGIC         AND schedule.airdate >= '2024-07-01T00:00:00'::timestamp - interval '60' day
# MAGIC         AND schedule.airdate <= '2024-07-13T00:00:00'::timestamp
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS show
# MAGIC         ON schedule.fk_show_id = show.show_id
# MAGIC         AND show.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = c.fk_show_id 
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
# MAGIC         ON c.vizio_epg_station = vizio_station.station_id
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
# MAGIC         ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
# MAGIC         AND c.vizio_epg_program != '0'
# MAGIC     JOIN prod.detection.content_ids_firehose AS cid
# MAGIC         ON cid.content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
# MAGIC         ON m.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC         AND cl.client_name <> 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'nielsen'
# MAGIC     JOIN prod.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-07-11T00:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-07-01T00:00:00'::timestamp::double)::timestamp 
# MAGIC     LEFT OUTER JOIN activity_obfuscation AS appb
# MAGIC         ON tis.app_name = appb.app_name
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb 
# MAGIC         ON tis.app_name = acrb.app_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist chanb 
# MAGIC         ON vizio_station.name = chanb.channel_name
# MAGIC     LEFT OUTER JOIN
# MAGIC         prod.detection.tv_ip_address AS ip
# MAGIC         ON c.session_start >= ip.create_timestamp
# MAGIC         AND c.session_start < ip.next_create_timestamp
# MAGIC         AND ip.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'nielsen' != 'nielsen'  
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND c.fk_show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND c.fk_show_id  = rep_nyc_nat.fk_show_id 
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-07-01T00:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-07-11T00:00:00'::timestamp
# MAGIC     AND c.partition_key >= '2024-07-01'
# MAGIC     AND c.partition_key <= '2024-07-11'
# MAGIC     AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test;
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test (
# MAGIC     tvid, 
# MAGIC     hash, 
# MAGIC     zipcode, 
# MAGIC     dma, 
# MAGIC     episode_id, 
# MAGIC     show_title, 
# MAGIC     air_date, 
# MAGIC     channel_callsign, 
# MAGIC     mt_start, 
# MAGIC     ts_start, 
# MAGIC     ts_end, 
# MAGIC     channel_affiliate, 
# MAGIC     live, 
# MAGIC     ip, 
# MAGIC     input_category, 
# MAGIC     input_device, 
# MAGIC     app_service
# MAGIC )
# MAGIC WITH activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'nielsen'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'nielsen'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT vendor_station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT vendor_station_id AS station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%nielsen%'
# MAGIC )
# MAGIC , inscape_station_map_dedupe AS (
# MAGIC     SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
# MAGIC     FROM (
# MAGIC         SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
# MAGIC         FROM detection.inscape_station_map) ism
# MAGIC     WHERE ism.rn = 1
# MAGIC )
# MAGIC SELECT /*+ BROADCAST(cid) */  DISTINCT
# MAGIC     COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
# MAGIC     '', 
# MAGIC     NULLIF(location.zipcode, ''), 
# MAGIC     REPLACE(dma.dma_name, ',', ''), 
# MAGIC     NULLIF(CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TMS' != 'TMS' THEN NULL 
# MAGIC             ELSE vizio_program.program_tms_id
# MAGIC         END
# MAGIC     ELSE
# MAGIC         CASE WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC             WHEN (cl.client_id is not null) THEN NULL
# MAGIC         ELSE
# MAGIC             CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC                 WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC                 WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key
# MAGIC         END
# MAGIC     END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC             ELSE vizio_program.title END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id is not null) THEN NULL
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'nielsen' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(c.tms_airdate, c.airdate)
# MAGIC         WHEN (cl.client_id is not null) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC         WHEN 'nielsen' != 'nielsen' THEN COALESCE(c.tms_airdate, c.airdate)
# MAGIC         WHEN 'nielsen' = 'nielsen' THEN c.tms_airdate
# MAGIC         ELSE NULL
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'nielsen' != 'nielsen' THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC         WHEN 'nielsen' = 'nielsen' AND c.tms_airdate IS NOT NULL THEN map.inscape_call_sign
# MAGIC     END,
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN 'nielsen' != 'nielsen' THEN LEAST(c.media_time_start, c.runtime)
# MAGIC         WHEN 'nielsen' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC         ELSE LEAST(c.media_time_start, c.runtime)
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL
# MAGIC     THEN CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED' 
# MAGIC         ELSE vizio_station.name END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC             -- WHEN 'nielsen' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
# MAGIC             WHEN 'nielsen' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC             WHEN c.tms_station_id IS NOT NULL THEN
# MAGIC                 CASE WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC                      WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC                            OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC                            OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC                         THEN station.station_affil ELSE NULL END
# MAGIC             WHEN c.tms_station_id IS NULL AND c.fk_station_id IS NOT NULL THEN
# MAGIC                 CASE WHEN (backup_station.inscape_station_name IS NOT NULL) THEN backup_station.inscape_station_name
# MAGIC                      WHEN (LOWER(backup_station.station_affil) LIKE '%affiliate%'
# MAGIC                           OR LOWER(backup_station.station_affil) LIKE '%independent%'
# MAGIC                           OR LOWER(backup_station.station_affil) LIKE '%low power%')
# MAGIC                         THEN backup_station.station_affil ELSE NULL END END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC         WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN 'nielsen' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
# MAGIC         WHEN 'nielsen' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN
# MAGIC       CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC          WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC           WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC           WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC          WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC           ELSE tis.app_name END
# MAGIC     WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
# MAGIC         THEN 'vMVPD'
# MAGIC     END
# MAGIC FROM
# MAGIC     dev_temp.detection.viewing_content_firehose_historic_0731 AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-07-11T00:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC     JOIN prod.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     JOIN prod.detection.tv_populations AS u
# MAGIC         ON c.fk_tvid = u.fk_tvid 
# MAGIC     JOIN prod.detection.populations AS pop
# MAGIC         ON u.fk_population_id = pop.population_id 
# MAGIC         AND pop.population_name = 'opted_in'
# MAGIC     JOIN prod.detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     LEFT OUTER JOIN prod.detection.dma AS dma
# MAGIC         ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.input_source inps 
# MAGIC         ON c.fk_input_source_id = inps.input_source_id
# MAGIC -- Historical change start
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS map
# MAGIC         ON map.mapped_vendor_station_id = c.tms_station_id
# MAGIC         AND map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN detection.epg_station AS station
# MAGIC         ON station.station_id = c.tms_station_id
# MAGIC         AND station.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN detection.epg_show AS show
# MAGIC         ON show.show_id = c.tms_show_id
# MAGIC         AND show.vendor_name = 'TMS'
# MAGIC     -- Backup values
# MAGIC     LEFT OUTER JOIN detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = c.fk_show_id
# MAGIC        AND c.tms_show_id IS NULL
# MAGIC        AND backup_show.vendor_name = 'TIVO'
# MAGIC        AND 'nielsen' != 'nielsen'
# MAGIC     LEFT OUTER JOIN detection.epg_station AS backup_station
# MAGIC         ON backup_station.station_id = c.fk_station_id
# MAGIC        AND c.tms_station_id IS NULL
# MAGIC        AND backup_station.vendor_name = 'TIVO'
# MAGIC        AND 'nielsen' != 'nielsen'
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS backup_map
# MAGIC         ON backup_map.mapped_vendor_station_id = c.fk_station_id
# MAGIC         AND c.tms_station_id IS NULL
# MAGIC         AND backup_map.mapped_vendor = 'TIVO'
# MAGIC         AND 'nielsen' != 'nielsen'
# MAGIC -- Historical change End
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
# MAGIC         ON c.tms_station_id = station_blacklist.station_id
# MAGIC         AND station_blacklist.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
# MAGIC         ON c.tms_station_id = station_obfs.vendor_station_id
# MAGIC         AND station_obfs.vendor_name = map.mapped_vendor  
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
# MAGIC         ON c.vizio_epg_station = vizio_station.station_id
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
# MAGIC         ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
# MAGIC         AND c.vizio_epg_program != '0'
# MAGIC     JOIN prod.detection.content_ids_firehose AS cid
# MAGIC         ON cid.content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
# MAGIC         ON m.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC         AND cl.client_name <> 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'nielsen'
# MAGIC     JOIN prod.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-07-11T00:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-07-01T00:00:00'::timestamp::double)::timestamp 
# MAGIC     LEFT OUTER JOIN activity_obfuscation AS appb
# MAGIC         ON tis.app_name = appb.app_name
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb 
# MAGIC         ON tis.app_name = acrb.app_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist chanb 
# MAGIC         ON vizio_station.name = chanb.channel_name
# MAGIC     LEFT OUTER JOIN
# MAGIC         prod.detection.tv_ip_address AS ip
# MAGIC         ON c.session_start >= ip.create_timestamp
# MAGIC         AND c.session_start < ip.next_create_timestamp
# MAGIC         AND ip.create_timestamp <= '2024-07-11T00:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-07-01T00:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'nielsen' != 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND c.fk_show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC         AND 'nielsen' != 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND c.fk_show_id  = rep_nyc_nat.fk_show_id 
# MAGIC         AND 'nielsen' != 'nielsen'
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-07-01T00:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-07-11T00:00:00'::timestamp
# MAGIC     AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC (SELECT 'Matching Rows' AS table_name
# MAGIC  , DATE(exs_report.ts_start) AS report_date
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC FROM dev.mohit_gangwani.content_with_null_nielsen_existing_table_backfill_test AS exs_report
# MAGIC JOIN dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test      AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip <=> new_report.ip
# MAGIC WHERE TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end) > 0
# MAGIC   AND TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end) > 0
# MAGIC  GROUP BY 2
# MAGIC )
# MAGIC UNION
# MAGIC (SELECT 'TiVo Existing Table' AS table_name
# MAGIC  , DATE(ts_start) AS report_date
# MAGIC  , COUNT(*) AS total_matching_rows
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_with_null_nielsen_existing_table_backfill_test AS exs_report
# MAGIC  WHERE TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end) > 0
# MAGIC  GROUP BY 2
# MAGIC )
# MAGIC UNION
# MAGIC (SELECT 'TiVo New Table' AS table_name
# MAGIC  , DATE(ts_start) AS report_date
# MAGIC  , COUNT(*) AS total_matching_rows
# MAGIC  , COUNT(DISTINCT new_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test      AS new_report
# MAGIC  WHERE TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end) > 0
# MAGIC  GROUP BY 2
# MAGIC )
# MAGIC ORDER BY 2, 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(exs_report.ts_start) AS report_date
# MAGIC , CASE WHEN NVL(exs_report.input_category, '') = NVL(new_report.input_category, '') THEN 'Match'
# MAGIC        ELSE 'No Match'
# MAGIC   END AS input_category_match
# MAGIC , CASE WHEN NVL(exs_report.input_device, '') = NVL(new_report.input_device, '') THEN 'Match'
# MAGIC        ELSE 'No Match'
# MAGIC   END AS input_device_match
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC FROM dev.mohit_gangwani.content_with_null_nielsen_existing_table_backfill_test AS exs_report
# MAGIC JOIN dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test      AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip = new_report.ip
# MAGIC WHERE TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end) > 0
# MAGIC   AND TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end) > 0
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(exs_report.ts_start) AS report_date
# MAGIC , CASE WHEN NVL(exs_report.zipcode, '') = NVL(new_report.zipcode, '') THEN 'Match'
# MAGIC        ELSE 'No Match'
# MAGIC   END AS zipcode_match
# MAGIC , CASE WHEN NVL(exs_report.dma, '') = NVL(new_report.dma, '') THEN 'Match'
# MAGIC        ELSE 'No Match'
# MAGIC   END AS dma_match
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC FROM dev.mohit_gangwani.content_with_null_nielsen_existing_table_backfill_test AS exs_report
# MAGIC JOIN dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test      AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip = new_report.ip
# MAGIC WHERE TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end) > 0
# MAGIC   AND TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end) > 0
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(exs_report.ts_start) AS report_date
# MAGIC , CASE WHEN exs_report.air_date IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.air_date = new_report.air_date THEN '1 - Match'
# MAGIC                        WHEN exs_report.air_date != new_report.air_date THEN '3 - No Match'
# MAGIC                        WHEN new_report.air_date IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.air_date IS NULL AND new_report.air_date IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS airdate_match
# MAGIC , CASE WHEN exs_report.episode_id IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.episode_id = new_report.episode_id THEN '1 - Match'
# MAGIC                        WHEN exs_report.episode_id != new_report.episode_id THEN '3 - No Match'
# MAGIC                        WHEN new_report.episode_id IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.episode_id IS NULL AND new_report.episode_id IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS episode_id_match
# MAGIC , CASE WHEN exs_report.channel_callsign IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.channel_callsign = new_report.channel_callsign THEN '1 - Match'
# MAGIC                        WHEN exs_report.channel_callsign != new_report.channel_callsign THEN '3 - No Match'
# MAGIC                        WHEN new_report.channel_callsign IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.channel_callsign IS NULL AND new_report.channel_callsign IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS channel_callsign_match
# MAGIC , CASE WHEN exs_report.live IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.live = new_report.live THEN '1 - Match'
# MAGIC                        WHEN exs_report.live != new_report.live THEN '3 - No Match'
# MAGIC                        WHEN new_report.live IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.live IS NULL AND new_report.live IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS live_match
# MAGIC , CASE WHEN exs_report.show_title  IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.show_title = new_report.show_title THEN '1 - Match'
# MAGIC                        WHEN exs_report.show_title != new_report.show_title THEN '3 - No Match'
# MAGIC                        WHEN new_report.show_title IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.show_title IS NULL AND new_report.show_title IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS show_title_match
# MAGIC , CASE WHEN exs_report.channel_affiliate IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.channel_affiliate = new_report.channel_affiliate THEN '1 - Match'
# MAGIC                        WHEN exs_report.channel_affiliate != new_report.channel_affiliate THEN '3 - No Match'
# MAGIC                        WHEN new_report.channel_affiliate IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.channel_affiliate IS NULL AND new_report.channel_affiliate IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS channel_affiliate_match
# MAGIC , COUNT(*)*1.0 AS session_count
# MAGIC FROM dev.mohit_gangwani.content_with_null_cognet_existing_table_backfill_test AS exs_report
# MAGIC JOIN dev.mohit_gangwani.content_with_null_cognet_new_table_backfill_test      AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip <=> new_report.ip
# MAGIC WHERE TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end) > 0
# MAGIC   AND TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end) > 0
# MAGIC GROUP BY 1,2,3,4,5,6,7
# MAGIC ORDER BY 1,2,3,4,5,6,7

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT DATE_TRUNC('MONTH', new_report.air_date), COUNT(*)
# MAGIC -- SELECT new_report.channel_callsign, exs_report.channel_callsign, COUNT(*)
# MAGIC SELECT new_report.episode_id, COUNT(*)
# MAGIC FROM dev.mohit_gangwani.content_with_null_cognet_existing_table_backfill_test AS exs_report
# MAGIC JOIN dev.mohit_gangwani.content_with_null_cognet_new_table_backfill_test      AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip <=> new_report.ip
# MAGIC AND exs_report.episode_id IS NULL AND new_report.episode_id IS NOT NULL
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM prod.detection.epg_show
# MAGIC WHERE database_key IN (4107445684,
# MAGIC 19074461,
# MAGIC 14657339543,
# MAGIC 982878477,
# MAGIC 1677539,
# MAGIC 14519215404)

# COMMAND ----------

# MAGIC %sql
# MAGIC --- should return 0.
# MAGIC select count(*)
# MAGIC FROM dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test AS new_report
# MAGIC where channel_callsign like '%,%' 
# MAGIC or show_title like '%,%'
# MAGIC or channel_affiliate like '%,%'
# MAGIC or dma like '%,%'

# COMMAND ----------

# MAGIC %sql
# MAGIC --- should return 0.
# MAGIC select count(*)
# MAGIC from dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test
# MAGIC where app_service = 'WatchFree+' and (live is null or live is false)

# COMMAND ----------

# MAGIC %sql
# MAGIC --- should return 0
# MAGIC select count(*)
# MAGIC from dev.mohit_gangwani.content_with_null_nielsen_new_table_backfill_test
# MAGIC where app_service in ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games')
# MAGIC and (channel_callsign is not null
# MAGIC or show_title is not null
# MAGIC or episode_id is not null
# MAGIC or air_date is not null
# MAGIC or mt_start is not null
# MAGIC or channel_affiliate is not null)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN exs_report.episode_id IS NULL THEN 'Null EPID' END AS null_epid
# MAGIC  , CASE WHEN exs_report.show_title IS NULL THEN 'Null show title' END AS null_show_title
# MAGIC  , CASE WHEN exs_report.air_date IS NULL THEN 'Null airdate' END AS null_air_date
# MAGIC  , CASE WHEN exs_report.channel_callsign IS NULL THEN 'Null callsign' END AS null_callsign
# MAGIC  , CASE WHEN exs_report.channel_affiliate IS NULL THEN 'Null affiliate' END AS null_affiliate
# MAGIC , CASE WHEN live IS NULL THEN 'Null live' END AS null_live
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_with_null_cognet_existing_table_backfill_test AS exs_report
# MAGIC --  WHERE exs_report.app_service IS NULL
# MAGIC  GROUP BY 1, 2, 3, 4, 5, 6

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN new_report.episode_id IS NULL THEN 'Null EPID' END AS null_epid
# MAGIC  , CASE WHEN new_report.show_title IS NULL THEN 'Null show title' END AS null_show_title
# MAGIC  , CASE WHEN new_report.air_date IS NULL THEN 'Null airdate' END AS null_air_date
# MAGIC  , CASE WHEN new_report.channel_callsign IS NULL THEN 'Null callsign' END AS null_callsign
# MAGIC  , CASE WHEN new_report.channel_affiliate IS NULL THEN 'Null affiliate' END AS null_affiliate
# MAGIC  , CASE WHEN live IS NULL THEN 'Null live' END AS null_live
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT new_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_with_null_cognet_new_table_backfill_test AS new_report
# MAGIC --  WHERE new_report.app_service IS NULL
# MAGIC --  WHERE COALESCE(new_report.app_service, '') != 'WatchFree+'
# MAGIC  GROUP BY 1, 2, 3, 4, 5, 6

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT new_report.*
# MAGIC FROM dev.mohit_gangwani.r388_content_with_null_nielsen_new_table AS new_report
# MAGIC WHERE new_report.channel_affiliate IS NULL
# MAGIC --  AND new_report.live IN ('t', 'f')
# MAGIC  AND new_report.episode_id IS NOT NULL
# MAGIC  AND new_report.channel_callsign IS NULL
# MAGIC ORDER BY tvid, ts_start
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT new_report.*
# MAGIC FROM dev.mohit_gangwani.r388_content_with_null_nielsen_new_table AS new_report
# MAGIC JOIN dev.mohit_gangwani.r388_content_with_null_nielsen_existing_table AS exs_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip = new_report.ip
# MAGIC  AND exs_report.air_date IS NULL
# MAGIC  AND new_report.air_date IS NOT NULL
# MAGIC ORDER BY new_report.tvid, new_report.ts_start
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT MIN(session_start), MAX(session_end)
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT MIN(session_start), MAX(session_end)
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC WHERE session_start >= '2024-07-01T00:00:00'
# MAGIC AND session_start < '2024-07-11T00:00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN vc.tms_station_id IS NULL THEN 'TMS ID - NULL' ELSE 'TMS ID - Not Null' END AS tms_id_check
# MAGIC , CASE WHEN vc.fk_station_id IS NULL THEN 'TIVO ID - NULL' ELSE 'TIVO ID - Not Null' END AS tivo_id_check
# MAGIC , COUNT(*)*1.0 AS session_count
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731 vc
# MAGIC WHERE vc.fk_content_id != 3468026
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN sh.show_id IS NOT NULL THEN 'EPG Show Maps'
# MAGIC             ELSE 'Value Present, Does not Match' END AS match_type
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT vc.tms_show_id) AS distinct_show_id_count
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731 vc
# MAGIC LEFT JOIN detection.epg_show sh
# MAGIC   ON sh.show_id = vc.tms_show_id
# MAGIC  AND sh.vendor_name = 'TMS'
# MAGIC WHERE vc.tms_show_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN sh.show_id IS NOT NULL THEN 'EPG Show Maps'
# MAGIC             ELSE 'Value Present, Does not Match' END AS match_type
# MAGIC , COUNT(*) AS session_count
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731 vc
# MAGIC LEFT JOIN detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE vc.fk_show_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN sh.show_id IS NOT NULL THEN 'EPG Show Maps'
# MAGIC             ELSE 'Value Present, Does not Match' END AS match_type
# MAGIC , COUNT(*) AS session_count
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731 vc
# MAGIC LEFT JOIN detection.epg_show sh
# MAGIC   ON sh.show_id = vc.tuner_channel_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE vc.tuner_channel_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN sh.show_id IS NOT NULL THEN 'EPG Show Maps'
# MAGIC             ELSE 'Value Present, Does not Match' END AS match_type
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT vc.tms_show_id) AS distinct_show_id_count
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731 vc
# MAGIC LEFT JOIN detection.epg_show sh
# MAGIC   ON sh.show_id = vc.tms_tuner_channel_id
# MAGIC  AND sh.vendor_name = 'TMS'
# MAGIC WHERE vc.tms_tuner_channel_id IS NOT NULL
# MAGIC GROUP BY CUBE(match_type)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(vc.session_start)
# MAGIC , CASE WHEN st.station_id IS NOT NULL THEN 'EPG station Maps'
# MAGIC             ELSE 'Value Present, Does not Match' END AS match_type
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT vc.tms_station_id) AS distinct_station_id_count
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731 vc
# MAGIC LEFT JOIN stage.detection.epg_station st
# MAGIC   ON st.station_id = vc.tms_station_id
# MAGIC  AND st.vendor_name = 'TMS'
# MAGIC WHERE vc.tms_station_id IS NOT NULL
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN st.inscape_station_id IS NOT NULL THEN 'EPG Show Maps'
# MAGIC             ELSE 'Value Present, Does not Match' END AS match_type
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT vc.tms_station_id) AS distinct_station_id_count
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731 vc
# MAGIC LEFT JOIN detection.inscape_station_map st
# MAGIC   ON st.mapped_vendor_station_id = vc.tms_station_id
# MAGIC  AND st.mapped_vendor = 'TMS'
# MAGIC WHERE vc.tms_station_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN st.station_id IS NOT NULL THEN 'EPG Show Maps'
# MAGIC             ELSE 'Value Present, Does not Match' END AS match_type
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT vc.fk_station_id) AS distinct_station_id_count
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731 vc
# MAGIC LEFT JOIN detection.epg_station st
# MAGIC   ON st.station_id = vc.fk_station_id
# MAGIC  AND st.vendor_name = 'TIVO'
# MAGIC WHERE vc.fk_station_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN st.station_id IS NOT NULL THEN 'EPG Schedule Maps'
# MAGIC             ELSE 'Value Present, Does not Match' END AS match_type
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT vc.fk_station_id) AS distinct_station_id_count
# MAGIC FROM dev_temp.detection.viewing_content_firehose_historic_0731 vc
# MAGIC LEFT JOIN detection.epg_station st
# MAGIC   ON st.station_id = vc.fk_station_id
# MAGIC  AND st.vendor_name = 'TIVO'
# MAGIC WHERE vc.fk_station_id IS NOT NULL
# MAGIC GROUP BY 1
