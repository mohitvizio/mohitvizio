# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.r444_content_cognet_existing_table;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.r444_content_cognet_existing_table (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.r444_content_cognet_existing_table (
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
# MAGIC              ELSE vizio_program.program_tms_id 
# MAGIC         END
# MAGIC     ELSE CASE
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key 
# MAGIC         END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL 
# MAGIC     THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC                 ELSE vizio_program.title END
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'cognet' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'cognet' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
# MAGIC         ELSE schedule.airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'cognet' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC         ELSE map.inscape_call_sign   
# MAGIC         END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'cognet' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC         ELSE LEAST(c.media_time_start, schedule.duration) 
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC     CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED'
# MAGIC         ELSE vizio_station.name END
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
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN 'cognet' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC     WHEN 'cognet' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC         CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC             WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC             WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC             WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC             WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC             ELSE tis.app_name END
# MAGIC         WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC     END
# MAGIC FROM
# MAGIC     detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-08-10T07:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-11T07:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-11T07:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-10T07:00:00'::timestamp
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
# MAGIC         AND schedule.airdate >= '2024-08-10T07:00:00'::timestamp - interval '60' day
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
# MAGIC         AND tvis.create_timestamp <= '2024-08-11T07:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-10T07:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-08-11T07:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-10T07:00:00'::timestamp::double)::timestamp 
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
# MAGIC         AND ip.create_timestamp <= '2024-08-11T07:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-08-10T07:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'cognet' != 'nielsen'  
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND backup_show.show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND backup_show.show_id  = rep_nyc_nat.fk_show_id 
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-10T07:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-08-11T07:00:00'::timestamp
# MAGIC     AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
# MAGIC     AND cl.client_id IS NULL
# MAGIC     AND (cid.content_cid <> 'unknown' OR vizio_station.name IS NOT NULL)
# MAGIC     AND CASE WHEN tvis.category = 'APPS' AND vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     AND CASE WHEN c.fk_station_id IS NOT NULL AND station_blacklist.station_id IS NOT NULL THEN FALSE ELSE TRUE END;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.r444_content_cognet_new_table;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.r444_content_cognet_new_table (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.r444_content_cognet_new_table (
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
# MAGIC              ELSE vizio_program.program_tms_id 
# MAGIC         END
# MAGIC     ELSE CASE
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key 
# MAGIC         END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL 
# MAGIC     THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC                 ELSE vizio_program.title END
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'cognet' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(c.airdate, c.tms_airdate)
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'cognet' != 'nielsen' THEN COALESCE(c.airdate, c.tms_airdate)
# MAGIC         WHEN 'cognet' = 'nielsen' THEN c.tms_airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(c.tms_airdate, c.airdate) IS NULL THEN NULL 
# MAGIC         WHEN 'cognet' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL 
# MAGIC         ELSE COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC         END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'cognet' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(c.tms_airdate, c.airdate) IS NULL THEN NULL 
# MAGIC         ELSE LEAST(c.media_time_start, c.runtime) 
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC     CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED'
# MAGIC         ELSE vizio_station.name END
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC             WHEN 'cognet' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC             WHEN 'cognet' != 'nielsen' AND COALESCE(c.tms_airdate, c.airdate) IS NULL THEN NULL 
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
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN 'cognet' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
# MAGIC     WHEN 'cognet' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL 
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC         CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC             WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC             WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC             WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC             WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC             ELSE tis.app_name END
# MAGIC         WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC     END
# MAGIC FROM
# MAGIC     dev.detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-08-10T07:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-11T07:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-11T07:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-10T07:00:00'::timestamp
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
# MAGIC         AND tvis.create_timestamp <= '2024-08-11T07:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-10T07:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-08-11T07:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-10T07:00:00'::timestamp::double)::timestamp 
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
# MAGIC         AND ip.create_timestamp <= '2024-08-11T07:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-08-10T07:00:00'::timestamp
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
# MAGIC         c.session_start >= '2024-08-10T07:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-08-11T07:00:00'::timestamp
# MAGIC     AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
# MAGIC     AND cl.client_id IS NULL
# MAGIC     AND (cid.content_cid <> 'unknown' OR vizio_station.name IS NOT NULL)
# MAGIC     AND CASE WHEN tvis.category = 'APPS' AND vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     AND CASE WHEN c.fk_station_id IS NOT NULL AND station_blacklist.station_id IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.r444_content_only_existing_table_tms;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.r444_content_only_existing_table_tms (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.r444_content_only_existing_table_tms (
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
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TMS' != 'TMS' THEN NULL
# MAGIC              ELSE vizio_program.program_tms_id 
# MAGIC         END
# MAGIC     ELSE CASE
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key 
# MAGIC         END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL 
# MAGIC     THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC                 ELSE vizio_program.title END
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'nielsen' != 'cognet' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'nielsen' != 'cognet' THEN COALESCE(schedule.airdate, c.airdate)
# MAGIC         ELSE schedule.airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'nielsen' != 'cognet' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'nielsen' = 'cognet' AND schedule.airdate IS NULL THEN NULL 
# MAGIC         ELSE map.inscape_call_sign   
# MAGIC         END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'nielsen' != 'cognet' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'nielsen' = 'cognet' AND schedule.airdate IS NULL THEN NULL 
# MAGIC         ELSE LEAST(c.media_time_start, schedule.duration) 
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC     CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED'
# MAGIC         ELSE vizio_station.name END
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL 
# MAGIC             WHEN 'nielsen' != 'cognet' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC             WHEN 'nielsen' = 'cognet' AND schedule.airdate IS NULL THEN NULL
# MAGIC             WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC             WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC                 OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC                 OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC             THEN station.station_affil ELSE NULL END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN 'nielsen' != 'cognet' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC     WHEN 'nielsen' = 'cognet' AND schedule.airdate IS NULL THEN NULL 
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC         CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC             WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC             WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC             WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC             WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC             ELSE tis.app_name END
# MAGIC         WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC     END
# MAGIC FROM
# MAGIC     detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-08-27T16:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-27T17:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-27T17:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-27T16:00:00'::timestamp
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
# MAGIC         AND schedule.airdate >= '2024-08-27T16:00:00'::timestamp - interval '60' day
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
# MAGIC         AND tvis.create_timestamp <= '2024-08-27T17:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-27T16:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-08-27T17:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-27T16:00:00'::timestamp::double)::timestamp 
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
# MAGIC         AND ip.create_timestamp <= '2024-08-27T17:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-08-27T16:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'nielsen' != 'cognet'  
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND backup_show.show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND backup_show.show_id  = rep_nyc_nat.fk_show_id 
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-27T16:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-08-27T17:00:00'::timestamp
# MAGIC     AND ABS(MOD(c.fk_tvid, 5)) = 0
# MAGIC     AND c.partition_key = '2024-08-27'
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
# MAGIC     AND cl.client_id IS NULL
# MAGIC     AND (cid.content_cid <> 'unknown' OR vizio_station.name IS NOT NULL)
# MAGIC     AND CASE WHEN tvis.category = 'APPS' AND vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     AND CASE WHEN c.fk_station_id IS NOT NULL AND station_blacklist.station_id IS NOT NULL THEN FALSE ELSE TRUE END;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.r444_content_only_new_table_tms;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.r444_content_only_new_table_tms (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.r444_content_only_new_table_tms (
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
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TMS' != 'TMS' THEN NULL
# MAGIC              ELSE vizio_program.program_tms_id 
# MAGIC         END
# MAGIC     ELSE CASE
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key 
# MAGIC         END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL 
# MAGIC     THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC                 ELSE vizio_program.title END
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'cognet' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(c.tms_airdate, c.airdate)
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'cognet' != 'nielsen' THEN COALESCE(c.tms_airdate, c.airdate)
# MAGIC         WHEN 'cognet' = 'nielsen' THEN c.tms_airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(c.tms_airdate, c.airdate) IS NULL THEN NULL 
# MAGIC         WHEN 'cognet' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL 
# MAGIC         ELSE COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC         END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'cognet' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC         WHEN 'cognet' != 'nielsen' AND COALESCE(c.tms_airdate, c.airdate) IS NULL THEN NULL 
# MAGIC         ELSE LEAST(c.media_time_start, c.runtime) 
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC     CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED'
# MAGIC         ELSE vizio_station.name END
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC             WHEN 'cognet' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC             WHEN 'cognet' != 'nielsen' AND COALESCE(c.tms_airdate, c.airdate) IS NULL THEN NULL 
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
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN 'cognet' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
# MAGIC     WHEN 'cognet' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL 
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC         CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC             WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC             WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC             WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC             WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC             ELSE tis.app_name END
# MAGIC         WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC     END
# MAGIC FROM
# MAGIC     dev.detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-08-27T16:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-27T17:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-27T17:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-27T16:00:00'::timestamp
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
# MAGIC        AND 'cognet' != 'nielsen'
# MAGIC     LEFT OUTER JOIN detection.epg_station AS backup_station
# MAGIC         ON backup_station.station_id = c.fk_station_id
# MAGIC        AND c.tms_station_id IS NULL
# MAGIC        AND backup_station.vendor_name = 'TIVO'
# MAGIC        AND 'cognet' != 'nielsen'
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS backup_map
# MAGIC         ON backup_map.mapped_vendor_station_id = c.fk_station_id
# MAGIC         AND c.tms_station_id IS NULL
# MAGIC         AND backup_map.mapped_vendor = 'TIVO'
# MAGIC         AND 'cognet' != 'nielsen'
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
# MAGIC         AND cl.client_name <> 'cognet'
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'cognet'
# MAGIC     JOIN prod.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-08-27T17:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-27T16:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-08-27T17:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-27T16:00:00'::timestamp::double)::timestamp 
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
# MAGIC         AND ip.create_timestamp <= '2024-08-27T17:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-08-27T16:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'cognet' != 'nielsen'  
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.tms_airdate = rep_local.airdate
# MAGIC         AND c.tms_show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC         AND 'cognet' != 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.tms_airdate = rep_nyc_nat.airdate
# MAGIC         AND c.tms_show_id  = rep_nyc_nat.fk_show_id 
# MAGIC         AND 'cognet' != 'nielsen'
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-27T16:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-08-27T17:00:00'::timestamp
# MAGIC     AND ABS(MOD(c.fk_tvid, 5)) = 0
# MAGIC     AND c.partition_key = '2024-08-27'
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, backup_station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
# MAGIC     AND cl.client_id IS NULL
# MAGIC     AND (cid.content_cid <> 'unknown' OR vizio_station.name IS NOT NULL)
# MAGIC     AND CASE WHEN tvis.category = 'APPS' AND vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     AND CASE WHEN COALESCE(c.fk_station_id, c.tms_station_id) IS NOT NULL AND station_blacklist.station_id IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id_tuner string, show_title_tuner string, air_date_tuner string, channel_callsign_tuner string, mt_start_tuner integer, ts_start timestamp, ts_end timestamp, channel_affiliate_tuner string, live_tuner string, ip string, input_category_tuner string, input_device_tuner string, app_service_tuner string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms (
# MAGIC     tvid, 
# MAGIC     hash, 
# MAGIC     zipcode, 
# MAGIC     dma, 
# MAGIC     episode_id_tuner, 
# MAGIC     show_title_tuner, 
# MAGIC     air_date_tuner, 
# MAGIC     channel_callsign_tuner, 
# MAGIC     mt_start_tuner, 
# MAGIC     ts_start, 
# MAGIC     ts_end, 
# MAGIC     channel_affiliate_tuner, 
# MAGIC     live_tuner, 
# MAGIC     ip, 
# MAGIC     input_category_tuner, 
# MAGIC     input_device_tuner, 
# MAGIC     app_service_tuner
# MAGIC )
# MAGIC WITH activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'scripps'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'scripps'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%scripps%'
# MAGIC )
# MAGIC SELECT /*+ BROADCAST(cid) */  DISTINCT
# MAGIC     COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
# MAGIC     '', 
# MAGIC     NULLIF(location.zipcode, ''), 
# MAGIC     REPLACE(dma.dma_name, ',', ''), 
# MAGIC     NULLIF(CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TMS' != 'TMS' THEN NULL
# MAGIC              ELSE vizio_program.program_tms_id 
# MAGIC         END
# MAGIC     ELSE CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key
# MAGIC         END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL 
# MAGIC     THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC                 ELSE vizio_program.title END
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL
# MAGIC         WHEN 'scripps' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'scripps' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
# MAGIC         ELSE schedule.airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN station.station_call_sign 
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'scripps' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'scripps' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         ELSE map.inscape_call_sign
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'scripps' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'scripps' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         WHEN c.tuner_channel_id IS NOT NULL THEN LEAST((unix_timestamp(c.session_start)-unix_timestamp(schedule.airdate)), schedule.duration) 
# MAGIC         ELSE LEAST(c.media_time_start, schedule.duration)
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC     CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED'
# MAGIC         ELSE vizio_station.name END
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL 
# MAGIC             WHEN 'scripps' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC             WHEN 'scripps' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC             WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC             WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC                 OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC                 OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC             THEN station.station_affil ELSE NULL END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN 'scripps' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC     WHEN 'scripps' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC     WHEN c.tuner_channel_id IS NOT NULL THEN 't'
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     CASE
# MAGIC         WHEN c.tuner_channel_id IS NOT NULL AND UPPER(tvis.category) in ('APPS', 'OTHER', 'OTT')
# MAGIC         THEN 'HD TV'
# MAGIC         WHEN UPPER(tvis.category) = 'OTHER' AND tis.app_name = 'WatchFree+' AND cid.content_cid != 'unknown'
# MAGIC         THEN 'HD TV'
# MAGIC         ELSE tvis.category
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.tuner_channel_id IS NOT NULL AND UPPER(tvis.category) in ('APPS', 'OTHER', 'OTT')
# MAGIC         THEN 'OTA'
# MAGIC         ELSE tvis.input_device
# MAGIC     END, 
# MAGIC     CASE 
# MAGIC         WHEN c.tuner_channel_id is not null 
# MAGIC             AND (input_device IS NULL OR input_device = 'OTA') 
# MAGIC             THEN 'WatchFree+' 
# MAGIC         WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC         CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+' 
# MAGIC             WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED' 
# MAGIC             WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED' 
# MAGIC             WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL 
# MAGIC             WHEN lower(tis.app_name) = 'unknown' THEN NULL 
# MAGIC             ELSE tis.app_name END 
# MAGIC         WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') 
# MAGIC             THEN 'vMVPD' 
# MAGIC         WHEN inps.input_source IN ('DTV', 'TUNER', 'COAXIAL', 'ATV') AND (input_device IS NULL OR input_device = 'OTA') 
# MAGIC              AND tis.app_name ='WatchFree+' AND c.is_live = TRUE THEN 'WatchFree+' 
# MAGIC     END
# MAGIC FROM
# MAGIC     detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-08-27T18:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-27T19:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-27T19:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-27T18:00:00'::timestamp
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
# MAGIC         ON map.inscape_station_id = coalesce(c.tuner_channel_id, c.fk_station_id)
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
# MAGIC         AND schedule.airdate >= '2024-08-27T18:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS show
# MAGIC         ON schedule.fk_show_id = show.show_id
# MAGIC         AND show.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = coalesce(c.tuner_program_id, c.fk_show_id)
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
# MAGIC         AND cl.client_name <> 'scripps'
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'scripps'
# MAGIC     JOIN prod.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-08-27T19:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-27T18:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-08-27T19:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-27T18:00:00'::timestamp::double)::timestamp 
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
# MAGIC         AND ip.create_timestamp <= '2024-08-27T19:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-08-27T18:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'scripps' != 'nielsen'  
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND backup_show.show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND backup_show.show_id  = rep_nyc_nat.fk_show_id 
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-27T18:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-08-27T19:00:00'::timestamp
# MAGIC     AND c.partition_key = '2024-08-27'
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
# MAGIC     AND cl.client_id IS NULL
# MAGIC     AND (cid.content_cid <> 'unknown' OR vizio_station.name IS NOT NULL
# MAGIC     OR c.tuner_channel_id IS NOT NULL)
# MAGIC     AND CASE WHEN tvis.category = 'APPS' AND vizio_station.name IS NULL AND c.tuner_channel_id IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     AND CASE WHEN c.fk_station_id IS NOT NULL AND station_blacklist.station_id IS NOT NULL THEN FALSE ELSE TRUE END

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id_tuner string, show_title_tuner string, air_date_tuner string, channel_callsign_tuner string, mt_start_tuner integer, ts_start timestamp, ts_end timestamp, channel_affiliate_tuner string, live_tuner string, ip string, input_category_tuner string, input_device_tuner string, app_service_tuner string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms (
# MAGIC     tvid, 
# MAGIC     hash, 
# MAGIC     zipcode, 
# MAGIC     dma, 
# MAGIC     episode_id_tuner, 
# MAGIC     show_title_tuner, 
# MAGIC     air_date_tuner, 
# MAGIC     channel_callsign_tuner, 
# MAGIC     mt_start_tuner, 
# MAGIC     ts_start, 
# MAGIC     ts_end, 
# MAGIC     channel_affiliate_tuner, 
# MAGIC     live_tuner, 
# MAGIC     ip, 
# MAGIC     input_category_tuner, 
# MAGIC     input_device_tuner, 
# MAGIC     app_service_tuner
# MAGIC )
# MAGIC WITH activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'scripps'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'scripps'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT vendor_station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT vendor_station_id AS station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%scripps%'
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
# MAGIC              ELSE vizio_program.program_tms_id 
# MAGIC         END
# MAGIC     ELSE CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC             ELSE show.database_key
# MAGIC         END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL 
# MAGIC     THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC                 ELSE vizio_program.title END
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL
# MAGIC         WHEN 'scripps' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(c.tms_airdate, c.airdate)
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'scripps' != 'nielsen' THEN COALESCE(c.tms_airdate, c.airdate)
# MAGIC         ELSE COALESCE(c.tms_airdate, c.airdate)
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign) 
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'scripps' != 'nielsen' AND COALESCE(c.tms_airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'scripps' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC         ELSE COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN 'scripps' != 'nielsen' AND COALESCE(c.tms_airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'scripps' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC         WHEN c.tuner_channel_id IS NOT NULL THEN LEAST((unix_timestamp(c.session_start)-unix_timestamp(COALESCE(c.tms_airdate, c.airdate))), c.runtime) 
# MAGIC         ELSE LEAST(c.media_time_start, c.runtime)
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC     CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED'
# MAGIC         ELSE vizio_station.name END
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL 
# MAGIC             -- WHEN 'scripps' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
# MAGIC             WHEN 'scripps' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC             WHEN COALESCE(c.tms_station_id, c.tms_tuner_channel_id) IS NOT NULL THEN
# MAGIC                 CASE WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC                      WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC                            OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC                            OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC                         THEN station.station_affil ELSE NULL END
# MAGIC             WHEN COALESCE(c.tms_station_id, c.tms_tuner_channel_id) IS NULL AND COALESCE(c.fk_station_id, c.tuner_channel_id) IS NOT NULL THEN
# MAGIC                 CASE WHEN (backup_station.inscape_station_name IS NOT NULL) THEN backup_station.inscape_station_name
# MAGIC                      WHEN (LOWER(backup_station.station_affil) LIKE '%affiliate%'
# MAGIC                           OR LOWER(backup_station.station_affil) LIKE '%independent%'
# MAGIC                           OR LOWER(backup_station.station_affil) LIKE '%low power%')
# MAGIC                         THEN backup_station.station_affil ELSE NULL END END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN 'scripps' != 'nielsen' AND COALESCE(c.tms_airdate, c.airdate) IS NULL THEN NULL
# MAGIC     WHEN 'scripps' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC     WHEN COALESCE(c.tms_tuner_channel_id, c.tuner_channel_id) IS NOT NULL THEN 't'
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL AND UPPER(tvis.category) in ('APPS', 'OTHER', 'OTT')
# MAGIC         THEN 'HD TV'
# MAGIC         WHEN UPPER(tvis.category) = 'OTHER' AND tis.app_name = 'WatchFree+' AND cid.content_cid != 'unknown'
# MAGIC         THEN 'HD TV'
# MAGIC         ELSE tvis.category
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL AND UPPER(tvis.category) in ('APPS', 'OTHER', 'OTT')
# MAGIC         THEN 'OTA'
# MAGIC         ELSE tvis.input_device
# MAGIC     END, 
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) is not null 
# MAGIC             AND (input_device IS NULL OR input_device = 'OTA') 
# MAGIC             THEN 'WatchFree+' 
# MAGIC         WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC         CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+' 
# MAGIC             WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED' 
# MAGIC             WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED' 
# MAGIC             WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL 
# MAGIC             WHEN lower(tis.app_name) = 'unknown' THEN NULL 
# MAGIC             ELSE tis.app_name END 
# MAGIC         WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') 
# MAGIC             THEN 'vMVPD' 
# MAGIC         WHEN inps.input_source IN ('DTV', 'TUNER', 'COAXIAL', 'ATV') AND (input_device IS NULL OR input_device = 'OTA') 
# MAGIC              AND tis.app_name ='WatchFree+' AND c.is_live = TRUE THEN 'WatchFree+' 
# MAGIC     END
# MAGIC FROM
# MAGIC     prod.detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-08-27T18:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-27T19:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-27T19:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-27T18:00:00'::timestamp
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
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS map
# MAGIC         ON map.mapped_vendor_station_id = coalesce(c.tms_tuner_channel_id, c.tms_station_id)
# MAGIC         AND map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS station
# MAGIC         ON station.station_id = coalesce(c.tms_tuner_channel_id, c.tms_station_id)
# MAGIC         AND station.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS show
# MAGIC         ON show.show_id = coalesce(c.tms_tuner_program_id, c.tms_show_id)
# MAGIC         AND show.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = coalesce(c.tuner_program_id, c.fk_show_id)
# MAGIC         AND c.tms_show_id IS NULL
# MAGIC        AND backup_show.vendor_name = 'TIVO'
# MAGIC        AND 'scripps' != 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS backup_station
# MAGIC         ON backup_station.station_id = COALESCE(c.tuner_channel_id, c.fk_station_id)
# MAGIC        AND c.tms_station_id IS NULL
# MAGIC        AND backup_station.vendor_name = 'TIVO'
# MAGIC        AND 'scripps' != 'nielsen'
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS backup_map
# MAGIC         ON backup_map.mapped_vendor_station_id = COALESCE(c.tuner_channel_id, c.fk_station_id)
# MAGIC         AND c.tms_station_id IS NULL
# MAGIC         AND backup_map.mapped_vendor = 'TIVO'
# MAGIC         AND 'scripps' != 'nielsen'
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
# MAGIC         AND cl.client_name <> 'scripps'
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'scripps'
# MAGIC     JOIN prod.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-08-27T19:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-27T18:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-08-27T19:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-27T18:00:00'::timestamp::double)::timestamp 
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
# MAGIC         AND ip.create_timestamp <= '2024-08-27T19:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-08-27T18:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = COALESCE(map.inscape_station_id, backup_map.inscape_station_id)
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'scripps' != 'nielsen'  
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON COALESCE(map.inscape_station_id, backup_map.inscape_station_id) = rep_local.station_id 
# MAGIC         AND COALESCE(c.airdate, c.tms_airdate) = rep_local.airdate
# MAGIC         AND COALESCE(c.fk_show_id, c.tms_show_id, c.tms_tuner_program_id, c.tuner_program_id) = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC         AND 'scripps' != 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON COALESCE(map.inscape_station_id, backup_map.inscape_station_id) = rep_nyc_nat.station_id 
# MAGIC         AND COALESCE(c.airdate, c.tms_airdate) = rep_nyc_nat.airdate
# MAGIC         AND COALESCE(c.fk_show_id, c.tms_show_id, c.tms_tuner_program_id, c.tuner_program_id) = rep_nyc_nat.fk_show_id 
# MAGIC         AND 'scripps' != 'nielsen'
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-27T18:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-08-27T19:00:00'::timestamp
# MAGIC     AND c.partition_key = '2024-08-27'
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, backup_station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
# MAGIC     AND cl.client_id IS NULL
# MAGIC     AND (cid.content_cid <> 'unknown' OR vizio_station.name IS NOT NULL OR COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL)
# MAGIC     AND CASE WHEN tvis.category = 'APPS' AND vizio_station.name IS NULL AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     AND CASE WHEN COALESCE(c.fk_station_id, c.tms_station_id) IS NOT NULL AND station_blacklist.station_id IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC (SELECT 'Matching Rows' AS table_name
# MAGIC  , DATE(exs_report.ts_start) AS report_date
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms AS exs_report
# MAGIC  JOIN dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip <=> new_report.ip
# MAGIC  GROUP BY 2
# MAGIC )
# MAGIC UNION
# MAGIC (SELECT 'Existing Table' AS table_name
# MAGIC  , DATE(ts_start) AS report_date
# MAGIC  , COUNT(*) AS total_matching_rows
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms AS exs_report
# MAGIC  GROUP BY 2
# MAGIC )
# MAGIC UNION
# MAGIC (SELECT 'New Table' AS table_name
# MAGIC  , DATE(ts_start) AS report_date
# MAGIC  , COUNT(*) AS total_matching_rows
# MAGIC  , COUNT(DISTINCT new_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms AS new_report
# MAGIC  GROUP BY 2
# MAGIC )
# MAGIC ORDER BY 2, 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(exs_report.ts_start) AS report_date
# MAGIC , CASE WHEN exs_report.input_category_tuner <=> new_report.input_category_tuner THEN 'Match'
# MAGIC        ELSE 'No Match'
# MAGIC   END AS input_category_match
# MAGIC , CASE WHEN exs_report.input_device_tuner <=> new_report.input_device_tuner THEN 'Match'
# MAGIC        ELSE 'No Match'
# MAGIC   END AS input_device_match
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC FROM dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms AS exs_report
# MAGIC JOIN dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip <=> new_report.ip
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
# MAGIC FROM dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms AS exs_report
# MAGIC JOIN dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip <=> new_report.ip
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(exs_report.ts_start) AS report_date
# MAGIC , CASE WHEN exs_report.air_date_tuner IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.air_date_tuner = new_report.air_date_tuner THEN '1 - Match'
# MAGIC                        WHEN exs_report.air_date_tuner != new_report.air_date_tuner THEN '3 - No Match'
# MAGIC                        WHEN new_report.air_date_tuner IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.air_date_tuner IS NULL AND new_report.air_date_tuner IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS airdate_match
# MAGIC , CASE WHEN exs_report.episode_id_tuner IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.episode_id_tuner = new_report.episode_id_tuner THEN '1 - Match'
# MAGIC                        WHEN exs_report.episode_id_tuner != new_report.episode_id_tuner THEN '3 - No Match'
# MAGIC                        WHEN new_report.episode_id_tuner IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.episode_id_tuner IS NULL AND new_report.episode_id_tuner IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS episode_id_match
# MAGIC , CASE WHEN exs_report.channel_callsign_tuner IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.channel_callsign_tuner = new_report.channel_callsign_tuner THEN '1 - Match'
# MAGIC                        WHEN exs_report.channel_callsign_tuner != new_report.channel_callsign_tuner THEN '3 - No Match'
# MAGIC                        WHEN new_report.channel_callsign_tuner IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.channel_callsign_tuner IS NULL AND new_report.channel_callsign_tuner IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS channel_callsign_match
# MAGIC , CASE WHEN exs_report.live_tuner IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.live_tuner = new_report.live_tuner THEN '1 - Match'
# MAGIC                        WHEN exs_report.live_tuner != new_report.live_tuner THEN '3 - No Match'
# MAGIC                        WHEN new_report.live_tuner IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.live_tuner IS NULL AND new_report.live_tuner IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS live_match
# MAGIC , CASE WHEN exs_report.show_title_tuner  IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.show_title_tuner = new_report.show_title_tuner THEN '1 - Match'
# MAGIC                        WHEN exs_report.show_title_tuner != new_report.show_title_tuner THEN '3 - No Match'
# MAGIC                        WHEN new_report.show_title_tuner IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.show_title_tuner IS NULL AND new_report.show_title_tuner IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS show_title_match
# MAGIC , CASE WHEN exs_report.channel_affiliate_tuner IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.channel_affiliate_tuner = new_report.channel_affiliate_tuner THEN '1 - Match'
# MAGIC                        WHEN exs_report.channel_affiliate_tuner != new_report.channel_affiliate_tuner THEN '3 - No Match'
# MAGIC                        WHEN new_report.channel_affiliate_tuner IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.channel_affiliate_tuner IS NULL AND new_report.channel_affiliate_tuner IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS channel_affiliate_match
# MAGIC , COUNT(*)*1.0 AS session_count
# MAGIC FROM dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms AS new_report
# MAGIC JOIN dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms AS exs_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip <=> new_report.ip
# MAGIC GROUP BY 1,2,3,4,5,6,7
# MAGIC ORDER BY 1,2,3,4,5,6,7

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH exs_table AS (
# MAGIC   SELECT app_service_tuner AS app_service
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms AS exs_report
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC , new_table AS (
# MAGIC   SELECT app_service_tuner AS app_service
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT new_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms AS new_report
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC SELECT NVL(e.app_service, n.app_service) AS app_service
# MAGIC , e.session_count AS existing_session_count
# MAGIC , n.session_count AS new_session_count
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC , e.ttl_hrs AS existing_ttl_hrs
# MAGIC , n.ttl_hrs AS new_ttl_hrs
# MAGIC , (e.session_count - n.session_count)/(e.session_count*.01) AS session_count_diff_perc
# MAGIC , (e.total_tvs - n.total_tvs)/(e.total_tvs*.01) AS tv_count_diff_perc
# MAGIC , (e.ttl_hrs - n.ttl_hrs)/(e.ttl_hrs*.01) AS duration_diff_perc
# MAGIC FROM exs_table e
# MAGIC FULL OUTER JOIN new_table n
# MAGIC  ON e.app_service <=> n.app_service

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH exs_table AS (
# MAGIC   SELECT input_category_tuner AS input_category
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms AS exs_report
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC , new_table AS (
# MAGIC   SELECT input_category_tuner AS input_category
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT new_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms AS new_report
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC SELECT NVL(e.input_category, n.input_category) AS input_category
# MAGIC , e.session_count AS existing_session_count
# MAGIC , n.session_count AS new_session_count
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC , e.ttl_hrs AS existing_ttl_hrs
# MAGIC , n.ttl_hrs AS new_ttl_hrs
# MAGIC , (e.session_count - n.session_count)/(e.session_count*.01) AS session_count_diff_perc
# MAGIC , (e.total_tvs - n.total_tvs)/(e.total_tvs*.01) AS tv_count_diff_perc
# MAGIC , (e.ttl_hrs - n.ttl_hrs)/(e.ttl_hrs*.01) AS duration_diff_perc
# MAGIC FROM exs_table e
# MAGIC FULL OUTER JOIN new_table n
# MAGIC  ON e.input_category <=> n.input_category

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH exs_table AS (
# MAGIC   SELECT exs_report.input_device_tuner AS input_device
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms AS exs_report
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC , new_table AS (
# MAGIC   SELECT input_device_tuner AS input_device
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT new_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_newtable_toggle_on_tms AS new_report
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC SELECT NVL(e.input_device, n.input_device) AS input_device
# MAGIC , e.session_count AS existing_session_count
# MAGIC , n.session_count AS new_session_count
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC , e.ttl_hrs AS existing_ttl_hrs
# MAGIC , n.ttl_hrs AS new_ttl_hrs
# MAGIC , (e.session_count - n.session_count)/(e.session_count*.01) AS session_count_diff_perc
# MAGIC , (e.total_tvs - n.total_tvs)/(e.total_tvs*.01) AS tv_count_diff_perc
# MAGIC , (e.ttl_hrs - n.ttl_hrs)/(e.ttl_hrs*.01) AS duration_diff_perc
# MAGIC FROM exs_table e
# MAGIC FULL OUTER JOIN new_table n
# MAGIC  ON e.input_device <=> n.input_device

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH exs_table AS (
# MAGIC   SELECT exs_report.input_device
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.content_null_scripps_tuner_existingtable_toggle_on_tms AS exs_report
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC , new_table AS (
# MAGIC   SELECT input_device
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT new_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.r444_content_only_new_table_tms AS new_report
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC SELECT NVL(e.input_device, n.input_device) AS input_device
# MAGIC , e.session_count AS existing_session_count
# MAGIC , n.session_count AS new_session_count
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC , e.ttl_hrs AS existing_ttl_hrs
# MAGIC , n.ttl_hrs AS new_ttl_hrs
# MAGIC , (e.session_count - n.session_count)/(e.session_count*.01) AS session_count_diff_perc
# MAGIC , (e.total_tvs - n.total_tvs)/(e.total_tvs*.01) AS tv_count_diff_perc
# MAGIC , (e.ttl_hrs - n.ttl_hrs)/(e.ttl_hrs*.01) AS duration_diff_perc
# MAGIC FROM exs_table e
# MAGIC FULL OUTER JOIN new_table n
# MAGIC  ON e.input_device <=> n.input_device

# COMMAND ----------

# MAGIC %sql
# MAGIC --- should return 0.
# MAGIC select count(*)
# MAGIC FROM dev.mohit_gangwani.r444_content_nielsen_new_table AS new_report
# MAGIC where channel_callsign like '%,%' 
# MAGIC or show_title like '%,%'
# MAGIC or channel_affiliate like '%,%'
# MAGIC or dma like '%,%'

# COMMAND ----------

# MAGIC %sql
# MAGIC --- should return 0.
# MAGIC select count(*)
# MAGIC from dev.mohit_gangwani.r444_content_nielsen_new_table AS new_report
# MAGIC where app_service = 'WatchFree+' and (live is null or live is false)

# COMMAND ----------

# MAGIC %sql
# MAGIC --- should return 0
# MAGIC select count(*)
# MAGIC from dev.mohit_gangwani.r444_content_nielsen_new_table AS new_report
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
# MAGIC  FROM dev.mohit_gangwani.r444_content_nielsen_existing_table AS exs_report
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
# MAGIC  FROM dev.mohit_gangwani.r444_content_nielsen_new_table AS new_report
# MAGIC --  WHERE new_report.app_service IS NULL
# MAGIC --  WHERE COALESCE(new_report.app_service, '') != 'WatchFree+'
# MAGIC  GROUP BY 1, 2, 3, 4, 5, 6

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT new_report.app_service
# MAGIC -- , ni.station_id IS NOT NULL
# MAGIC , COUNT(*)
# MAGIC FROM dev.mohit_gangwani.r444_content_cognet_existing_table AS exs_report
# MAGIC JOIN dev.mohit_gangwani.r444_content_cognet_new_table AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip = new_report.ip
# MAGIC  AND exs_report.episode_id IS NULL
# MAGIC  AND new_report.episode_id IS NOT NULL
# MAGIC -- LEFT JOIN detection.nielsen_only_distribution_blacklist ni
# MAGIC --   ON ni.nielsen_channel_callsign = exs_report.channel_callsign
# MAGIC GROUP BY 1

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
# MAGIC SELECT DATE_TRUNC('HOUR', session_start)
# MAGIC , COALESCE(vizio_epg_airing, vizio_epg_station, vizio_epg_program) IS NULL AS null_wf
# MAGIC , COALESCE(fk_station_id, fk_show_id, fk_schedule_id) IS NULL AS null_tivo_fk
# MAGIC , COALESCE(tms_station_id, tms_show_id, tms_schedule_id) IS NULL AS null_tms_fk
# MAGIC , COALESCE(tuner_schedule_id, tuner_program_id, tuner_channel_id) IS NULL AS null_tuner_tivo
# MAGIC , COALESCE(tms_tuner_schedule_id, tms_tuner_program_id, tms_tuner_channel_id) IS NULL AS null_tuner_tms
# MAGIC , COUNT(*)
# MAGIC FROM dev.detection.viewing_content_firehose
# MAGIC WHERE session_start >= '2024-08-02 00:00:00'
# MAGIC AND session_start < '2024-08-03 00:00:00'
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DATE('2024-08-02 03:00:00'::TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DATE_TRUNC('HOUR', session_start), COUNT(*)
# MAGIC FROM dev.detection.viewing_content_firehose
# MAGIC WHERE session_start >= CURRENT_DATE - 1
# MAGIC AND partition_key >= DATE(CURRENT_DATE - 1)
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC (SELECT DATE_TRUNC('HOUR', session_start), COUNT(DISTINCT fk_tvid) AS tv_count, COUNT(*) AS session_count, SUM(session_duration)/3600.0 AS ttl_duration, 'Prod' AS schema_
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC WHERE session_start >= CURRENT_DATE - 2
# MAGIC AND partition_key >= DATE(CURRENT_DATE - 2)
# MAGIC GROUP BY 1)
# MAGIC UNION
# MAGIC (SELECT DATE_TRUNC('HOUR', session_start), COUNT(DISTINCT fk_tvid) AS tv_count, COUNT(*) AS session_count, SUM(session_duration)/3600.0 AS ttl_duration, 'Dev' AS schema_
# MAGIC FROM dev.detection.viewing_content_firehose
# MAGIC WHERE session_start >= CURRENT_DATE - 2
# MAGIC AND partition_key >= DATE(CURRENT_DATE - 2)
# MAGIC GROUP BY 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DATE(session_start), COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DATE(session_start), COUNT(*)
# MAGIC FROM prod.historic.viewing_content_firehose
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT mapped_vendor_station_id, COUNT(*) FROM detection.inscape_station_map GROUP BY 1 HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM detection.inscape_station_map WHERE mapped_vendor_station_id IN (95090, 118100, 91362, 112770, 89433, 114871, 91285, 92211, 118928, 93118, 91075, 124374)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM detection.inscape_station_map WHERE inscape_station_id IN (203869, 372, 203871, 2512, 594, 198212, 2495, 178616, 204414, 103817, 87102, 93306, 86663, 92625, 203510, 102207, 203512, 113985, 203513, 107880, 203514, 105804, 2756, 183302)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.scripps_test_call_sign_specific;
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.scripps_test_call_sign_specific (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id_tms string, show_title_tms string, air_date_tms string, channel_callsign_tms string, mt_start_tms integer, ts_start timestamp, ts_end timestamp, channel_affiliate_tms string, live_tms string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.scripps_test_call_sign_specific (
# MAGIC     tvid, 
# MAGIC     hash, 
# MAGIC     zipcode, 
# MAGIC     dma, 
# MAGIC     episode_id_tms, 
# MAGIC     show_title_tms, 
# MAGIC     air_date_tms, 
# MAGIC     channel_callsign_tms, 
# MAGIC     mt_start_tms, 
# MAGIC     ts_start, 
# MAGIC     ts_end, 
# MAGIC     channel_affiliate_tms, 
# MAGIC     live_tms, 
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
# MAGIC         AND override.client_name = 'scripps'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'scripps'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT vendor_station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT vendor_station_id AS station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%scripps%'
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
# MAGIC              ELSE vizio_program.program_tms_id 
# MAGIC         END
# MAGIC     ELSE
# MAGIC         CASE WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC             WHEN (cl.client_id is not null) THEN NULL
# MAGIC         ELSE
# MAGIC             CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC                 WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC                 WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
# MAGIC                 ELSE show.database_key 
# MAGIC             END
# MAGIC         END
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC         CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC             WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
# MAGIC                 THEN vizio_program.series_aggregate_title
# MAGIC                 ELSE vizio_program.title END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC     WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'scripps' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
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
# MAGIC         WHEN 'scripps' != 'nielsen' THEN COALESCE(c.tms_airdate, c.airdate)
# MAGIC         WHEN 'scripps' != 'nielsen' THEN c.tms_airdate
# MAGIC         ELSE NULL
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC         WHEN (cl.client_id IS NOT NULL) THEN NUll
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'scripps' != 'nielsen' THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC         WHEN 'scripps' = 'nielsen' AND c.tms_airdate IS NOT NULL THEN map.inscape_call_sign 
# MAGIC         ELSE NULL 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN 'scripps' != 'nielsen' THEN LEAST(c.media_time_start, c.runtime)
# MAGIC         WHEN 'scripps' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC         ELSE LEAST(c.media_time_start, c.runtime) 
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC     CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED'
# MAGIC         ELSE vizio_station.name END
# MAGIC     WHEN acrb.app_name IS NOT NULL THEN NULL
# MAGIC     WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC     ELSE
# MAGIC         CASE 
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC             WHEN station_obfs.station_id IS NOT NULL THEN NULL 
# MAGIC             WHEN 'scripps' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
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
# MAGIC         WHEN 'scripps' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
# MAGIC         WHEN 'scripps' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     ip.ip_address, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC         CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC             WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC             WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC             WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC             WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC             ELSE tis.app_name END
# MAGIC         WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC     END
# MAGIC FROM
# MAGIC     dev.detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-08-27T18:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-27T19:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-27T19:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-27T18:00:00'::timestamp
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
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS map
# MAGIC         ON map.mapped_vendor_station_id = c.tms_station_id
# MAGIC         AND map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS station
# MAGIC         ON station.station_id = c.tms_station_id
# MAGIC         AND station.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS show
# MAGIC         ON show.show_id = c.tms_show_id
# MAGIC         AND show.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = c.fk_show_id
# MAGIC         AND c.tms_show_id IS NULL
# MAGIC        AND backup_show.vendor_name = 'TIVO'
# MAGIC        AND 'scripps' != 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS backup_station
# MAGIC         ON backup_station.station_id = c.fk_station_id
# MAGIC        AND c.tms_station_id IS NULL
# MAGIC        AND backup_station.vendor_name = 'TIVO'
# MAGIC        AND 'scripps' != 'nielsen'
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS backup_map
# MAGIC         ON backup_map.mapped_vendor_station_id = c.fk_station_id
# MAGIC         AND c.tms_station_id IS NULL
# MAGIC         AND backup_map.mapped_vendor = 'TIVO'
# MAGIC         AND 'scripps' != 'nielsen'
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
# MAGIC         AND cl.client_name <> 'scripps'
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'scripps'
# MAGIC     JOIN prod.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-08-27T19:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-27T18:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-08-27T19:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-27T18:00:00'::timestamp::double)::timestamp 
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
# MAGIC         AND ip.create_timestamp <= '2024-08-27T19:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-08-27T18:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = COALESCE(map.inscape_station_id, backup_map.inscape_station_id)
# MAGIC         AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'scripps' != 'nielsen'  
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND COALESCE(c.airdate, c.tms_airdate) = rep_local.airdate
# MAGIC         AND COALESCE(c.fk_show_id, c.tms_show_id) = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC         AND 'scripps' != 'nielsen'
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND COALESCE(c.airdate, c.tms_airdate) = rep_nyc_nat.airdate
# MAGIC         AND COALESCE(c.fk_show_id, c.tms_show_id) = rep_nyc_nat.fk_show_id 
# MAGIC         AND 'scripps' != 'nielsen'
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-27T18:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-08-27T19:00:00'::timestamp
# MAGIC     AND c.partition_key = '2024-08-27'
# MAGIC     AND MOD(c.fk_tvid, 10) = 0
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, backup_station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
# MAGIC     AND cl.client_id IS NULL
# MAGIC     AND (cid.content_cid <> 'unknown' OR vizio_station.name IS NOT NULL)
# MAGIC     AND CASE WHEN tvis.category = 'APPS' AND vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     AND CASE WHEN COALESCE(c.fk_station_id, c.tms_station_id) IS NOT NULL AND station_blacklist.station_id IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC      AND CASE
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'scripps' != 'nielsen' THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
# MAGIC         WHEN 'scripps' = 'nielsen' THEN map.inscape_call_sign 
# MAGIC         ELSE NULL 
# MAGIC      END in ('BBCAHD','BBCAPH','CNBCHD','CNNHD','FBNHD','FBNP','FNCHD','FUSIONH','HBOHD','HBOHDP','HLNHD','MNBCHD','NEWSY','NEWSYSD','NEWSMXH')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COALESCE(channel_callsign_tms, 'n/a'), COUNT(*)
# MAGIC FROM dev.mohit_gangwani.scripps_test_call_sign_specific
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM detection.dma
# MAGIC WHERE CASE WHEN dma_id >= 500 THEN NULL
# MAGIC            ELSE dma_id END IN (20, 43, 22)
