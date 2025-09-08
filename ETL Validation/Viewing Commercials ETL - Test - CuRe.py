# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.all_commercial_feed_comscore_existing_table;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.all_commercial_feed_comscore_existing_table (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip_null string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.all_commercial_feed_comscore_existing_table (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip_null, input_category, input_device, app_service
# MAGIC )
# MAGIC WITH clients_table_to_join AS (
# MAGIC     SELECT *
# MAGIC     FROM prod.detection.clients 
# MAGIC     WHERE 
# MAGIC         client_name in ('kinetiq', 'comscore')
# MAGIC )
# MAGIC ,nielsen_replacement_national_nyc_alias AS (
# MAGIC     SELECT station_id, fk_show_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM prod.detection.nielsen_replacement_national_nyc
# MAGIC     GROUP BY 1,2,3,4
# MAGIC )
# MAGIC ,nielsen_replacement_local_alias AS (
# MAGIC     SELECT station_id, fk_show_id, dma_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM prod.detection.nielsen_replacement_local
# MAGIC     GROUP BY 1,2,3,4,5
# MAGIC )
# MAGIC , activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'comscore'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'comscore'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC ),
# MAGIC epg_schedule_latest AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM prod.detection.epg_schedule_latest sch
# MAGIC     WHERE sch.airdate >= '2024-08-10T07:00:00'::timestamp - interval '60' day
# MAGIC       AND sch.airdate <= '2024-08-10T07:00:00'::timestamp + interval '1' day
# MAGIC ),
# MAGIC epg_program_aggregate AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM prod.detection.vizio_epg_program_aggregate 
# MAGIC ),
# MAGIC viewing_content_firehose AS (
# MAGIC     SELECT DISTINCT fk_tvid, fk_station_id, media_time_start, session_start, session_end, airdate, fk_content_id, is_live
# MAGIC     FROM prod.detection.viewing_content_firehose AS content
# MAGIC     WHERE session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND session_start < '2024-08-12T12:00:00'::timestamp
# MAGIC ),
# MAGIC content_ids_firehose AS (
# MAGIC     SELECT * FROM detection.content_ids_firehose AS cid
# MAGIC     WHERE content_id IN (
# MAGIC         SELECT DISTINCT c.fk_content_id
# MAGIC         FROM prod.detection.viewing_content_firehose AS c
# MAGIC         WHERE c.session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC             AND c.session_start <= '2024-08-12T12:00:00'::timestamp
# MAGIC     )
# MAGIC ), station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%comscore%'
# MAGIC )
# MAGIC SELECT 
# MAGIC /*+ BROADCAST(m), 
# MAGIC   BROADCAST(cl), 
# MAGIC   BROADCAST(tp), 
# MAGIC   BROADCAST(pop),
# MAGIC   BROADCAST(location), 
# MAGIC   BROADCAST(epg_program_aggregate), 
# MAGIC   BROADCAST(prev_cid), 
# MAGIC   BROADCAST(next_cid), 
# MAGIC   BROADCAST(next_map),
# MAGIC   BROADCAST(next_vizio_station),
# MAGIC   BROADCAST(next_vizio_program),
# MAGIC   BROADCAST(prev_vizio_station),
# MAGIC   BROADCAST(prev_vizio_program),
# MAGIC   BROADCAST(next_schedule),
# MAGIC   BROADCAST(next_program),
# MAGIC   BROADCAST(next_station),
# MAGIC   BROADCAST(next_schedule),
# MAGIC   BROADCAST(next_program),
# MAGIC   BROADCAST(next_program_alt),
# MAGIC   BROADCAST(prev_program_alt),
# MAGIC   BROADCAST(prev_map),
# MAGIC   BROADCAST(prev_station),
# MAGIC   BROADCAST(prev_schedule),
# MAGIC   BROADCAST(prev_program),
# MAGIC   RANGE_JOIN(next_schedule, 604800),
# MAGIC   RANGE_JOIN(prev_schedule, 604800) */
# MAGIC DISTINCT 
# MAGIC     COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
# MAGIC     '', 
# MAGIC     NULLIF(location.zipcode, ''), 
# MAGIC     REPLACE(dma.dma_name, ',', ''), 
# MAGIC     m.external_id, 
# MAGIC     c.media_time_start, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     NULLIF(CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND prev_schedule.fk_show_id IS NULL THEN NULL
# MAGIC  WHEN (prev_station_id = 0) THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0]) 
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  WHEN prev_program.database_key IS NOT NULL THEN prev_program.database_key 
# MAGIC  WHEN 'TIVO' = 'TMS' AND prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
# MAGIC  ELSE NULL
# MAGIC  END,''), 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN (prev_station_id = 0) THEN prev_filecontent.title
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  ELSE REPLACE(COALESCE(prev_program.title, prev_program_alt.title,
# MAGIC  CASE WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
# MAGIC  ELSE prev_vizio_program.title
# MAGIC  END), ',', '')
# MAGIC  END, 
# MAGIC     c.prev_session_start, 
# MAGIC     c.prev_session_end, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  WHEN c.prev_station_id IS NOT NULL THEN prev_map.inscape_call_sign
# MAGIC  WHEN (prev_station_id = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL
# MAGIC  WHEN (prev_station.inscape_station_name IS NOT NULL)
# MAGIC  THEN prev_station.inscape_station_name
# MAGIC  WHEN (LOWER(prev_station.station_affil) LIKE '%affiliate%' OR LOWER(prev_station.station_affil) LIKE '%independent%' OR LOWER(prev_station.station_affil) LIKE '%low power%')
# MAGIC  THEN prev_station.station_affil
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
# MAGIC  WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     NULLIF(CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC  WHEN c.next_station_id IS NOT NULL AND next_schedule.fk_show_id IS NULL THEN NULL
# MAGIC  WHEN (next_station_id = 0) THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0]) 
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  WHEN next_program.database_key IS NOT NULL THEN next_program.database_key
# MAGIC  WHEN 'TIVO' = 'TMS' AND next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
# MAGIC  ELSE NULL
# MAGIC  END,''), 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN (next_station_id = 0) THEN next_filecontent.title
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  ELSE REPLACE(COALESCE(next_program.title, next_program_alt.title,
# MAGIC  CASE WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
# MAGIC  ELSE next_vizio_program.title
# MAGIC  END), ',', '')
# MAGIC  END, 
# MAGIC     c.next_session_start, 
# MAGIC     c.next_session_end, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.next_station_id IS NOT NULL THEN
# MAGIC  CASE WHEN NVL(next_program.show_id, next_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  ELSE next_map.inscape_call_sign END
# MAGIC  WHEN (next_station_id = 0) THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.next_station_id IS NOT NULL AND NVL(next_program.show_id, next_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  WHEN (next_station.inscape_station_name IS NOT NULL)
# MAGIC  THEN next_station.inscape_station_name
# MAGIC  WHEN (LOWER(next_station.station_affil) LIKE '%affiliate%' OR LOWER(next_station.station_affil) LIKE '%independent%' OR LOWER(next_station.station_affil) LIKE '%low power%')
# MAGIC  THEN next_station.station_affil
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
# MAGIC  WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED'
# MAGIC  AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL
# MAGIC  ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC  END, 
# MAGIC     REPLACE(m.brand_name, ',', ''), 
# MAGIC     REPLACE(m.title, ',', ''), 
# MAGIC     m.duration, 
# MAGIC     NULL AS ip, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC             CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC                 WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC                 WHEN LOWER(coalesce(tis.app_name)) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND prev_show_id IS NOT NULL THEN NULL
# MAGIC                 WHEN LOWER(coalesce(tis.app_name)) = 'unknown' THEN NULL
# MAGIC                 ELSE coalesce(tis.app_name) END
# MAGIC             WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC         END
# MAGIC     FROM detection.viewing_commercials_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z 
# MAGIC         ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     INNER JOIN
# MAGIC         prod.detection.tv AS tv
# MAGIC         ON c.fk_tvid = tv.tvid
# MAGIC         AND tv.oem = 'VIZIO'
# MAGIC     JOIN
# MAGIC         prod.detection.tv_populations AS tp
# MAGIC         ON c.fk_tvid = tp.fk_tvid 
# MAGIC     JOIN
# MAGIC         prod.detection.populations AS pop
# MAGIC         ON tp.fk_population_id = pop.population_id 
# MAGIC         AND LOWER(pop.population_name) = 'opted_in'
# MAGIC     JOIN
# MAGIC         prod.detection.commercial_id_external_firehose AS m
# MAGIC         ON c.fk_commercial_id = m.fk_commercial_id
# MAGIC     JOIN
# MAGIC         clients_table_to_join cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC     JOIN
# MAGIC         prod.detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     JOIN 
# MAGIC         prod.detection.tv_input_stats_firehose  tvis    
# MAGIC         ON c.session_start >= tvis.create_timestamp 
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-08-12T12:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tvis.fk_tvid   
# MAGIC         AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     JOIN
# MAGIC         prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-12T12:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-12T07:00:00'::timestamp
# MAGIC     JOIN 
# MAGIC         prod.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     LEFT OUTER JOIN
# MAGIC         prod.detection.dma AS dma ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
# MAGIC         ON c.prev_vizio_epg_station = prev_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
# MAGIC         ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS next_vizio_station 
# MAGIC         ON c.next_vizio_epg_station = next_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
# MAGIC         ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS prev_content 
# MAGIC         ON c.fk_tvid = prev_content.fk_tvid
# MAGIC         AND prev_content.session_start = c.prev_session_start
# MAGIC     LEFT OUTER JOIN prod.detection.inscape_station_map AS prev_map
# MAGIC         ON prev_map.inscape_station_id = c.prev_station_id
# MAGIC         AND prev_map.mapped_vendor = 'TIVO'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS prev_station  
# MAGIC         ON prev_map.mapped_vendor_station_id = prev_station.station_id  
# MAGIC         AND prev_station.vendor_name = prev_map.mapped_vendor 
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
# MAGIC         ON c.prev_station_id = prev_station_blacklist.station_id
# MAGIC         AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS prev_station_obfs
# MAGIC         ON c.prev_station_id = prev_station_obfs.station_id
# MAGIC         AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS prev_schedule 
# MAGIC         ON prev_map.mapped_vendor_station_id= prev_schedule.fk_station_id
# MAGIC         AND prev_station.vendor_name = prev_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) >= prev_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) < prev_schedule.airdate_end
# MAGIC         AND prev_schedule.airdate >= '2024-08-12T07:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS prev_program
# MAGIC         ON prev_schedule.fk_show_id = prev_program.show_id
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS prev_program_alt
# MAGIC         ON c.prev_show_id = prev_program_alt.show_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS prev_filecontent 
# MAGIC         ON c.prev_show_id = prev_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS next_filecontent 
# MAGIC         ON c.next_show_id = next_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m_filter 
# MAGIC         ON m_filter.fk_content_id = prev_content.fk_content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as prev_cid on c.prev_show_id = prev_cid.content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as next_cid on c.next_show_id = next_cid.content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cl2
# MAGIC         ON m_filter.fk_client_id = cl2.client_id
# MAGIC         AND cl2.client_name not in ('kinetiq', 'comscore')
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS next_content 
# MAGIC         ON c.fk_tvid = next_content.fk_tvid
# MAGIC         AND next_content.session_start = c.next_session_start
# MAGIC         AND next_content.airdate IS NOT NULL
# MAGIC     LEFT OUTER JOIN prod.detection.inscape_station_map AS next_map
# MAGIC         ON next_map.inscape_station_id = c.next_station_id
# MAGIC         AND next_map.mapped_vendor = 'TIVO' 
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS next_station 
# MAGIC         ON next_map.mapped_vendor_station_id = next_station.station_id 
# MAGIC         AND next_station.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
# MAGIC         ON c.next_station_id = next_station_blacklist.station_id
# MAGIC         AND next_station_blacklist.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS next_station_obfs
# MAGIC         ON c.next_station_id = next_station_obfs.station_id
# MAGIC         AND next_station_obfs.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS next_schedule 
# MAGIC         ON next_map.mapped_vendor_station_id= next_schedule.fk_station_id 
# MAGIC         AND next_station.vendor_name = next_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) >= next_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) < next_schedule.airdate_end
# MAGIC         AND next_schedule.airdate >= '2024-08-12T07:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS next_program
# MAGIC         ON next_schedule.fk_show_id = next_program.show_id
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS next_program_alt
# MAGIC         ON c.next_show_id = next_program_alt.show_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis    
# MAGIC         ON  c.session_start >=  (tis.create_timestamp::double)::timestamp
# MAGIC         AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND tis.create_timestamp <= ('2024-08-12T12:00:00'::timestamp::double)::timestamp
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-12T07:00:00'::timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid 
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN activity_obfuscation appb 
# MAGIC         ON coalesce(tis.app_name) = appb.app_name 
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb
# MAGIC         ON coalesce(tis.app_name) = acrb.app_name
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist prev_chanb 
# MAGIC         ON prev_vizio_station.name = prev_chanb.channel_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist next_chanb 
# MAGIC         ON next_vizio_station.name = next_chanb.channel_name
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS prev_nielsen_blacklist
# MAGIC         ON prev_nielsen_blacklist.station_id = prev_map.inscape_station_id 
# MAGIC         AND c.session_start >= prev_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < prev_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS next_nielsen_blacklist
# MAGIC         ON next_nielsen_blacklist.station_id = next_map.inscape_station_id 
# MAGIC         AND c.session_start >= next_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < next_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_prev
# MAGIC         ON prev_map.inscape_station_id  = rep_local_prev.station_id
# MAGIC         AND c.prev_show_id = rep_local_prev.fk_show_id
# MAGIC         AND c.fk_dma_id = rep_local_prev.dma_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_prev
# MAGIC         ON prev_map.inscape_station_id  = rep_nyc_nat_prev.station_id
# MAGIC         AND c.prev_show_id = rep_nyc_nat_prev.fk_show_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_next
# MAGIC         ON next_map.inscape_station_id  = rep_local_next.station_id
# MAGIC         AND c.next_show_id = rep_local_next.fk_show_id
# MAGIC         AND c.fk_dma_id = rep_local_next.dma_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_next
# MAGIC         ON next_map.inscape_station_id  = rep_nyc_nat_next.station_id
# MAGIC         AND c.next_show_id = rep_nyc_nat_next.fk_show_id
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-12T12:00:00'::timestamp
# MAGIC         AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC         AND c.partition_key = '2024-08-12'
# MAGIC         AND CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.all_commercial_feed_comscore_existing_table_tms;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.all_commercial_feed_comscore_existing_table_tms (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip_null string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.all_commercial_feed_comscore_existing_table_tms (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip_null, input_category, input_device, app_service
# MAGIC )
# MAGIC WITH clients_table_to_join AS (
# MAGIC     SELECT *
# MAGIC     FROM prod.detection.clients 
# MAGIC     WHERE 
# MAGIC         client_name in ('kinetiq', 'comscore')
# MAGIC )
# MAGIC ,nielsen_replacement_national_nyc_alias AS (
# MAGIC     SELECT station_id, fk_show_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM prod.detection.nielsen_replacement_national_nyc
# MAGIC     GROUP BY 1,2,3,4
# MAGIC )
# MAGIC ,nielsen_replacement_local_alias AS (
# MAGIC     SELECT station_id, fk_show_id, dma_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM prod.detection.nielsen_replacement_local
# MAGIC     GROUP BY 1,2,3,4,5
# MAGIC )
# MAGIC , activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'comscore'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'comscore'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC ),
# MAGIC epg_schedule_latest AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM prod.detection.epg_schedule_latest sch
# MAGIC     WHERE sch.airdate >= '2024-08-12T07:00:00'::timestamp - interval '60' day
# MAGIC       AND sch.airdate <= '2024-08-12T07:00:00'::timestamp + interval '1' day
# MAGIC ),
# MAGIC epg_program_aggregate AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM prod.detection.vizio_epg_program_aggregate 
# MAGIC ),
# MAGIC viewing_content_firehose AS (
# MAGIC     SELECT DISTINCT fk_tvid, fk_station_id, media_time_start, session_start, session_end, airdate, fk_content_id, is_live
# MAGIC     FROM prod.detection.viewing_content_firehose AS content
# MAGIC     WHERE session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND session_start < '2024-08-12T12:00:00'::timestamp
# MAGIC ),
# MAGIC content_ids_firehose AS (
# MAGIC     SELECT * FROM detection.content_ids_firehose AS cid
# MAGIC     WHERE content_id IN (
# MAGIC         SELECT DISTINCT c.fk_content_id
# MAGIC         FROM prod.detection.viewing_content_firehose AS c
# MAGIC         WHERE c.session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC             AND c.session_start <= '2024-08-12T12:00:00'::timestamp
# MAGIC     )
# MAGIC ), station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%comscore%'
# MAGIC )
# MAGIC SELECT 
# MAGIC /*+ BROADCAST(m), 
# MAGIC   BROADCAST(cl), 
# MAGIC   BROADCAST(tp), 
# MAGIC   BROADCAST(pop),
# MAGIC   BROADCAST(location), 
# MAGIC   BROADCAST(epg_program_aggregate), 
# MAGIC   BROADCAST(prev_cid), 
# MAGIC   BROADCAST(next_cid), 
# MAGIC   BROADCAST(next_map),
# MAGIC   BROADCAST(next_vizio_station),
# MAGIC   BROADCAST(next_vizio_program),
# MAGIC   BROADCAST(prev_vizio_station),
# MAGIC   BROADCAST(prev_vizio_program),
# MAGIC   BROADCAST(next_schedule),
# MAGIC   BROADCAST(next_program),
# MAGIC   BROADCAST(next_station),
# MAGIC   BROADCAST(next_schedule),
# MAGIC   BROADCAST(next_program),
# MAGIC   BROADCAST(next_program_alt),
# MAGIC   BROADCAST(prev_program_alt),
# MAGIC   BROADCAST(prev_map),
# MAGIC   BROADCAST(prev_station),
# MAGIC   BROADCAST(prev_schedule),
# MAGIC   BROADCAST(prev_program),
# MAGIC   RANGE_JOIN(next_schedule, 604800),
# MAGIC   RANGE_JOIN(prev_schedule, 604800) */
# MAGIC DISTINCT 
# MAGIC     COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
# MAGIC     '', 
# MAGIC     NULLIF(location.zipcode, ''), 
# MAGIC     REPLACE(dma.dma_name, ',', ''), 
# MAGIC     m.external_id, 
# MAGIC     c.media_time_start, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     NULLIF(CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND prev_schedule.fk_show_id IS NULL THEN NULL
# MAGIC  WHEN (prev_station_id = 0) THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0]) 
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  WHEN prev_program.database_key IS NOT NULL THEN prev_program.database_key 
# MAGIC  WHEN 'TMS' = 'TMS' AND prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
# MAGIC  ELSE NULL
# MAGIC  END,''), 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN (prev_station_id = 0) THEN prev_filecontent.title
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  ELSE REPLACE(COALESCE(prev_program.title, prev_program_alt.title,
# MAGIC  CASE WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
# MAGIC  ELSE prev_vizio_program.title
# MAGIC  END), ',', '')
# MAGIC  END, 
# MAGIC     c.prev_session_start, 
# MAGIC     c.prev_session_end, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  WHEN c.prev_station_id IS NOT NULL THEN prev_map.inscape_call_sign
# MAGIC  WHEN (prev_station_id = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL
# MAGIC  WHEN (prev_station.inscape_station_name IS NOT NULL)
# MAGIC  THEN prev_station.inscape_station_name
# MAGIC  WHEN (LOWER(prev_station.station_affil) LIKE '%affiliate%' OR LOWER(prev_station.station_affil) LIKE '%independent%' OR LOWER(prev_station.station_affil) LIKE '%low power%')
# MAGIC  THEN prev_station.station_affil
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
# MAGIC  WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     NULLIF(CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC  WHEN c.next_station_id IS NOT NULL AND next_schedule.fk_show_id IS NULL THEN NULL
# MAGIC  WHEN (next_station_id = 0) THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0]) 
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  WHEN next_program.database_key IS NOT NULL THEN next_program.database_key
# MAGIC  WHEN 'TMS' = 'TMS' AND next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
# MAGIC  ELSE NULL
# MAGIC  END,''), 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN (next_station_id = 0) THEN next_filecontent.title
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  ELSE REPLACE(COALESCE(next_program.title, next_program_alt.title,
# MAGIC  CASE WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
# MAGIC  ELSE next_vizio_program.title
# MAGIC  END), ',', '')
# MAGIC  END, 
# MAGIC     c.next_session_start, 
# MAGIC     c.next_session_end, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.next_station_id IS NOT NULL THEN
# MAGIC  CASE WHEN NVL(next_program.show_id, next_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  ELSE next_map.inscape_call_sign END
# MAGIC  WHEN (next_station_id = 0) THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.next_station_id IS NOT NULL AND NVL(next_program.show_id, next_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  WHEN (next_station.inscape_station_name IS NOT NULL)
# MAGIC  THEN next_station.inscape_station_name
# MAGIC  WHEN (LOWER(next_station.station_affil) LIKE '%affiliate%' OR LOWER(next_station.station_affil) LIKE '%independent%' OR LOWER(next_station.station_affil) LIKE '%low power%')
# MAGIC  THEN next_station.station_affil
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
# MAGIC  WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED'
# MAGIC  AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL
# MAGIC  ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC  END, 
# MAGIC     REPLACE(m.brand_name, ',', ''), 
# MAGIC     REPLACE(m.title, ',', ''), 
# MAGIC     m.duration, 
# MAGIC     NULL AS ip, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC             CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC                 WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC                 WHEN LOWER(coalesce(tis.app_name)) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND prev_show_id IS NOT NULL THEN NULL
# MAGIC                 WHEN LOWER(coalesce(tis.app_name)) = 'unknown' THEN NULL
# MAGIC                 ELSE coalesce(tis.app_name) END
# MAGIC             WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC         END
# MAGIC     FROM detection.viewing_commercials_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z 
# MAGIC         ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     INNER JOIN
# MAGIC         prod.detection.tv AS tv
# MAGIC         ON c.fk_tvid = tv.tvid
# MAGIC         AND tv.oem = 'VIZIO'
# MAGIC     JOIN
# MAGIC         prod.detection.tv_populations AS tp
# MAGIC         ON c.fk_tvid = tp.fk_tvid 
# MAGIC     JOIN
# MAGIC         prod.detection.populations AS pop
# MAGIC         ON tp.fk_population_id = pop.population_id 
# MAGIC         AND LOWER(pop.population_name) = 'opted_in'
# MAGIC     JOIN
# MAGIC         prod.detection.commercial_id_external_firehose AS m
# MAGIC         ON c.fk_commercial_id = m.fk_commercial_id
# MAGIC     JOIN
# MAGIC         clients_table_to_join cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC     JOIN
# MAGIC         prod.detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     JOIN 
# MAGIC         prod.detection.tv_input_stats_firehose  tvis    
# MAGIC         ON c.session_start >= tvis.create_timestamp 
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-08-12T12:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tvis.fk_tvid   
# MAGIC         AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     JOIN
# MAGIC         prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-12T12:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-12T07:00:00'::timestamp
# MAGIC     JOIN 
# MAGIC         prod.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     LEFT OUTER JOIN
# MAGIC         prod.detection.dma AS dma ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
# MAGIC         ON c.prev_vizio_epg_station = prev_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
# MAGIC         ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS next_vizio_station 
# MAGIC         ON c.next_vizio_epg_station = next_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
# MAGIC         ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS prev_content 
# MAGIC         ON c.fk_tvid = prev_content.fk_tvid
# MAGIC         AND prev_content.session_start = c.prev_session_start
# MAGIC     LEFT OUTER JOIN prod.detection.inscape_station_map AS prev_map
# MAGIC         ON prev_map.inscape_station_id = c.prev_station_id
# MAGIC         AND prev_map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS prev_station  
# MAGIC         ON prev_map.mapped_vendor_station_id = prev_station.station_id  
# MAGIC         AND prev_station.vendor_name = prev_map.mapped_vendor 
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
# MAGIC         ON c.prev_station_id = prev_station_blacklist.station_id
# MAGIC         AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS prev_station_obfs
# MAGIC         ON c.prev_station_id = prev_station_obfs.station_id
# MAGIC         AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS prev_schedule 
# MAGIC         ON prev_map.mapped_vendor_station_id= prev_schedule.fk_station_id
# MAGIC         AND prev_station.vendor_name = prev_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) >= prev_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) < prev_schedule.airdate_end
# MAGIC         AND prev_schedule.airdate >= '2024-08-12T07:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS prev_program
# MAGIC         ON prev_schedule.fk_show_id = prev_program.show_id
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS prev_program_alt
# MAGIC         ON c.prev_show_id = prev_program_alt.show_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS prev_filecontent 
# MAGIC         ON c.prev_show_id = prev_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS next_filecontent 
# MAGIC         ON c.next_show_id = next_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m_filter 
# MAGIC         ON m_filter.fk_content_id = prev_content.fk_content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as prev_cid on c.prev_show_id = prev_cid.content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as next_cid on c.next_show_id = next_cid.content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cl2
# MAGIC         ON m_filter.fk_client_id = cl2.client_id
# MAGIC         AND cl2.client_name not in ('kinetiq', 'comscore')
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS next_content 
# MAGIC         ON c.fk_tvid = next_content.fk_tvid
# MAGIC         AND next_content.session_start = c.next_session_start
# MAGIC         AND next_content.airdate IS NOT NULL
# MAGIC     LEFT OUTER JOIN prod.detection.inscape_station_map AS next_map
# MAGIC         ON next_map.inscape_station_id = c.next_station_id
# MAGIC         AND next_map.mapped_vendor = 'TMS' 
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS next_station 
# MAGIC         ON next_map.mapped_vendor_station_id = next_station.station_id 
# MAGIC         AND next_station.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
# MAGIC         ON c.next_station_id = next_station_blacklist.station_id
# MAGIC         AND next_station_blacklist.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS next_station_obfs
# MAGIC         ON c.next_station_id = next_station_obfs.station_id
# MAGIC         AND next_station_obfs.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS next_schedule 
# MAGIC         ON next_map.mapped_vendor_station_id= next_schedule.fk_station_id 
# MAGIC         AND next_station.vendor_name = next_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) >= next_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) < next_schedule.airdate_end
# MAGIC         AND next_schedule.airdate >= '2024-08-12T07:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS next_program
# MAGIC         ON next_schedule.fk_show_id = next_program.show_id
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS next_program_alt
# MAGIC         ON c.next_show_id = next_program_alt.show_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis    
# MAGIC         ON  c.session_start >=  (tis.create_timestamp::double)::timestamp
# MAGIC         AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND tis.create_timestamp <= ('2024-08-12T12:00:00'::timestamp::double)::timestamp
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-12T07:00:00'::timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid 
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN activity_obfuscation appb 
# MAGIC         ON coalesce(tis.app_name) = appb.app_name 
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb
# MAGIC         ON coalesce(tis.app_name) = acrb.app_name
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist prev_chanb 
# MAGIC         ON prev_vizio_station.name = prev_chanb.channel_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist next_chanb 
# MAGIC         ON next_vizio_station.name = next_chanb.channel_name
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS prev_nielsen_blacklist
# MAGIC         ON prev_nielsen_blacklist.station_id = prev_map.inscape_station_id 
# MAGIC         AND c.session_start >= prev_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < prev_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS next_nielsen_blacklist
# MAGIC         ON next_nielsen_blacklist.station_id = next_map.inscape_station_id 
# MAGIC         AND c.session_start >= next_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < next_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_prev
# MAGIC         ON prev_map.inscape_station_id  = rep_local_prev.station_id
# MAGIC         AND c.prev_show_id = rep_local_prev.fk_show_id
# MAGIC         AND c.fk_dma_id = rep_local_prev.dma_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_prev
# MAGIC         ON prev_map.inscape_station_id  = rep_nyc_nat_prev.station_id
# MAGIC         AND c.prev_show_id = rep_nyc_nat_prev.fk_show_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_next
# MAGIC         ON next_map.inscape_station_id  = rep_local_next.station_id
# MAGIC         AND c.next_show_id = rep_local_next.fk_show_id
# MAGIC         AND c.fk_dma_id = rep_local_next.dma_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_next
# MAGIC         ON next_map.inscape_station_id  = rep_nyc_nat_next.station_id
# MAGIC         AND c.next_show_id = rep_nyc_nat_next.fk_show_id
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-12T12:00:00'::timestamp
# MAGIC         AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC         AND c.partition_key = '2024-08-12'
# MAGIC         AND CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.all_commercial_feed_comscore_new_table;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.all_commercial_feed_comscore_new_table (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip_null string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.all_commercial_feed_comscore_new_table (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip_null, input_category, input_device, app_service
# MAGIC )
# MAGIC WITH clients_table_to_join AS (
# MAGIC     SELECT *
# MAGIC     FROM prod.detection.clients 
# MAGIC     WHERE 
# MAGIC         client_name in ('kinetiq', 'comscore')
# MAGIC )
# MAGIC ,nielsen_replacement_national_nyc_alias AS (
# MAGIC     SELECT station_id, fk_show_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM prod.detection.nielsen_replacement_national_nyc
# MAGIC     GROUP BY 1,2,3,4
# MAGIC )
# MAGIC ,nielsen_replacement_local_alias AS (
# MAGIC     SELECT station_id, fk_show_id, dma_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM prod.detection.nielsen_replacement_local
# MAGIC     GROUP BY 1,2,3,4,5
# MAGIC )
# MAGIC , activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'comscore'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'comscore'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC ),
# MAGIC epg_program_aggregate AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM prod.detection.vizio_epg_program_aggregate 
# MAGIC ),
# MAGIC viewing_content_firehose AS (
# MAGIC     SELECT DISTINCT fk_tvid, fk_station_id, media_time_start, session_start, session_end, airdate, fk_content_id, is_live
# MAGIC     FROM prod.detection.viewing_content_firehose AS content
# MAGIC     WHERE session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND session_start < '2024-08-12T12:00:00'::timestamp
# MAGIC ),
# MAGIC content_ids_firehose AS (
# MAGIC     SELECT * FROM detection.content_ids_firehose AS cid
# MAGIC     WHERE content_id IN (
# MAGIC         SELECT DISTINCT c.fk_content_id
# MAGIC         FROM prod.detection.viewing_content_firehose AS c
# MAGIC         WHERE c.session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC             AND c.session_start <= '2024-08-12T12:00:00'::timestamp
# MAGIC     )
# MAGIC ), station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT vendor_station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT vendor_station_id AS station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%comscore%'
# MAGIC ),
# MAGIC inscape_map_deduped AS (
# MAGIC     SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
# MAGIC     FROM (
# MAGIC         SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
# MAGIC         FROM prod.detection.inscape_station_map) ism
# MAGIC     WHERE ism.rn = 1
# MAGIC )
# MAGIC SELECT 
# MAGIC DISTINCT 
# MAGIC     COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
# MAGIC     '', 
# MAGIC     NULLIF(location.zipcode, ''), 
# MAGIC     REPLACE(dma.dma_name, ',', ''), 
# MAGIC     m.external_id, 
# MAGIC     c.media_time_start, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     NULLIF(CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC  WHEN COALESCE(c.prev_station_id, c.tms_prev_station_id) = 0 THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0]) 
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  WHEN prev_program.database_key IS NOT NULL THEN prev_program.database_key 
# MAGIC  WHEN 'TIVO' = 'TMS' AND prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
# MAGIC  ELSE NULL
# MAGIC  END,''), 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.prev_station_id, c.tms_prev_station_id) = 0 THEN prev_filecontent.title
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  ELSE REPLACE(COALESCE(prev_program.title, prev_program_backup.title,
# MAGIC  CASE WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
# MAGIC  ELSE prev_vizio_program.title
# MAGIC  END), ',', '')
# MAGIC  END, 
# MAGIC     c.prev_session_start, 
# MAGIC     c.prev_session_end, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.prev_station_id, c.tms_prev_station_id) IS NOT NULL THEN COALESCE(prev_map.inscape_call_sign, prev_map_backup.inscape_call_sign)
# MAGIC  WHEN (prev_station_id = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.prev_station_id IS NOT NULL THEN
# MAGIC       CASE WHEN (prev_station.inscape_station_name IS NOT NULL) THEN prev_station.inscape_station_name
# MAGIC            WHEN (LOWER(prev_station.station_affil) LIKE '%affiliate%'
# MAGIC                  OR LOWER(prev_station.station_affil) LIKE '%independent%'
# MAGIC                  OR LOWER(prev_station.station_affil) LIKE '%low power%')
# MAGIC                 THEN prev_station.station_affil END
# MAGIC  WHEN c.prev_station_id IS NULL AND c.tms_prev_station_id IS NOT NULL THEN
# MAGIC       CASE WHEN (prev_station_backup.inscape_station_name IS NOT NULL) THEN prev_station_backup.inscape_station_name
# MAGIC            WHEN (LOWER(prev_station_backup.station_affil) LIKE '%affiliate%'
# MAGIC                  OR LOWER(prev_station_backup.station_affil) LIKE '%independent%'
# MAGIC                  OR LOWER(prev_station_backup.station_affil) LIKE '%low power%')
# MAGIC                 THEN prev_station_backup.station_affil END
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
# MAGIC  WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     NULLIF(CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.next_station_id, c.tms_next_station_id) = 0 THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0]) 
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  WHEN next_program.database_key IS NOT NULL THEN next_program.database_key
# MAGIC  WHEN 'TIVO' = 'TMS' AND next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
# MAGIC  ELSE NULL
# MAGIC  END,''), 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.next_station_id, c.tms_next_station_id) = 0 THEN next_filecontent.title
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  ELSE REPLACE(COALESCE(next_program.title, next_program_backup.title,
# MAGIC  CASE WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
# MAGIC  ELSE next_vizio_program.title
# MAGIC  END), ',', '')
# MAGIC  END, 
# MAGIC     c.next_session_start, 
# MAGIC     c.next_session_end, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.next_station_id, c.tms_next_station_id) IS NOT NULL THEN COALESCE(next_map.inscape_call_sign, next_map_backup.inscape_call_sign)
# MAGIC  WHEN COALESCE(c.next_station_id, c.tms_next_station_id) = 0 THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.next_station_id IS NOT NULL THEN
# MAGIC       CASE WHEN (next_station.inscape_station_name IS NOT NULL) THEN next_station.inscape_station_name
# MAGIC            WHEN (LOWER(next_station.station_affil) LIKE '%affiliate%'
# MAGIC                  OR LOWER(next_station.station_affil) LIKE '%independent%'
# MAGIC                  OR LOWER(next_station.station_affil) LIKE '%low power%')
# MAGIC                 THEN next_station.station_affil END
# MAGIC  WHEN c.next_station_id IS NULL AND c.tms_next_station_id IS NOT NULL THEN
# MAGIC       CASE WHEN (next_station_backup.inscape_station_name IS NOT NULL) THEN next_station_backup.inscape_station_name
# MAGIC            WHEN (LOWER(next_station_backup.station_affil) LIKE '%affiliate%'
# MAGIC                  OR LOWER(next_station_backup.station_affil) LIKE '%independent%'
# MAGIC                  OR LOWER(next_station_backup.station_affil) LIKE '%low power%')
# MAGIC                 THEN next_station_backup.station_affil END
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
# MAGIC  WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED'
# MAGIC  AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC  END, 
# MAGIC     REPLACE(m.brand_name, ',', ''), 
# MAGIC     REPLACE(m.title, ',', ''), 
# MAGIC     m.duration, 
# MAGIC     NULL AS ip, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC             CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC                 WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC                 WHEN LOWER(coalesce(tis.app_name)) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND prev_show_id IS NOT NULL THEN NULL
# MAGIC                 WHEN LOWER(coalesce(tis.app_name)) = 'unknown' THEN NULL
# MAGIC                 ELSE coalesce(tis.app_name) END
# MAGIC             WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC         END
# MAGIC     FROM dev.detection.viewing_commercials_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z 
# MAGIC         ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     INNER JOIN
# MAGIC         prod.detection.tv AS tv
# MAGIC         ON c.fk_tvid = tv.tvid
# MAGIC         AND tv.oem = 'VIZIO'
# MAGIC     JOIN
# MAGIC         prod.detection.tv_populations AS tp
# MAGIC         ON c.fk_tvid = tp.fk_tvid 
# MAGIC     JOIN
# MAGIC         prod.detection.populations AS pop
# MAGIC         ON tp.fk_population_id = pop.population_id 
# MAGIC         AND LOWER(pop.population_name) = 'opted_in'
# MAGIC     JOIN
# MAGIC         prod.detection.commercial_id_external_firehose AS m
# MAGIC         ON c.fk_commercial_id = m.fk_commercial_id
# MAGIC     JOIN
# MAGIC         clients_table_to_join cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC     JOIN
# MAGIC         prod.detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     JOIN 
# MAGIC         prod.detection.tv_input_stats_firehose  tvis    
# MAGIC         ON c.session_start >= tvis.create_timestamp 
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-08-12T12:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tvis.fk_tvid   
# MAGIC         AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     JOIN
# MAGIC         prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-12T12:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-12T07:00:00'::timestamp
# MAGIC     JOIN 
# MAGIC         prod.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     LEFT OUTER JOIN
# MAGIC         prod.detection.dma AS dma ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
# MAGIC         ON c.prev_vizio_epg_station = prev_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
# MAGIC         ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS next_vizio_station 
# MAGIC         ON c.next_vizio_epg_station = next_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
# MAGIC         ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS prev_content 
# MAGIC         ON c.fk_tvid = prev_content.fk_tvid
# MAGIC         AND prev_content.session_start = c.prev_session_start
# MAGIC
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS prev_station  
# MAGIC         ON prev_station.station_id = c.prev_station_id
# MAGIC         AND prev_station.vendor_name = 'TIVO'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS prev_station_backup
# MAGIC         ON prev_station_backup.station_id = c.tms_prev_station_id
# MAGIC         AND c.prev_station_id IS NULL
# MAGIC         AND prev_station_backup.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN inscape_map_deduped AS prev_map
# MAGIC         ON prev_map.mapped_vendor_station_id = c.prev_station_id
# MAGIC         AND prev_map.mapped_vendor = 'TIVO'
# MAGIC     LEFT OUTER JOIN inscape_map_deduped AS prev_map_backup
# MAGIC         ON prev_map_backup.mapped_vendor_station_id = c.tms_prev_station_id
# MAGIC         AND c.prev_station_id IS NULL
# MAGIC         AND prev_map_backup.mapped_vendor = 'TMS' 
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
# MAGIC         ON c.prev_station_id = prev_station_blacklist.station_id
# MAGIC         AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS prev_station_obfs
# MAGIC         ON c.prev_station_id = prev_station_obfs.vendor_station_id
# MAGIC         AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS prev_program
# MAGIC         ON c.prev_show_id = prev_program.show_id
# MAGIC        AND prev_program.vendor_name = 'TIVO'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS prev_program_backup
# MAGIC         ON c.tms_prev_show_id = prev_program_backup.show_id
# MAGIC        AND prev_program_backup.vendor_name = 'TMS'
# MAGIC         AND c.prev_show_id IS NULL
# MAGIC
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS prev_filecontent 
# MAGIC         ON c.prev_show_id = prev_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS next_filecontent 
# MAGIC         ON c.next_show_id = next_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m_filter 
# MAGIC         ON m_filter.fk_content_id = prev_content.fk_content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as prev_cid on c.prev_show_id = prev_cid.content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as next_cid on c.next_show_id = next_cid.content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cl2
# MAGIC         ON m_filter.fk_client_id = cl2.client_id
# MAGIC         AND cl2.client_name not in ('kinetiq', 'comscore')
# MAGIC
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS next_station  
# MAGIC         ON next_station.station_id = c.next_station_id
# MAGIC         AND next_station.vendor_name = 'TIVO'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS next_station_backup
# MAGIC         ON next_station_backup.station_id = c.tms_next_station_id
# MAGIC         AND c.next_station_id IS NULL
# MAGIC         AND next_station_backup.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN inscape_map_deduped AS next_map
# MAGIC         ON next_map.mapped_vendor_station_id = c.next_station_id
# MAGIC         AND next_map.mapped_vendor = 'TIVO'
# MAGIC     LEFT OUTER JOIN inscape_map_deduped AS next_map_backup
# MAGIC         ON next_map_backup.mapped_vendor_station_id = c.tms_next_station_id
# MAGIC         AND c.next_station_id IS NULL
# MAGIC         AND next_map_backup.mapped_vendor = 'TMS' 
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
# MAGIC         ON c.next_station_id = next_station_blacklist.station_id
# MAGIC         AND next_station_blacklist.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS next_station_obfs
# MAGIC         ON c.next_station_id = next_station_obfs.vendor_station_id
# MAGIC         AND next_station_obfs.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS next_program
# MAGIC         ON c.next_show_id = next_program.show_id
# MAGIC        AND next_program.vendor_name = 'TIVO'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS next_program_backup
# MAGIC         ON c.tms_next_show_id = next_program_backup.show_id
# MAGIC        AND next_program_backup.vendor_name = 'TMS'
# MAGIC         AND c.next_show_id IS NULL
# MAGIC
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >=  (tis.create_timestamp::double)::timestamp
# MAGIC         AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND tis.create_timestamp <= ('2024-08-12T12:00:00'::timestamp::double)::timestamp
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-12T07:00:00'::timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid 
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN activity_obfuscation appb 
# MAGIC         ON coalesce(tis.app_name) = appb.app_name 
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb
# MAGIC         ON coalesce(tis.app_name) = acrb.app_name
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist prev_chanb 
# MAGIC         ON prev_vizio_station.name = prev_chanb.channel_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist next_chanb 
# MAGIC         ON next_vizio_station.name = next_chanb.channel_name
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS prev_nielsen_blacklist
# MAGIC         ON prev_nielsen_blacklist.station_id = prev_map.inscape_station_id 
# MAGIC         AND c.session_start >= prev_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < prev_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS next_nielsen_blacklist
# MAGIC         ON next_nielsen_blacklist.station_id = next_map.inscape_station_id 
# MAGIC         AND c.session_start >= next_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < next_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_prev
# MAGIC         ON prev_map.inscape_station_id  = rep_local_prev.station_id
# MAGIC         AND c.prev_show_id = rep_local_prev.fk_show_id
# MAGIC         AND c.fk_dma_id = rep_local_prev.dma_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_prev
# MAGIC         ON prev_map.inscape_station_id  = rep_nyc_nat_prev.station_id
# MAGIC         AND c.prev_show_id = rep_nyc_nat_prev.fk_show_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_next
# MAGIC         ON next_map.inscape_station_id  = rep_local_next.station_id
# MAGIC         AND c.next_show_id = rep_local_next.fk_show_id
# MAGIC         AND c.fk_dma_id = rep_local_next.dma_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_next
# MAGIC         ON next_map.inscape_station_id  = rep_nyc_nat_next.station_id
# MAGIC         AND c.next_show_id = rep_nyc_nat_next.fk_show_id
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-12T12:00:00'::timestamp
# MAGIC         AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC         AND c.partition_key = '2024-08-12'
# MAGIC         AND CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.all_commercial_feed_comscore_new_table_tms;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.all_commercial_feed_comscore_new_table_tms (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip_null string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.all_commercial_feed_comscore_new_table_tms (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip_null, input_category, input_device, app_service
# MAGIC )
# MAGIC WITH clients_table_to_join AS (
# MAGIC     SELECT *
# MAGIC     FROM prod.detection.clients 
# MAGIC     WHERE 
# MAGIC         client_name in ('kinetiq', 'comscore')
# MAGIC )
# MAGIC ,nielsen_replacement_national_nyc_alias AS (
# MAGIC     SELECT station_id, fk_show_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM prod.detection.nielsen_replacement_national_nyc
# MAGIC     GROUP BY 1,2,3,4
# MAGIC )
# MAGIC ,nielsen_replacement_local_alias AS (
# MAGIC     SELECT station_id, fk_show_id, dma_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM prod.detection.nielsen_replacement_local
# MAGIC     GROUP BY 1,2,3,4,5
# MAGIC )
# MAGIC , activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'comscore'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'comscore'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC ),
# MAGIC epg_program_aggregate AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM prod.detection.vizio_epg_program_aggregate 
# MAGIC ),
# MAGIC viewing_content_firehose AS (
# MAGIC     SELECT DISTINCT fk_tvid, fk_station_id, media_time_start, session_start, session_end, airdate, fk_content_id, is_live
# MAGIC     FROM prod.detection.viewing_content_firehose AS content
# MAGIC     WHERE session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND session_start < '2024-08-12T12:00:00'::timestamp
# MAGIC ),
# MAGIC content_ids_firehose AS (
# MAGIC     SELECT * FROM detection.content_ids_firehose AS cid
# MAGIC     WHERE content_id IN (
# MAGIC         SELECT DISTINCT c.fk_content_id
# MAGIC         FROM prod.detection.viewing_content_firehose AS c
# MAGIC         WHERE c.session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC             AND c.session_start <= '2024-08-12T12:00:00'::timestamp
# MAGIC     )
# MAGIC ), station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT vendor_station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM prod.detection.station_distribution_obfuscation_overwrite
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT vendor_station_id AS station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%comscore%'
# MAGIC ),
# MAGIC inscape_map_deduped AS (
# MAGIC     SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
# MAGIC     FROM (
# MAGIC         SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
# MAGIC         FROM prod.detection.inscape_station_map) ism
# MAGIC     WHERE ism.rn = 1
# MAGIC )
# MAGIC SELECT 
# MAGIC DISTINCT 
# MAGIC     COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
# MAGIC     '', 
# MAGIC     NULLIF(location.zipcode, ''), 
# MAGIC     REPLACE(dma.dma_name, ',', ''), 
# MAGIC     m.external_id, 
# MAGIC     c.media_time_start, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     NULLIF(CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL 
# MAGIC  WHEN COALESCE(c.tms_prev_station_id, c.prev_station_id) = 0 THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0]) 
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  WHEN prev_program.database_key IS NOT NULL THEN prev_program.database_key 
# MAGIC  WHEN 'TMS' = 'TMS' AND prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
# MAGIC  ELSE NULL
# MAGIC  END,''), 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.tms_prev_station_id, c.prev_station_id) = 0 THEN prev_filecontent.title
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  ELSE REPLACE(COALESCE(prev_program.title, prev_program_backup.title,
# MAGIC  CASE WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
# MAGIC  ELSE prev_vizio_program.title
# MAGIC  END), ',', '')
# MAGIC  END, 
# MAGIC     c.prev_session_start, 
# MAGIC     c.prev_session_end, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.tms_prev_station_id, c.prev_station_id) IS NOT NULL THEN COALESCE(prev_map.inscape_call_sign, prev_map_backup.inscape_call_sign)
# MAGIC  WHEN (prev_station_id = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.tms_prev_station_id IS NOT NULL THEN
# MAGIC       CASE WHEN (prev_station.inscape_station_name IS NOT NULL) THEN prev_station.inscape_station_name
# MAGIC            WHEN (LOWER(prev_station.station_affil) LIKE '%affiliate%'
# MAGIC                  OR LOWER(prev_station.station_affil) LIKE '%independent%'
# MAGIC                  OR LOWER(prev_station.station_affil) LIKE '%low power%')
# MAGIC                 THEN prev_station.station_affil END
# MAGIC  WHEN c.tms_prev_station_id IS NULL AND c.prev_station_id IS NOT NULL THEN
# MAGIC       CASE WHEN (prev_station_backup.inscape_station_name IS NOT NULL) THEN prev_station_backup.inscape_station_name
# MAGIC            WHEN (LOWER(prev_station_backup.station_affil) LIKE '%affiliate%'
# MAGIC                  OR LOWER(prev_station_backup.station_affil) LIKE '%independent%'
# MAGIC                  OR LOWER(prev_station_backup.station_affil) LIKE '%low power%')
# MAGIC                 THEN prev_station_backup.station_affil END
# MAGIC  WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
# MAGIC  WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     NULLIF(CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.tms_next_station_id, c.next_station_id) = 0 THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0]) 
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  WHEN next_program.database_key IS NOT NULL THEN next_program.database_key
# MAGIC  WHEN 'TMS' = 'TMS' AND next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
# MAGIC  ELSE NULL
# MAGIC  END,''), 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.tms_next_station_id, c.next_station_id) = 0 THEN next_filecontent.title
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC  ELSE REPLACE(COALESCE(next_program.title, next_program_backup.title,
# MAGIC  CASE WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
# MAGIC  ELSE next_vizio_program.title
# MAGIC  END), ',', '')
# MAGIC  END, 
# MAGIC     c.next_session_start, 
# MAGIC     c.next_session_end, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN COALESCE(c.tms_next_station_id, c.next_station_id) IS NOT NULL THEN COALESCE(next_map.inscape_call_sign, next_map_backup.inscape_call_sign)
# MAGIC  WHEN COALESCE(c.tms_next_station_id, c.next_station_id) = 0 THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
# MAGIC  WHEN c.tms_next_station_id IS NOT NULL THEN
# MAGIC       CASE WHEN (next_station.inscape_station_name IS NOT NULL) THEN next_station.inscape_station_name
# MAGIC            WHEN (LOWER(next_station.station_affil) LIKE '%affiliate%'
# MAGIC                  OR LOWER(next_station.station_affil) LIKE '%independent%'
# MAGIC                  OR LOWER(next_station.station_affil) LIKE '%low power%')
# MAGIC                 THEN next_station.station_affil END
# MAGIC  WHEN c.tms_next_station_id IS NULL AND c.next_station_id IS NOT NULL THEN
# MAGIC       CASE WHEN (next_station_backup.inscape_station_name IS NOT NULL) THEN next_station_backup.inscape_station_name
# MAGIC            WHEN (LOWER(next_station_backup.station_affil) LIKE '%affiliate%'
# MAGIC                  OR LOWER(next_station_backup.station_affil) LIKE '%independent%'
# MAGIC                  OR LOWER(next_station_backup.station_affil) LIKE '%low power%')
# MAGIC                 THEN next_station_backup.station_affil END
# MAGIC  WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
# MAGIC  WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED'
# MAGIC  AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
# MAGIC  ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC  END, 
# MAGIC     REPLACE(m.brand_name, ',', ''), 
# MAGIC     REPLACE(m.title, ',', ''), 
# MAGIC     m.duration, 
# MAGIC     NULL AS ip, 
# MAGIC     tvis.category, 
# MAGIC     tvis.input_device, 
# MAGIC     CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
# MAGIC             CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC                 WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
# MAGIC                 WHEN LOWER(coalesce(tis.app_name)) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND prev_show_id IS NOT NULL THEN NULL
# MAGIC                 WHEN LOWER(coalesce(tis.app_name)) = 'unknown' THEN NULL
# MAGIC                 ELSE coalesce(tis.app_name) END
# MAGIC             WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC         END
# MAGIC     FROM dev.detection.viewing_commercials_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z 
# MAGIC         ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     INNER JOIN
# MAGIC         prod.detection.tv AS tv
# MAGIC         ON c.fk_tvid = tv.tvid
# MAGIC         AND tv.oem = 'VIZIO'
# MAGIC     JOIN
# MAGIC         prod.detection.tv_populations AS tp
# MAGIC         ON c.fk_tvid = tp.fk_tvid 
# MAGIC     JOIN
# MAGIC         prod.detection.populations AS pop
# MAGIC         ON tp.fk_population_id = pop.population_id 
# MAGIC         AND LOWER(pop.population_name) = 'opted_in'
# MAGIC     JOIN
# MAGIC         prod.detection.commercial_id_external_firehose AS m
# MAGIC         ON c.fk_commercial_id = m.fk_commercial_id
# MAGIC     JOIN
# MAGIC         clients_table_to_join cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC     JOIN
# MAGIC         prod.detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     JOIN 
# MAGIC         prod.detection.tv_input_stats_firehose  tvis    
# MAGIC         ON c.session_start >= tvis.create_timestamp 
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-08-12T12:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tvis.fk_tvid   
# MAGIC         AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     JOIN
# MAGIC         prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-12T12:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-12T07:00:00'::timestamp
# MAGIC     JOIN 
# MAGIC         prod.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     LEFT OUTER JOIN
# MAGIC         prod.detection.dma AS dma ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
# MAGIC         ON c.prev_vizio_epg_station = prev_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
# MAGIC         ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN prod.detection.vizio_epg_station AS next_vizio_station 
# MAGIC         ON c.next_vizio_epg_station = next_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
# MAGIC         ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS prev_content 
# MAGIC         ON c.fk_tvid = prev_content.fk_tvid
# MAGIC         AND prev_content.session_start = c.prev_session_start
# MAGIC
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS prev_station  
# MAGIC         ON prev_station.station_id = c.tms_prev_station_id
# MAGIC         AND prev_station.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS prev_station_backup
# MAGIC         ON prev_station_backup.station_id = c.prev_station_id
# MAGIC         AND c.tms_prev_station_id IS NULL
# MAGIC         AND prev_station_backup.vendor_name = 'TIVO'
# MAGIC     LEFT OUTER JOIN inscape_map_deduped AS prev_map
# MAGIC         ON prev_map.mapped_vendor_station_id = c.tms_prev_station_id
# MAGIC         AND prev_map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN inscape_map_deduped AS prev_map_backup
# MAGIC         ON prev_map_backup.mapped_vendor_station_id = c.prev_station_id
# MAGIC         AND c.tms_prev_station_id IS NULL
# MAGIC         AND prev_map_backup.mapped_vendor = 'TIVO' 
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
# MAGIC         ON c.tms_prev_station_id = prev_station_blacklist.station_id
# MAGIC         AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS prev_station_obfs
# MAGIC         ON c.tms_prev_station_id = prev_station_obfs.vendor_station_id
# MAGIC         AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS prev_program
# MAGIC         ON c.tms_prev_show_id = prev_program.show_id
# MAGIC        AND prev_program.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS prev_program_backup
# MAGIC         ON c.prev_show_id = prev_program_backup.show_id
# MAGIC        AND prev_program_backup.vendor_name = 'TIVO'
# MAGIC         AND c.tms_prev_show_id IS NULL
# MAGIC
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS prev_filecontent 
# MAGIC         ON c.prev_show_id = prev_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS next_filecontent 
# MAGIC         ON c.next_show_id = next_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m_filter 
# MAGIC         ON m_filter.fk_content_id = prev_content.fk_content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as prev_cid on c.prev_show_id = prev_cid.content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as next_cid on c.next_show_id = next_cid.content_id
# MAGIC     LEFT OUTER JOIN prod.detection.clients cl2
# MAGIC         ON m_filter.fk_client_id = cl2.client_id
# MAGIC         AND cl2.client_name not in ('kinetiq', 'comscore')
# MAGIC
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS next_station  
# MAGIC         ON next_station.station_id = c.tms_next_station_id
# MAGIC         AND next_station.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_station AS next_station_backup
# MAGIC         ON next_station_backup.station_id = c.next_station_id
# MAGIC         AND c.tms_next_station_id IS NULL
# MAGIC         AND next_station_backup.vendor_name = 'TIVO'
# MAGIC     LEFT OUTER JOIN inscape_map_deduped AS next_map
# MAGIC         ON next_map.mapped_vendor_station_id = c.tms_next_station_id
# MAGIC         AND next_map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN inscape_map_deduped AS next_map_backup
# MAGIC         ON next_map_backup.mapped_vendor_station_id = c.next_station_id
# MAGIC         AND c.tms_next_station_id IS NULL
# MAGIC         AND next_map_backup.mapped_vendor = 'TIVO' 
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
# MAGIC         ON c.tms_next_station_id = next_station_blacklist.station_id
# MAGIC         AND next_station_blacklist.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS next_station_obfs
# MAGIC         ON c.tms_next_station_id = next_station_obfs.vendor_station_id
# MAGIC         AND next_station_obfs.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS next_program
# MAGIC         ON c.tms_next_show_id = next_program.show_id
# MAGIC        AND next_program.vendor_name = 'TMS'
# MAGIC     LEFT OUTER JOIN prod.detection.epg_show AS next_program_backup
# MAGIC         ON c.next_show_id = next_program_backup.show_id
# MAGIC        AND next_program_backup.vendor_name = 'TIVO'
# MAGIC         AND c.tms_next_show_id IS NULL
# MAGIC
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >=  (tis.create_timestamp::double)::timestamp
# MAGIC         AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND tis.create_timestamp <= ('2024-08-12T12:00:00'::timestamp::double)::timestamp
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-12T07:00:00'::timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid 
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN activity_obfuscation appb 
# MAGIC         ON coalesce(tis.app_name) = appb.app_name 
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb
# MAGIC         ON coalesce(tis.app_name) = acrb.app_name
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist prev_chanb 
# MAGIC         ON prev_vizio_station.name = prev_chanb.channel_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         prod.detection.free_channels_distribution_blacklist next_chanb 
# MAGIC         ON next_vizio_station.name = next_chanb.channel_name
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS prev_nielsen_blacklist
# MAGIC         ON prev_nielsen_blacklist.station_id = prev_map.inscape_station_id 
# MAGIC         AND c.session_start >= prev_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < prev_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS next_nielsen_blacklist
# MAGIC         ON next_nielsen_blacklist.station_id = next_map.inscape_station_id 
# MAGIC         AND c.session_start >= next_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < next_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_prev
# MAGIC         ON prev_map.inscape_station_id  = rep_local_prev.station_id
# MAGIC         AND c.prev_show_id = rep_local_prev.fk_show_id
# MAGIC         AND c.fk_dma_id = rep_local_prev.dma_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_prev
# MAGIC         ON prev_map.inscape_station_id  = rep_nyc_nat_prev.station_id
# MAGIC         AND c.prev_show_id = rep_nyc_nat_prev.fk_show_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_next
# MAGIC         ON next_map.inscape_station_id  = rep_local_next.station_id
# MAGIC         AND c.next_show_id = rep_local_next.fk_show_id
# MAGIC         AND c.fk_dma_id = rep_local_next.dma_id
# MAGIC     LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_next
# MAGIC         ON next_map.inscape_station_id  = rep_nyc_nat_next.station_id
# MAGIC         AND c.next_show_id = rep_nyc_nat_next.fk_show_id
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-08-12T07:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-12T12:00:00'::timestamp
# MAGIC         AND ABS(MOD(c.fk_tvid, 10)) = 0
# MAGIC         AND c.partition_key = '2024-08-12'
# MAGIC         AND CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC (SELECT 'Matching Rows' AS table_name
# MAGIC  , DATE_TRUNC('HOUR', exs_report.ts_start) AS report_date
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.all_commercial_feed_comscore_existing_table AS exs_report
# MAGIC  JOIN dev.mohit_gangwani.all_commercial_feed_comscore_new_table AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.value = new_report.value
# MAGIC  GROUP BY 2
# MAGIC )
# MAGIC UNION
# MAGIC (SELECT 'TiVo Existing Table' AS table_name
# MAGIC  , DATE_TRUNC('HOUR', ts_start) AS report_date
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.all_commercial_feed_comscore_existing_table AS exs_report
# MAGIC  GROUP BY 2
# MAGIC )
# MAGIC UNION
# MAGIC (SELECT 'TiVo New Table' AS table_name
# MAGIC  , DATE_TRUNC('HOUR', ts_start) AS report_date
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT new_report.tvid) AS total_tvs
# MAGIC  , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
# MAGIC  FROM dev.mohit_gangwani.all_commercial_feed_comscore_new_table AS new_report
# MAGIC  GROUP BY 2
# MAGIC )
# MAGIC ORDER BY 2, 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(exs_report.ts_start) AS report_date
# MAGIC , CASE WHEN exs_report.input_category <=> new_report.input_category THEN 'Match'
# MAGIC        ELSE 'No Match'
# MAGIC   END AS input_category_match
# MAGIC , CASE WHEN exs_report.input_device <=> new_report.input_device THEN 'Match'
# MAGIC        ELSE 'No Match'
# MAGIC   END AS input_device_match
# MAGIC , COUNT(*) AS session_count
# MAGIC , COUNT(DISTINCT exs_report.tvid) AS total_tvs
# MAGIC FROM dev.mohit_gangwani.all_commercial_feed_comscore_existing_table AS exs_report
# MAGIC JOIN dev.mohit_gangwani.all_commercial_feed_comscore_new_table AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.value = new_report.value
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
# MAGIC FROM dev.mohit_gangwani.all_commercial_feed_comscore_existing_table AS exs_report
# MAGIC JOIN dev.mohit_gangwani.all_commercial_feed_comscore_new_table AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.value = new_report.value
# MAGIC GROUP BY 1,2,3
# MAGIC ORDER BY 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(exs_report.ts_start) AS report_date
# MAGIC , CASE WHEN exs_report.prev_episode_id IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.prev_episode_id = new_report.prev_episode_id THEN '1 - Match'
# MAGIC                        WHEN exs_report.prev_episode_id != new_report.prev_episode_id THEN '3 - No Match'
# MAGIC                        WHEN NULLIF(new_report.prev_episode_id, '') IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.prev_episode_id IS NULL AND new_report.prev_episode_id IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS prev_episode_id_match
# MAGIC , CASE WHEN exs_report.next_episode_id IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.next_episode_id = new_report.next_episode_id THEN '1 - Match'
# MAGIC                        WHEN exs_report.next_episode_id != new_report.next_episode_id THEN '3 - No Match'
# MAGIC                        WHEN NULLIF(new_report.next_episode_id, '') IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.next_episode_id IS NULL AND NULLIF(new_report.next_episode_id, '') IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS next_episode_id_match
# MAGIC , CASE WHEN exs_report.live IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.live = new_report.live THEN '1 - Match'
# MAGIC                        WHEN exs_report.live != new_report.live THEN '3 - No Match'
# MAGIC                        WHEN new_report.live IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.live IS NULL AND new_report.live IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS live_match
# MAGIC , CASE WHEN exs_report.prev_title  IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.prev_title = new_report.prev_title THEN '1 - Match'
# MAGIC                        WHEN exs_report.prev_title != new_report.prev_title THEN '3 - No Match'
# MAGIC                        WHEN new_report.prev_title IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.prev_title IS NULL AND new_report.prev_title IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS prev_title_match
# MAGIC , CASE WHEN exs_report.next_title  IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.next_title = new_report.next_title THEN '1 - Match'
# MAGIC                        WHEN exs_report.next_title != new_report.next_title THEN '3 - No Match'
# MAGIC                        WHEN new_report.next_title IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.next_title IS NULL AND new_report.next_title IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS next_title_match
# MAGIC , COUNT(*)*1.0 AS session_count
# MAGIC FROM dev.mohit_gangwani.all_commercial_feed_comscore_existing_table AS exs_report
# MAGIC JOIN dev.mohit_gangwani.all_commercial_feed_comscore_new_table AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.value = new_report.value
# MAGIC GROUP BY 1,2,3,4,5,6--,7
# MAGIC ORDER BY 1,2,3,4,5,6--,7

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(exs_report.ts_start) AS report_date
# MAGIC , CASE WHEN exs_report.prev_channel_callsign IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.prev_channel_callsign = new_report.prev_channel_callsign THEN '1 - Match'
# MAGIC                        WHEN exs_report.prev_channel_callsign != new_report.prev_channel_callsign THEN '3 - No Match'
# MAGIC                        WHEN new_report.prev_channel_callsign IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.prev_channel_callsign IS NULL AND new_report.prev_channel_callsign IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS prev_channel_callsign_match
# MAGIC , CASE WHEN exs_report.next_channel_callsign IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.next_channel_callsign = new_report.next_channel_callsign THEN '1 - Match'
# MAGIC                        WHEN exs_report.next_channel_callsign != new_report.next_channel_callsign THEN '3 - No Match'
# MAGIC                        WHEN new_report.next_channel_callsign IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.next_channel_callsign IS NULL AND new_report.next_channel_callsign IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS next_channel_callsign_match
# MAGIC
# MAGIC , CASE WHEN exs_report.prev_network_affiliate IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.prev_network_affiliate = new_report.prev_network_affiliate THEN '1 - Match'
# MAGIC                        WHEN exs_report.prev_network_affiliate != new_report.prev_network_affiliate THEN '3 - No Match'
# MAGIC                        WHEN new_report.prev_network_affiliate IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.prev_network_affiliate IS NULL AND new_report.prev_network_affiliate IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS prev_channel_affiliate_match
# MAGIC , CASE WHEN exs_report.next_network_affiliate IS NOT NULL THEN
# MAGIC            CASE WHEN exs_report.next_network_affiliate = new_report.next_network_affiliate THEN '1 - Match'
# MAGIC                        WHEN exs_report.next_network_affiliate != new_report.next_network_affiliate THEN '3 - No Match'
# MAGIC                        WHEN new_report.next_network_affiliate IS NULL THEN '4 - Existing Not Null, New Null'
# MAGIC                   END
# MAGIC         WHEN exs_report.next_network_affiliate IS NULL AND new_report.next_network_affiliate IS NOT NULL THEN '5 - Existing Null, New Not Null'
# MAGIC         ELSE '2 - All Null'
# MAGIC   END AS next_channel_affiliate_match
# MAGIC , COUNT(*)*1.0 AS session_count
# MAGIC FROM dev.mohit_gangwani.all_commercial_feed_comscore_existing_table AS exs_report
# MAGIC JOIN dev.mohit_gangwani.all_commercial_feed_comscore_new_table AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.value = new_report.value
# MAGIC GROUP BY 1,2,3,4,5
# MAGIC ORDER BY 1,2,3,4,5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT new_report.*
# MAGIC FROM dev.mohit_gangwani.all_commercial_feed_4c_existing_table AS exs_report
# MAGIC JOIN dev.mohit_gangwani.all_commercial_feed_4c_new_table AS new_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.value = new_report.value
# MAGIC  AND exs_report.next_episode_id IS NULL
# MAGIC  AND new_report.next_episode_id IS NOT NULL

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
# MAGIC  FROM dev.mohit_gangwani.r388_content_with_null_cognet_existing_table AS exs_report
# MAGIC  WHERE exs_report.app_service IS NULL
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
# MAGIC  FROM dev.mohit_gangwani.r388_content_with_null_cognet_new_table AS new_report
# MAGIC  WHERE new_report.app_service IS NULL
# MAGIC --  WHERE COALESCE(new_report.app_service, '') != 'WatchFree+'
# MAGIC  GROUP BY 1, 2, 3, 4, 5, 6

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT new_report.*
# MAGIC FROM dev.mohit_gangwani.r388_content_with_null_cognet_new_table AS new_report
# MAGIC WHERE new_report.channel_affiliate IS NULL
# MAGIC --  AND new_report.live IN ('t', 'f')
# MAGIC  AND new_report.episode_id IS NOT NULL
# MAGIC  AND new_report.channel_callsign IS NULL
# MAGIC ORDER BY tvid, ts_start
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT x.*
# MAGIC FROM dev.mohit_gangwani.r388_content_with_null_cognet_existing_table x
# MAGIC WHERE x.tvid = '10027110_90042_532952871'
# MAGIC AND x.ts_start = '2024-08-01T20:59:16'
# MAGIC -- WHERE x.channel_affiliate IS NULL
# MAGIC --  AND x.live IN ('t', 'f')
# MAGIC --  AND episode_id IS NOT NULL
# MAGIC ORDER BY tvid, ts_start
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.r388_content_with_null_nielsen_new_table;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.r388_content_with_null_nielsen_new_table (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC INSERT INTO dev.mohit_gangwani.r388_content_with_null_nielsen_new_table (
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
# MAGIC         WHEN 'nielsen' != 'nielsen' THEN COALESCE(c.airdate, c.tms_airdate)
# MAGIC         WHEN 'nielsen' = 'nielsen' THEN c.airdate
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
# MAGIC         WHEN 'nielsen' = 'nielsen' AND c.airdate IS NOT NULL THEN map.inscape_call_sign
# MAGIC         -- WHEN 'nielsen' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
# MAGIC         -- WHEN 'nielsen' = 'nielsen' AND c.airdate IS NULL THEN NULL
# MAGIC         -- ELSE COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
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
# MAGIC         WHEN 'nielsen' = 'nielsen' AND c.airdate IS NULL THEN NULL
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
# MAGIC             WHEN 'nielsen' = 'nielsen' AND c.airdate IS NULL THEN NULL
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
# MAGIC         WHEN 'nielsen' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
# MAGIC         WHEN 'nielsen' = 'nielsen' AND c.airdate IS NULL THEN NULL
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
# MAGIC     dev.detection.viewing_content_firehose AS c
# MAGIC     JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-08-01T07:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-02T07:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-02T07:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-01T07:00:00'::timestamp
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
# MAGIC        AND 'nielsen' != 'nielsen'
# MAGIC     LEFT OUTER JOIN detection.epg_station AS backup_station
# MAGIC         ON backup_station.station_id = c.tms_station_id
# MAGIC        AND c.fk_station_id IS NULL
# MAGIC        AND backup_station.vendor_name = 'TMS'
# MAGIC        AND 'nielsen' != 'nielsen'
# MAGIC     LEFT OUTER JOIN inscape_station_map_dedupe AS backup_map
# MAGIC         ON backup_map.mapped_vendor_station_id = c.tms_station_id
# MAGIC         AND c.fk_station_id IS NULL
# MAGIC         AND backup_map.mapped_vendor = 'TMS'
# MAGIC         AND 'nielsen' != 'nielsen'
# MAGIC -- Historical change End
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
# MAGIC         ON map.inscape_station_id = station_blacklist.station_id
# MAGIC         AND station_blacklist.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
# MAGIC         ON c.fk_station_id = station_obfs.station_id
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
# MAGIC         AND tvis.create_timestamp <= '2024-08-02T07:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-01T07:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-08-02T07:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-01T07:00:00'::timestamp::double)::timestamp 
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
# MAGIC         AND ip.create_timestamp <= '2024-08-02T07:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-08-01T07:00:00'::timestamp
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
# MAGIC         c.session_start >= '2024-08-01T07:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-08-02T07:00:00'::timestamp
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
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.r388_content_with_null_nielsen_existing_table;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.r388_content_with_null_nielsen_existing_table (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC INSERT INTO dev.mohit_gangwani.r388_content_with_null_nielsen_existing_table (
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
# MAGIC     JOIN prod.detection.tv AS tv ON c.session_start >= '2024-08-01T07:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-08-02T07:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN prod.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-08-02T07:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-08-01T07:00:00'::timestamp
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
# MAGIC         AND schedule.airdate >= '2024-08-01T07:00:00'::timestamp - interval '60' day
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
# MAGIC         AND tvis.create_timestamp <= '2024-08-02T07:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-08-01T07:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-08-02T07:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-08-01T07:00:00'::timestamp::double)::timestamp 
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
# MAGIC         AND ip.create_timestamp <= '2024-08-02T07:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-08-01T07:00:00'::timestamp
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
# MAGIC         c.session_start >= '2024-08-01T07:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-08-02T07:00:00'::timestamp
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
# MAGIC SELECT CASE WHEN vc.fk_station_id IS NULL THEN 'Null Station ID' END AS null_station_id
# MAGIC , CASE WHEN vc.fk_show_id IS NULL THEN 'Null Show ID' END AS null_show_id
# MAGIC , CASE WHEN vc.airdate IS NULL THEN 'Null airdate' END AS null_airdate
# MAGIC , COUNT(*)
# MAGIC FROM dev.detection.viewing_content_firehose vc
# MAGIC JOIN (
# MAGIC SELECT SPLIT_PART(new_report.tvid, '_', 1) AS tvid, new_report.ts_start, new_report.ts_end
# MAGIC FROM dev.mohit_gangwani.r388_content_with_null_nielsen_new_table AS new_report
# MAGIC JOIN dev.mohit_gangwani.r388_content_with_null_nielsen_existing_table AS exs_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip = new_report.ip
# MAGIC  AND exs_report.air_date IS NULL
# MAGIC  AND new_report.air_date IS NOT NULL) x
# MAGIC ON vc.fk_tvid = x.tvid
# MAGIC AND vc.session_start = x.ts_start
# MAGIC AND vc.session_end = x.ts_end
# MAGIC WHERE vc.session_start >= '2024-08-01T07:00:00'::timestamp
# MAGIC     AND vc.session_start < '2024-08-02T07:00:00'::timestamp
# MAGIC GROUP BY 1, 2, 3
# MAGIC
# MAGIC -- ORDER BY new_report.tvid, new_report.ts_start
# MAGIC -- LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT CASE WHEN vc.fk_station_id IS NULL THEN 'Null Station ID' END AS null_station_id
# MAGIC -- , CASE WHEN vc.fk_show_id IS NULL THEN 'Null Show ID' END AS null_show_id
# MAGIC -- , CASE WHEN vc.airdate IS NULL THEN 'Null airdate' END AS null_airdate
# MAGIC SELECT station_obfs.station_id
# MAGIC , COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose vc
# MAGIC JOIN (
# MAGIC   SELECT SPLIT_PART(exs_report.tvid, '_', 1) AS tvid, exs_report.ts_start, exs_report.ts_end
# MAGIC   FROM dev.mohit_gangwani.r388_content_with_null_nielsen_existing_table AS exs_report
# MAGIC   JOIN dev.mohit_gangwani.r388_content_with_null_nielsen_new_table AS new_report
# MAGIC     ON exs_report.tvid = new_report.tvid
# MAGIC   AND exs_report.ts_start = new_report.ts_start
# MAGIC   AND exs_report.ts_end = new_report.ts_end
# MAGIC   AND exs_report.ip = new_report.ip
# MAGIC   AND exs_report.channel_affiliate IS NULL
# MAGIC   AND new_report.channel_affiliate IS NOT NULL
# MAGIC  ) x
# MAGIC   ON vc.fk_tvid = x.tvid
# MAGIC   AND vc.session_start = x.ts_start
# MAGIC LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
# MAGIC         ON vc.fk_station_id = station_obfs.station_id
# MAGIC WHERE vc.session_start >= '2024-08-01T07:00:00'::timestamp
# MAGIC   AND vc.session_start < '2024-08-02T07:00:00'::timestamp
# MAGIC GROUP BY 1--, 2, 3
# MAGIC
# MAGIC -- ORDER BY new_report.tvid, new_report.ts_start
# MAGIC -- LIMIT 1000

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
# MAGIC SELECT * FROM prod.detection.viewing_content_firehose WHERE fk_tvid = '10027110' AND session_start = '2024-08-01T20:59:16'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT exs_report.*
# MAGIC FROM dev.mohit_gangwani.r388_content_with_null_nielsen_new_table AS new_report
# MAGIC JOIN dev.mohit_gangwani.r388_content_with_null_nielsen_existing_table AS exs_report
# MAGIC   ON exs_report.tvid = new_report.tvid
# MAGIC  AND exs_report.ts_start = new_report.ts_start
# MAGIC  AND exs_report.ts_end = new_report.ts_end
# MAGIC  AND exs_report.ip = new_report.ip
# MAGIC  AND exs_report.channel_callsign IS NOT NULL
# MAGIC  AND new_report.channel_callsign IS NULL
# MAGIC ORDER BY exs_report.tvid, exs_report.ts_start
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.detection.viewing_content_firehose WHERE fk_tvid = '136969970' AND session_start = '2024-08-02T03:00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.detection.viewing_content_firehose WHERE fk_tvid = '136969970' AND session_start = '2024-08-02T03:00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.detection.station_metadata_obfuscation

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DATE(session_start), COUNT(*)
# MAGIC FROM dev.detection.viewing_commercials_firehose_dedup c
# MAGIC WHERE c.session_start >= '2024-08-01T00:00:00'::timestamp
# MAGIC   AND c.session_start < '2024-08-20T00:00:00'::timestamp
# MAGIC   AND c.partition_key >= '2024-08-01'
# MAGIC   AND c.partition_key <= '2024-08-20'
# MAGIC GROUP BY 1
