# Databricks notebook source
# MAGIC %md
# MAGIC Creating stations blacklist table and metadata obfuscation table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS public.temp_mohit_station_distribution_blacklist;
# MAGIC CREATE TABLE public.temp_mohit_station_distribution_blacklist AS
# MAGIC SELECT ism.inscape_station_id AS station_id, st.vendor_name, 'NBC' AS client_name
# MAGIC FROM stage.detection.inscape_station_map ism
# MAGIC JOIN stage.detection.epg_station st
# MAGIC   ON st.station_id = ism.mapped_vendor_station_id
# MAGIC  AND st.vendor_name = ism.mapped_vendor
# MAGIC WHERE st.station_call_sign LIKE 'PKPOC%'
# MAGIC GROUP BY 1, 2, 3
# MAGIC UNION
# MAGIC SELECT ism.inscape_station_id, st.vendor_name, 'videoamp' AS client_name
# MAGIC FROM stage.detection.inscape_station_map ism
# MAGIC JOIN stage.detection.epg_station st
# MAGIC   ON st.station_id = ism.mapped_vendor_station_id
# MAGIC  AND st.vendor_name = ism.mapped_vendor
# MAGIC WHERE st.station_call_sign LIKE 'PKPOC%'
# MAGIC GROUP BY 1, 2, 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS public.temp_mohit_station_metadata_obfuscation;
# MAGIC CREATE TABLE public.temp_mohit_station_metadata_obfuscation AS
# MAGIC SELECT ism.inscape_station_id AS station_id, ism.mapped_vendor_station_id AS vendor_station_id, st.vendor_name
# MAGIC FROM stage.detection.inscape_station_map ism
# MAGIC JOIN stage.detection.epg_station st
# MAGIC   ON st.station_id = ism.mapped_vendor_station_id
# MAGIC  AND st.vendor_name = ism.mapped_vendor
# MAGIC WHERE st.station_call_sign LIKE 'PKPOC%'
# MAGIC GROUP BY 1, 2, 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM public.temp_mohit_station_distribution_blacklist;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM public.temp_mohit_station_metadata_obfuscation;

# COMMAND ----------

# MAGIC %md
# MAGIC Creating one day of data for the time we ran the tests and for the following day
# MAGIC 1. Content only

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS public.temp_mohit_r385_content_4c_2024_06_26_13_production (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip_null string, input_category string, input_device string, app_service string
# MAGIC     )
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO public.temp_mohit_r385_content_4c_2024_06_26_13_production (
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
# MAGIC     ip_null, 
# MAGIC     input_category, 
# MAGIC     input_device, 
# MAGIC     app_service
# MAGIC )
# MAGIC WITH activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = '4c'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = '4c'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM public.temp_mohit_station_distribution_blacklist
# MAGIC         WHERE client_name <> '4c'
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%4c%'
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
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
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
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN '4c' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN '4c' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
# MAGIC         ELSE schedule.airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN station.station_call_sign
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL -- 2024-06-26 Peacock Addition
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN '4c' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN '4c' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC         ELSE map.inscape_call_sign   
# MAGIC         END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN '4c' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN '4c' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC         ELSE LEAST(c.media_time_start, schedule.duration) 
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC     CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED'
# MAGIC         ELSE vizio_station.name END
# MAGIC     ELSE
# MAGIC         CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL -- 2024-06-26 Peacock Addition
# MAGIC         WHEN '4c' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN '4c' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC         WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC         THEN station.station_affil ELSE NULL END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC     WHEN '4c' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC     WHEN '4c' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     NULL AS ip, 
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
# MAGIC     stage.detection.viewing_content_firehose AS c
# MAGIC     JOIN detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         -- AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN stage.detection.tv AS tv ON c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN stage.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC     JOIN stage.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     JOIN stage.detection.tv_populations AS u
# MAGIC         ON c.fk_tvid = u.fk_tvid 
# MAGIC     JOIN detection.populations AS pop
# MAGIC         ON u.fk_population_id = pop.population_id 
# MAGIC         AND pop.population_name = 'opted_in'
# MAGIC     JOIN detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     LEFT OUTER JOIN detection.dma AS dma
# MAGIC         ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN detection.input_source inps 
# MAGIC         ON c.fk_input_source_id = inps.input_source_id
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS map
# MAGIC         ON map.inscape_station_id = c.fk_station_id
# MAGIC         AND map.mapped_vendor = 'TIVO'
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS station
# MAGIC         ON map.mapped_vendor_station_id = station.station_id 
# MAGIC         AND station.vendor_name = map.mapped_vendor
# MAGIC -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
# MAGIC       ON c.fk_station_id = station_blacklist.station_id
# MAGIC       AND station_blacklist.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS station_obfs
# MAGIC       ON c.fk_station_id = station_obfs.station_id
# MAGIC       AND station_obfs.vendor_name = map.mapped_vendor
# MAGIC -- End of new code
# MAGIC     LEFT OUTER JOIN detection.epg_schedule_latest AS schedule
# MAGIC         ON map.mapped_vendor_station_id = schedule.fk_station_id
# MAGIC         AND schedule.vendor_name = map.mapped_vendor
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) >= schedule.airdate  
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) <  schedule.airdate_end
# MAGIC         AND schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS show
# MAGIC         ON schedule.fk_show_id = show.show_id
# MAGIC         AND show.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = c.fk_show_id 
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS vizio_station 
# MAGIC         ON c.vizio_epg_station = vizio_station.station_id
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_program_aggregate AS vizio_program
# MAGIC         ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
# MAGIC         AND c.vizio_epg_program != '0'
# MAGIC     JOIN detection.content_ids_firehose AS cid
# MAGIC         ON cid.content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS m
# MAGIC         ON m.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.clients cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC         AND cl.client_name <> '4c'
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = '4c'
# MAGIC     JOIN stage.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN stage.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-06-14T21:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-06-13T21:00:00'::timestamp::double)::timestamp 
# MAGIC     LEFT OUTER JOIN activity_obfuscation AS appb
# MAGIC         ON tis.app_name = appb.app_name
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb 
# MAGIC         ON tis.app_name = acrb.app_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist chanb 
# MAGIC         ON vizio_station.name = chanb.channel_name
# MAGIC     LEFT OUTER JOIN detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         and c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         and c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND '4c' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     LEFT OUTER JOIN detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND backup_show.show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC         AND '4c' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     LEFT OUTER JOIN detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND backup_show.show_id  = rep_nyc_nat.fk_show_id
# MAGIC         AND '4c' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport')
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
# MAGIC     AND cl.client_id IS NULL
# MAGIC     AND (cid.content_cid <> 'unknown' OR vizio_station.name IS NOT NULL)
# MAGIC     AND CASE WHEN tvis.category = 'APPS' AND vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     AND CASE WHEN c.fk_station_id IS NOT NULL AND station_blacklist.station_id IS NOT NULL THEN FALSE ELSE TRUE END             -- 2024-06-26 Peacock Addition

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS public.temp_mohit_r385_content_nbc_2024_06_26_13_production (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip_null string, input_category string, input_device string, app_service string
# MAGIC     )
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO public.temp_mohit_r385_content_nbc_2024_06_26_13_production (
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
# MAGIC     ip_null, 
# MAGIC     input_category, 
# MAGIC     input_device, 
# MAGIC     app_service
# MAGIC )
# MAGIC WITH activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'NBC'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'NBC'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM public.temp_mohit_station_distribution_blacklist
# MAGIC         WHERE client_name <> 'NBC'
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%NBC%'
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
# MAGIC             WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
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
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'NBC' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN 'NBC' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
# MAGIC         ELSE schedule.airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN station.station_call_sign
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL -- 2024-06-26 Peacock Addition
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'NBC' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'NBC' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC         ELSE map.inscape_call_sign   
# MAGIC         END, 
# MAGIC     CASE
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN 'NBC' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'NBC' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC         ELSE LEAST(c.media_time_start, schedule.duration) 
# MAGIC     END, 
# MAGIC     c.session_start, 
# MAGIC     c.session_end, 
# MAGIC     CASE WHEN c.vizio_epg_station IS NOT NULL THEN
# MAGIC     CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED'
# MAGIC         ELSE vizio_station.name END
# MAGIC     ELSE
# MAGIC         CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL -- 2024-06-26 Peacock Addition
# MAGIC         WHEN 'NBC' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'NBC' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC         WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC         THEN station.station_affil ELSE NULL END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC     WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC     WHEN 'NBC' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC     WHEN 'NBC' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
# MAGIC     ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     NULL AS ip, 
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
# MAGIC     stage.detection.viewing_content_firehose AS c
# MAGIC     JOIN detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         -- AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN stage.detection.tv AS tv ON c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN stage.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC     JOIN stage.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     JOIN stage.detection.tv_populations AS u
# MAGIC         ON c.fk_tvid = u.fk_tvid 
# MAGIC     JOIN detection.populations AS pop
# MAGIC         ON u.fk_population_id = pop.population_id 
# MAGIC         AND pop.population_name = 'opted_in'
# MAGIC     JOIN detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     LEFT OUTER JOIN detection.dma AS dma
# MAGIC         ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN detection.input_source inps 
# MAGIC         ON c.fk_input_source_id = inps.input_source_id
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS map
# MAGIC         ON map.inscape_station_id = c.fk_station_id
# MAGIC         AND map.mapped_vendor = 'TIVO'
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS station
# MAGIC         ON map.mapped_vendor_station_id = station.station_id 
# MAGIC         AND station.vendor_name = map.mapped_vendor
# MAGIC -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
# MAGIC       ON c.fk_station_id = station_blacklist.station_id
# MAGIC       AND station_blacklist.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS station_obfs
# MAGIC       ON c.fk_station_id = station_obfs.station_id
# MAGIC       AND station_obfs.vendor_name = map.mapped_vendor
# MAGIC -- End of new code
# MAGIC     LEFT OUTER JOIN detection.epg_schedule_latest AS schedule
# MAGIC         ON map.mapped_vendor_station_id = schedule.fk_station_id
# MAGIC         AND schedule.vendor_name = map.mapped_vendor
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) >= schedule.airdate  
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) <  schedule.airdate_end
# MAGIC         AND schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS show
# MAGIC         ON schedule.fk_show_id = show.show_id
# MAGIC         AND show.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = c.fk_show_id 
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS vizio_station 
# MAGIC         ON c.vizio_epg_station = vizio_station.station_id
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_program_aggregate AS vizio_program
# MAGIC         ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
# MAGIC         AND c.vizio_epg_program != '0'
# MAGIC     JOIN detection.content_ids_firehose AS cid
# MAGIC         ON cid.content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS m
# MAGIC         ON m.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.clients cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC         AND cl.client_name <> 'NBC'
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'NBC'
# MAGIC     JOIN stage.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN stage.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-06-14T21:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-06-13T21:00:00'::timestamp::double)::timestamp 
# MAGIC     LEFT OUTER JOIN activity_obfuscation AS appb
# MAGIC         ON tis.app_name = appb.app_name
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb 
# MAGIC         ON tis.app_name = acrb.app_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist chanb 
# MAGIC         ON vizio_station.name = chanb.channel_name
# MAGIC     LEFT OUTER JOIN detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         and c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         and c.session_start < nielsen_blacklist.blacklist_end
# MAGIC         AND 'NBC' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     LEFT OUTER JOIN detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND backup_show.show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC         AND 'NBC' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     LEFT OUTER JOIN detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND backup_show.show_id  = rep_nyc_nat.fk_show_id
# MAGIC         AND 'NBC' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport')
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
# MAGIC     AND cl.client_id IS NULL
# MAGIC     AND (cid.content_cid <> 'unknown' OR vizio_station.name IS NOT NULL)
# MAGIC     AND CASE WHEN tvis.category = 'APPS' AND vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC     AND CASE WHEN c.fk_station_id IS NOT NULL AND station_blacklist.station_id IS NOT NULL THEN FALSE ELSE TRUE END             -- 2024-06-26 Peacock Addition

# COMMAND ----------

# MAGIC %md
# MAGIC 2. All commercials

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS public.temp_mohit_r471_all_commercial_feed_nielsen_2024_06_26_13_production;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS public.temp_mohit_r471_all_commercial_feed_nielsen_2024_06_26_13_production (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, nielsen_prev_episode_id string, nielsen_prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, nielsen_prev_channel_callsign string, nielsen_prev_network_affiliate string, nielsen_next_episode_id string, nielsen_next_title string, next_ts_start timestamp, next_ts_end timestamp, nielsen_next_channel_callsign string, nielsen_next_network_affiliate string, nielsen_live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO public.temp_mohit_r471_all_commercial_feed_nielsen_2024_06_26_13_production (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, nielsen_prev_episode_id, nielsen_prev_title, prev_ts_start, prev_ts_end, nielsen_prev_channel_callsign, nielsen_prev_network_affiliate, nielsen_next_episode_id, nielsen_next_title, next_ts_start, next_ts_end, nielsen_next_channel_callsign, nielsen_next_network_affiliate, nielsen_live, brand_name, title, duration, ip, input_category, input_device, app_service
# MAGIC )
# MAGIC WITH clients_table_to_join AS (
# MAGIC     SELECT *
# MAGIC     FROM detection.clients 
# MAGIC     WHERE 
# MAGIC         client_name = 'kinetiq'
# MAGIC )
# MAGIC , activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'nielsen'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'nielsen'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC ),
# MAGIC epg_schedule_latest AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM stage.detection.epg_schedule_latest sch
# MAGIC     WHERE sch.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC ),
# MAGIC epg_program_aggregate AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM detection.vizio_epg_program_aggregate 
# MAGIC ),
# MAGIC viewing_content_firehose AS (
# MAGIC     SELECT DISTINCT fk_tvid, fk_station_id, media_time_start, session_start, session_end, airdate, fk_content_id, is_live
# MAGIC     FROM stage.detection.viewing_content_firehose AS content
# MAGIC     WHERE session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC ),
# MAGIC content_ids_firehose AS (
# MAGIC     SELECT * FROM detection.content_ids_firehose AS cid
# MAGIC     WHERE content_id IN (
# MAGIC         SELECT DISTINCT c.fk_content_id
# MAGIC         FROM stage.detection.viewing_content_firehose AS c
# MAGIC         WHERE c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC             AND c.session_start <= '2024-06-14T21:00:00'::timestamp
# MAGIC     )
# MAGIC ),
# MAGIC station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM public.temp_mohit_station_distribution_blacklist
# MAGIC         WHERE client_name <> 'nielsen'
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%nielsen%'
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
# MAGIC         WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC         WHEN c.prev_station_id IS NOT NULL AND prev_schedule.fk_show_id IS NULL THEN NULL
# MAGIC         WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         WHEN (prev_station_id = 0) THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0]) 
# MAGIC         WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC         WHEN prev_program.database_key IS NOT NULL THEN prev_program.database_key 
# MAGIC         WHEN 'TMS' = 'TMS' AND prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
# MAGIC         ELSE NULL
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC         WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC         WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         WHEN (prev_station_id = 0) THEN prev_filecontent.title
# MAGIC         WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC         ELSE REPLACE(COALESCE(prev_program.title,
# MAGIC             CASE WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
# MAGIC                 ELSE prev_vizio_program.title
# MAGIC             END), ',', '')
# MAGIC     END, 
# MAGIC     c.prev_session_start, 
# MAGIC     c.prev_session_end, 
# MAGIC     CASE
# MAGIC         WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC         WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
# MAGIC         WHEN c.prev_station_id IS NOT NULL AND prev_program.show_id IS NULL THEN NULL
# MAGIC         WHEN c.prev_station_id IS NOT NULL THEN prev_map.inscape_call_sign
# MAGIC         WHEN (prev_station_id = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
# MAGIC         ELSE NULL
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC         WHEN c.prev_station_id IS NOT NULL AND prev_program.show_id IS NULL THEN NULL
# MAGIC         WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
# MAGIC         WHEN (prev_station.inscape_station_name IS NOT NULL)
# MAGIC             THEN prev_station.inscape_station_name
# MAGIC         WHEN (LOWER(prev_station.station_affil) LIKE '%affiliate%' OR LOWER(prev_station.station_affil) LIKE '%independent%' OR LOWER(prev_station.station_affil) LIKE '%low power%')
# MAGIC             THEN prev_station.station_affil
# MAGIC         WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
# MAGIC         WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
# MAGIC         ELSE NULL
# MAGIC     END, 
# MAGIC     NULLIF(CASE
# MAGIC         WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC         WHEN c.next_station_id IS NOT NULL AND next_schedule.fk_show_id IS NULL THEN NULL
# MAGIC         WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
# MAGIC         WHEN (next_station_id = 0) THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0]) 
# MAGIC         WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC         WHEN next_program.database_key IS NOT NULL THEN next_program.database_key
# MAGIC         WHEN 'TMS' = 'TMS' AND next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
# MAGIC         ELSE NULL
# MAGIC     END,''), 
# MAGIC     CASE
# MAGIC         WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC         WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         WHEN (next_station_id = 0) THEN next_filecontent.title
# MAGIC         WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
# MAGIC         ELSE REPLACE(COALESCE(next_program.title,
# MAGIC             CASE WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
# MAGIC                 ELSE next_vizio_program.title
# MAGIC             END), ',', '')
# MAGIC     END, 
# MAGIC     c.next_session_start, 
# MAGIC     c.next_session_end, 
# MAGIC     CASE
# MAGIC         WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC         WHEN next_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
# MAGIC         WHEN c.next_station_id IS NOT NULL THEN
# MAGIC              CASE WHEN next_program.show_id IS NULL THEN NULL
# MAGIC                   ELSE next_map.inscape_call_sign END
# MAGIC         WHEN (next_station_id = 0) THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC         WHEN c.next_station_id IS NOT NULL AND next_program.show_id IS NULL THEN NULL
# MAGIC         WHEN next_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
# MAGIC         WHEN (next_station.inscape_station_name IS NOT NULL)
# MAGIC             THEN next_station.inscape_station_name
# MAGIC         WHEN (LOWER(next_station.station_affil) LIKE '%affiliate%' OR LOWER(next_station.station_affil) LIKE '%independent%' OR LOWER(next_station.station_affil) LIKE '%low power%')
# MAGIC             THEN next_station.station_affil
# MAGIC         WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898  THEN 'OBFUSCATED'
# MAGIC         WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
# MAGIC         ELSE NULL
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC         WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED'
# MAGIC             AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
# MAGIC         WHEN c.prev_station_id IS NOT NULL AND prev_program.show_id IS NULL THEN NULL
# MAGIC         WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC     END, 
# MAGIC     REPLACE(m.brand_name, ',', ''), 
# MAGIC     REPLACE(m.title, ',', ''), 
# MAGIC     m.duration, 
# MAGIC     ip.ip_address, 
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
# MAGIC     FROM stage.detection.viewing_commercials_firehose AS c
# MAGIC     JOIN detection.zoo AS z 
# MAGIC         ON c.fk_zoo_id = z.zoo_id
# MAGIC         -- AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     INNER JOIN
# MAGIC         detection.tv AS tv
# MAGIC         ON c.fk_tvid = tv.tvid
# MAGIC         AND tv.oem = 'VIZIO'
# MAGIC     JOIN
# MAGIC         stage.detection.tv_populations AS tp
# MAGIC         ON c.fk_tvid = tp.fk_tvid 
# MAGIC     JOIN
# MAGIC         stage.detection.populations AS pop
# MAGIC         ON tp.fk_population_id = pop.population_id 
# MAGIC         AND LOWER(pop.population_name) = 'opted_in'
# MAGIC     JOIN
# MAGIC         detection.commercial_id_external_firehose AS m
# MAGIC         ON c.fk_commercial_id = m.fk_commercial_id
# MAGIC     JOIN
# MAGIC         clients_table_to_join cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC     JOIN
# MAGIC         detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     JOIN stage.detection.tv_input_stats_firehose  tvis    
# MAGIC         ON c.session_start >= tvis.create_timestamp 
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tvis.fk_tvid   
# MAGIC         AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     JOIN
# MAGIC         stage.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC         AND tv_settings.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC     JOIN detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     LEFT OUTER JOIN
# MAGIC         detection.dma AS dma ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS prev_vizio_station
# MAGIC         ON c.prev_vizio_epg_station = prev_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
# MAGIC         ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS next_vizio_station 
# MAGIC         ON c.next_vizio_epg_station = next_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
# MAGIC         ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS prev_content 
# MAGIC         ON c.fk_tvid = prev_content.fk_tvid
# MAGIC         AND prev_content.session_start = c.prev_session_start
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS prev_map
# MAGIC         ON prev_map.inscape_station_id = c.prev_station_id
# MAGIC         AND prev_map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS prev_station  
# MAGIC         ON prev_map.mapped_vendor_station_id = prev_station.station_id  
# MAGIC         AND prev_station.vendor_name = prev_map.mapped_vendor 
# MAGIC     -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
# MAGIC       ON c.prev_station_id = prev_station_blacklist.station_id
# MAGIC       AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS prev_station_obfs
# MAGIC       ON c.prev_station_id = prev_station_obfs.station_id
# MAGIC       AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
# MAGIC     -- End of new code
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS prev_schedule 
# MAGIC         ON prev_map.mapped_vendor_station_id= prev_schedule.fk_station_id
# MAGIC         AND prev_station.vendor_name = prev_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) >= prev_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) < prev_schedule.airdate_end
# MAGIC         AND prev_schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS prev_program
# MAGIC         ON prev_schedule.fk_show_id = prev_program.show_id
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS prev_program_alt
# MAGIC         ON c.prev_show_id = prev_program_alt.show_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS prev_filecontent 
# MAGIC         ON c.prev_show_id = prev_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS next_filecontent 
# MAGIC         ON c.next_show_id = next_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS m_filter 
# MAGIC         ON m_filter.fk_content_id = prev_content.fk_content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as prev_cid on c.prev_show_id = prev_cid.content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as next_cid on c.next_show_id = next_cid.content_id
# MAGIC     LEFT OUTER JOIN detection.clients cl2
# MAGIC         ON m_filter.fk_client_id = cl2.client_id
# MAGIC         AND cl2.client_name <> 'kinetiq'
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS next_content 
# MAGIC         ON c.fk_tvid = next_content.fk_tvid
# MAGIC         AND next_content.session_start = c.next_session_start  -- Bug Fix
# MAGIC         AND next_content.airdate IS NOT NULL
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS next_map
# MAGIC         ON next_map.inscape_station_id = c.next_station_id
# MAGIC         AND next_map.mapped_vendor = 'TMS' 
# MAGIC     -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
# MAGIC       ON c.next_station_id = next_station_blacklist.station_id
# MAGIC       AND next_station_blacklist.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS next_station_obfs
# MAGIC       ON c.next_station_id = next_station_obfs.station_id
# MAGIC       AND next_station_obfs.vendor_name = next_map.mapped_vendor
# MAGIC     -- End of new code
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS next_station 
# MAGIC         ON next_map.mapped_vendor_station_id = next_station.station_id 
# MAGIC         AND next_station.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS next_schedule 
# MAGIC         ON next_map.mapped_vendor_station_id= next_schedule.fk_station_id 
# MAGIC         AND next_station.vendor_name = next_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) >= next_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) < next_schedule.airdate_end
# MAGIC         AND next_schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS next_program
# MAGIC         ON next_schedule.fk_show_id = next_program.show_id
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS next_program_alt
# MAGIC         ON c.next_show_id = next_program_alt.show_id
# MAGIC     LEFT OUTER JOIN stage.detection.tv_inputsource tis    
# MAGIC         ON  c.session_start >=  (tis.create_timestamp::double)::timestamp
# MAGIC         AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND tis.create_timestamp <= ('2024-06-14T21:00:00'::timestamp::double)::timestamp
# MAGIC         AND tis.next_create_timestamp >= ('2024-06-13T21:00:00'::timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid 
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN activity_obfuscation appb 
# MAGIC         ON coalesce(tis.app_name) = appb.app_name 
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb
# MAGIC         ON coalesce(tis.app_name) = acrb.app_name
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist prev_chanb 
# MAGIC         ON prev_vizio_station.name = prev_chanb.channel_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist next_chanb 
# MAGIC         ON next_vizio_station.name = next_chanb.channel_name
# MAGIC     LEFT OUTER JOIN
# MAGIC         stage.detection.tv_ip_address AS ip
# MAGIC         ON c.session_start >= ip.create_timestamp
# MAGIC         AND c.session_start < ip.next_create_timestamp
# MAGIC         AND ip.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = ip.fk_tvid
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC         AND c.partition_key >= '2024-06-13T21:00:00'::timestamp::DATE
# MAGIC         AND c.partition_key <= '2024-06-14T21:00:00'::timestamp::DATE
# MAGIC         AND CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS public.temp_mohit_r450_commercial_dataplusmath_2024_06_26_13_production (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name_null string, title_null string, duration_null string, ip string, input_category string, input_device string, app_service string
# MAGIC     )
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO public.temp_mohit_r450_commercial_dataplusmath_2024_06_26_13_production (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name_null, title_null, duration_null, ip, input_category, input_device, app_service
# MAGIC )
# MAGIC WITH clients_table_to_join AS (
# MAGIC     SELECT *
# MAGIC     FROM detection.clients 
# MAGIC     WHERE 
# MAGIC         CASE WHEN 'dataplusmath' = 'nielsen' THEN client_name IN ('kinetiq', 'nielsen')
# MAGIC         ELSE client_name = 'dataplusmath' END
# MAGIC )
# MAGIC ,nielsen_replacement_national_nyc_alias AS (
# MAGIC     SELECT station_id, fk_show_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM detection.nielsen_replacement_national_nyc
# MAGIC     GROUP BY 1,2,3,4
# MAGIC )
# MAGIC ,nielsen_replacement_local_alias AS (
# MAGIC     SELECT station_id, fk_show_id, dma_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM detection.nielsen_replacement_local
# MAGIC     GROUP BY 1,2,3,4,5
# MAGIC )
# MAGIC , activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'dataplusmath'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'dataplusmath'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC ),
# MAGIC epg_schedule_latest AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM stage.detection.epg_schedule_latest sch
# MAGIC     WHERE sch.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC ),
# MAGIC epg_program_aggregate AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM detection.vizio_epg_program_aggregate 
# MAGIC ),
# MAGIC viewing_content_firehose AS (
# MAGIC     SELECT DISTINCT fk_tvid, fk_station_id, media_time_start, session_start, session_end, airdate, fk_content_id, is_live
# MAGIC     FROM stage.detection.viewing_content_firehose AS content
# MAGIC     WHERE session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC ),
# MAGIC content_ids_firehose AS (
# MAGIC     SELECT * FROM detection.content_ids_firehose AS cid
# MAGIC     WHERE content_id IN (
# MAGIC         SELECT DISTINCT c.fk_content_id
# MAGIC         FROM stage.detection.viewing_content_firehose AS c
# MAGIC         WHERE c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC             AND c.session_start <= '2024-06-14T21:00:00'::timestamp
# MAGIC     )
# MAGIC -- 2024-06-26 Peacock Addition start
# MAGIC ), station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM public.temp_mohit_station_distribution_blacklist
# MAGIC         WHERE client_name <> 'dataplusmath'
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%dataplusmath%'
# MAGIC )
# MAGIC -- 2024-06-26 Peacock Addition end
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
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  WHEN c.prev_station_id IS NOT NULL THEN prev_map.inscape_call_sign
# MAGIC  WHEN (prev_station_id = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
# MAGIC  WHEN c.next_station_id IS NOT NULL THEN
# MAGIC  CASE WHEN NVL(next_program.show_id, next_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  ELSE next_map.inscape_call_sign END
# MAGIC  WHEN (next_station_id = 0) THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL
# MAGIC  ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC  END, 
# MAGIC     NULL AS brand_name, 
# MAGIC     NULL AS title, 
# MAGIC     NULL AS duration, 
# MAGIC     ip.ip_address, 
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
# MAGIC     FROM stage.detection.viewing_commercials_firehose AS c
# MAGIC     JOIN detection.zoo AS z 
# MAGIC         ON c.fk_zoo_id = z.zoo_id
# MAGIC         -- AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     INNER JOIN
# MAGIC         detection.tv AS tv
# MAGIC         ON c.fk_tvid = tv.tvid
# MAGIC         AND tv.oem = 'VIZIO'
# MAGIC     JOIN
# MAGIC         stage.detection.tv_populations AS tp
# MAGIC         ON c.fk_tvid = tp.fk_tvid 
# MAGIC     JOIN
# MAGIC         stage.detection.populations AS pop
# MAGIC         ON tp.fk_population_id = pop.population_id 
# MAGIC         AND LOWER(pop.population_name) = 'opted_in'
# MAGIC     JOIN
# MAGIC         detection.commercial_id_external_firehose AS m
# MAGIC         ON c.fk_commercial_id = m.fk_commercial_id
# MAGIC     JOIN
# MAGIC         clients_table_to_join cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC     JOIN
# MAGIC         detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     JOIN stage.detection.tv_input_stats_firehose  tvis    
# MAGIC         ON c.session_start >= tvis.create_timestamp 
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tvis.fk_tvid   
# MAGIC         AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     JOIN
# MAGIC         stage.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC         AND tv_settings.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC     JOIN detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     LEFT OUTER JOIN
# MAGIC         detection.dma AS dma ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS prev_vizio_station
# MAGIC         ON c.prev_vizio_epg_station = prev_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
# MAGIC         ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS next_vizio_station 
# MAGIC         ON c.next_vizio_epg_station = next_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
# MAGIC         ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS prev_content 
# MAGIC         ON c.fk_tvid = prev_content.fk_tvid
# MAGIC         AND prev_content.session_start = c.prev_session_start
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS prev_map
# MAGIC         ON prev_map.inscape_station_id = c.prev_station_id
# MAGIC         AND prev_map.mapped_vendor = 'TIVO'
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS prev_station  
# MAGIC         ON prev_map.mapped_vendor_station_id = prev_station.station_id  
# MAGIC         AND prev_station.vendor_name = prev_map.mapped_vendor
# MAGIC     -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
# MAGIC       ON c.prev_station_id = prev_station_blacklist.station_id
# MAGIC       AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS prev_station_obfs
# MAGIC       ON c.prev_station_id = prev_station_obfs.station_id
# MAGIC       AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
# MAGIC     -- End of new code
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS prev_schedule 
# MAGIC         ON prev_map.mapped_vendor_station_id= prev_schedule.fk_station_id
# MAGIC         AND prev_station.vendor_name = prev_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) >= prev_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) < prev_schedule.airdate_end
# MAGIC         AND prev_schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS prev_program
# MAGIC         ON prev_schedule.fk_show_id = prev_program.show_id
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS prev_program_alt
# MAGIC         ON c.prev_show_id = prev_program_alt.show_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS prev_filecontent 
# MAGIC         ON c.prev_show_id = prev_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS next_filecontent 
# MAGIC         ON c.next_show_id = next_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS m_filter 
# MAGIC         ON m_filter.fk_content_id = prev_content.fk_content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as prev_cid on c.prev_show_id = prev_cid.content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as next_cid on c.next_show_id = next_cid.content_id
# MAGIC     LEFT OUTER JOIN detection.clients cl2
# MAGIC         ON m_filter.fk_client_id = cl2.client_id
# MAGIC         AND cl2.client_name <> 'dataplusmath'
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS next_content 
# MAGIC         ON c.fk_tvid = next_content.fk_tvid
# MAGIC         AND next_content.session_start = c.next_session_start   -- Bug Fix
# MAGIC         AND next_content.airdate IS NOT NULL
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS next_map
# MAGIC         ON next_map.inscape_station_id = c.next_station_id
# MAGIC         AND next_map.mapped_vendor = 'TIVO' 
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS next_station 
# MAGIC         ON next_map.mapped_vendor_station_id = next_station.station_id 
# MAGIC         AND next_station.vendor_name = next_map.mapped_vendor
# MAGIC     -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
# MAGIC       ON c.next_station_id = next_station_blacklist.station_id
# MAGIC       AND next_station_blacklist.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS next_station_obfs
# MAGIC       ON c.next_station_id = next_station_obfs.station_id
# MAGIC       AND next_station_obfs.vendor_name = next_map.mapped_vendor
# MAGIC     -- End of new code
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS next_schedule 
# MAGIC         ON next_map.mapped_vendor_station_id= next_schedule.fk_station_id 
# MAGIC         AND next_station.vendor_name = next_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) >= next_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) < next_schedule.airdate_end
# MAGIC         AND next_schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS next_program
# MAGIC         ON next_schedule.fk_show_id = next_program.show_id
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS next_program_alt
# MAGIC         ON c.next_show_id = next_program_alt.show_id
# MAGIC     LEFT OUTER JOIN stage.detection.tv_inputsource tis    
# MAGIC         ON  c.session_start >=  (tis.create_timestamp::double)::timestamp
# MAGIC         AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND tis.create_timestamp <= ('2024-06-14T21:00:00'::timestamp::double)::timestamp
# MAGIC         AND tis.next_create_timestamp >= ('2024-06-13T21:00:00'::timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid 
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN activity_obfuscation appb 
# MAGIC         ON coalesce(tis.app_name) = appb.app_name 
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb
# MAGIC         ON coalesce(tis.app_name) = acrb.app_name
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist prev_chanb 
# MAGIC         ON prev_vizio_station.name = prev_chanb.channel_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist next_chanb 
# MAGIC         ON next_vizio_station.name = next_chanb.channel_name
# MAGIC     LEFT OUTER JOIN
# MAGIC         stage.detection.tv_ip_address AS ip
# MAGIC         ON c.session_start >= ip.create_timestamp
# MAGIC         AND c.session_start < ip.next_create_timestamp
# MAGIC         AND ip.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN detection.nielsen_only_distribution_blacklist AS prev_nielsen_blacklist
# MAGIC         ON prev_nielsen_blacklist.station_id = prev_map.inscape_station_id 
# MAGIC         AND c.session_start >= prev_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < prev_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN detection.nielsen_only_distribution_blacklist AS next_nielsen_blacklist
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
# MAGIC         c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC         AND c.partition_key >= '2024-06-13T21:00:00'::timestamp::DATE
# MAGIC         AND c.partition_key <= '2024-06-14T21:00:00'::timestamp::DATE
# MAGIC         AND CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS public.temp_mohit_r450_commercial_nbc_2024_06_26_13_production (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name_null string, title_null string, duration_null string, ip string, input_category string, input_device string, app_service string
# MAGIC     )
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO public.temp_mohit_r450_commercial_nbc_2024_06_26_13_production (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name_null, title_null, duration_null, ip, input_category, input_device, app_service
# MAGIC )
# MAGIC WITH clients_table_to_join AS (
# MAGIC     SELECT *
# MAGIC     FROM detection.clients 
# MAGIC     WHERE 
# MAGIC         CASE WHEN 'NBC' = 'nielsen' THEN client_name IN ('kinetiq', 'nielsen')
# MAGIC         ELSE client_name = 'NBC' END
# MAGIC )
# MAGIC ,nielsen_replacement_national_nyc_alias AS (
# MAGIC     SELECT station_id, fk_show_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM detection.nielsen_replacement_national_nyc
# MAGIC     GROUP BY 1,2,3,4
# MAGIC )
# MAGIC ,nielsen_replacement_local_alias AS (
# MAGIC     SELECT station_id, fk_show_id, dma_id, tuner_channel_id, tuner_program_id
# MAGIC     FROM detection.nielsen_replacement_local
# MAGIC     GROUP BY 1,2,3,4,5
# MAGIC )
# MAGIC , activity_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'NBC'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'NBC'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC ),
# MAGIC epg_schedule_latest AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM stage.detection.epg_schedule_latest sch
# MAGIC     WHERE sch.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC ),
# MAGIC epg_program_aggregate AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM detection.vizio_epg_program_aggregate 
# MAGIC ),
# MAGIC viewing_content_firehose AS (
# MAGIC     SELECT DISTINCT fk_tvid, fk_station_id, media_time_start, session_start, session_end, airdate, fk_content_id, is_live
# MAGIC     FROM stage.detection.viewing_content_firehose AS content
# MAGIC     WHERE session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC ),
# MAGIC content_ids_firehose AS (
# MAGIC     SELECT * FROM detection.content_ids_firehose AS cid
# MAGIC     WHERE content_id IN (
# MAGIC         SELECT DISTINCT c.fk_content_id
# MAGIC         FROM stage.detection.viewing_content_firehose AS c
# MAGIC         WHERE c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC             AND c.session_start <= '2024-06-14T21:00:00'::timestamp
# MAGIC     )
# MAGIC -- 2024-06-26 Peacock Addition start
# MAGIC ), station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM public.temp_mohit_station_distribution_blacklist
# MAGIC         WHERE client_name <> 'NBC'
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%NBC%'
# MAGIC )
# MAGIC -- 2024-06-26 Peacock Addition end
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
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  WHEN c.prev_station_id IS NOT NULL THEN prev_map.inscape_call_sign
# MAGIC  WHEN (prev_station_id = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
# MAGIC  ELSE NULL
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
# MAGIC  WHEN c.next_station_id IS NOT NULL THEN
# MAGIC  CASE WHEN NVL(next_program.show_id, next_program_alt.show_id) IS NULL THEN NULL 
# MAGIC  ELSE next_map.inscape_call_sign END
# MAGIC  WHEN (next_station_id = 0) THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
# MAGIC  END, 
# MAGIC     CASE
# MAGIC  WHEN (cl2.client_id is not NULL) THEN NULL
# MAGIC  WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
# MAGIC  WHEN next_station_obfs.station_id IS NOT NULL THEN NULL                                                                                                    -- 2024-06-26 Peacock Addition
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
# MAGIC  WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC  WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL
# MAGIC  ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
# MAGIC  END, 
# MAGIC     NULL AS brand_name, 
# MAGIC     NULL AS title, 
# MAGIC     NULL AS duration, 
# MAGIC     ip.ip_address, 
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
# MAGIC     FROM stage.detection.viewing_commercials_firehose AS c
# MAGIC     JOIN detection.zoo AS z 
# MAGIC         ON c.fk_zoo_id = z.zoo_id
# MAGIC         -- AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     INNER JOIN
# MAGIC         detection.tv AS tv
# MAGIC         ON c.fk_tvid = tv.tvid
# MAGIC         AND tv.oem = 'VIZIO'
# MAGIC     JOIN
# MAGIC         stage.detection.tv_populations AS tp
# MAGIC         ON c.fk_tvid = tp.fk_tvid 
# MAGIC     JOIN
# MAGIC         stage.detection.populations AS pop
# MAGIC         ON tp.fk_population_id = pop.population_id 
# MAGIC         AND LOWER(pop.population_name) = 'opted_in'
# MAGIC     JOIN
# MAGIC         detection.commercial_id_external_firehose AS m
# MAGIC         ON c.fk_commercial_id = m.fk_commercial_id
# MAGIC     JOIN
# MAGIC         clients_table_to_join cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC     JOIN
# MAGIC         detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     JOIN stage.detection.tv_input_stats_firehose  tvis    
# MAGIC         ON c.session_start >= tvis.create_timestamp 
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tvis.fk_tvid   
# MAGIC         AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     JOIN
# MAGIC         stage.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC         AND tv_settings.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC     JOIN detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     LEFT OUTER JOIN
# MAGIC         detection.dma AS dma ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS prev_vizio_station
# MAGIC         ON c.prev_vizio_epg_station = prev_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
# MAGIC         ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS next_vizio_station 
# MAGIC         ON c.next_vizio_epg_station = next_vizio_station.station_id
# MAGIC     LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
# MAGIC         ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
# MAGIC         AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
# MAGIC         AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS prev_content 
# MAGIC         ON c.fk_tvid = prev_content.fk_tvid
# MAGIC         AND prev_content.session_start = c.prev_session_start
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS prev_map
# MAGIC         ON prev_map.inscape_station_id = c.prev_station_id
# MAGIC         AND prev_map.mapped_vendor = 'TIVO'
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS prev_station  
# MAGIC         ON prev_map.mapped_vendor_station_id = prev_station.station_id  
# MAGIC         AND prev_station.vendor_name = prev_map.mapped_vendor
# MAGIC     -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
# MAGIC       ON c.prev_station_id = prev_station_blacklist.station_id
# MAGIC       AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS prev_station_obfs
# MAGIC       ON c.prev_station_id = prev_station_obfs.station_id
# MAGIC       AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
# MAGIC     -- End of new code
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS prev_schedule 
# MAGIC         ON prev_map.mapped_vendor_station_id= prev_schedule.fk_station_id
# MAGIC         AND prev_station.vendor_name = prev_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) >= prev_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) < prev_schedule.airdate_end
# MAGIC         AND prev_schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS prev_program
# MAGIC         ON prev_schedule.fk_show_id = prev_program.show_id
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS prev_program_alt
# MAGIC         ON c.prev_show_id = prev_program_alt.show_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS prev_filecontent 
# MAGIC         ON c.prev_show_id = prev_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS next_filecontent 
# MAGIC         ON c.next_show_id = next_filecontent.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS m_filter 
# MAGIC         ON m_filter.fk_content_id = prev_content.fk_content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as prev_cid on c.prev_show_id = prev_cid.content_id
# MAGIC     LEFT OUTER JOIN content_ids_firehose as next_cid on c.next_show_id = next_cid.content_id
# MAGIC     LEFT OUTER JOIN detection.clients cl2
# MAGIC         ON m_filter.fk_client_id = cl2.client_id
# MAGIC         AND cl2.client_name <> 'NBC'
# MAGIC     LEFT OUTER JOIN viewing_content_firehose AS next_content 
# MAGIC         ON c.fk_tvid = next_content.fk_tvid
# MAGIC         AND next_content.session_start = c.next_session_start   -- Bug Fix
# MAGIC         AND next_content.airdate IS NOT NULL
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS next_map
# MAGIC         ON next_map.inscape_station_id = c.next_station_id
# MAGIC         AND next_map.mapped_vendor = 'TIVO' 
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS next_station 
# MAGIC         ON next_map.mapped_vendor_station_id = next_station.station_id 
# MAGIC         AND next_station.vendor_name = next_map.mapped_vendor
# MAGIC     -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
# MAGIC       ON c.next_station_id = next_station_blacklist.station_id
# MAGIC       AND next_station_blacklist.vendor_name = next_map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS next_station_obfs
# MAGIC       ON c.next_station_id = next_station_obfs.station_id
# MAGIC       AND next_station_obfs.vendor_name = next_map.mapped_vendor
# MAGIC     -- End of new code
# MAGIC     LEFT OUTER JOIN epg_schedule_latest AS next_schedule 
# MAGIC         ON next_map.mapped_vendor_station_id= next_schedule.fk_station_id 
# MAGIC         AND next_station.vendor_name = next_schedule.vendor_name 
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) >= next_schedule.airdate  
# MAGIC         AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) < next_schedule.airdate_end
# MAGIC         AND next_schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS next_program
# MAGIC         ON next_schedule.fk_show_id = next_program.show_id
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS next_program_alt
# MAGIC         ON c.next_show_id = next_program_alt.show_id
# MAGIC     LEFT OUTER JOIN stage.detection.tv_inputsource tis    
# MAGIC         ON  c.session_start >=  (tis.create_timestamp::double)::timestamp
# MAGIC         AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND tis.create_timestamp <= ('2024-06-14T21:00:00'::timestamp::double)::timestamp
# MAGIC         AND tis.next_create_timestamp >= ('2024-06-13T21:00:00'::timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid 
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN activity_obfuscation appb 
# MAGIC         ON coalesce(tis.app_name) = appb.app_name 
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb
# MAGIC         ON coalesce(tis.app_name) = acrb.app_name
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist prev_chanb 
# MAGIC         ON prev_vizio_station.name = prev_chanb.channel_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist next_chanb 
# MAGIC         ON next_vizio_station.name = next_chanb.channel_name
# MAGIC     LEFT OUTER JOIN
# MAGIC         stage.detection.tv_ip_address AS ip
# MAGIC         ON c.session_start >= ip.create_timestamp
# MAGIC         AND c.session_start < ip.next_create_timestamp
# MAGIC         AND ip.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN detection.nielsen_only_distribution_blacklist AS prev_nielsen_blacklist
# MAGIC         ON prev_nielsen_blacklist.station_id = prev_map.inscape_station_id 
# MAGIC         AND c.session_start >= prev_nielsen_blacklist.blacklist_start 
# MAGIC         AND c.session_start < prev_nielsen_blacklist.blacklist_end 
# MAGIC     LEFT OUTER JOIN detection.nielsen_only_distribution_blacklist AS next_nielsen_blacklist
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
# MAGIC         c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC         AND c.partition_key >= '2024-06-13T21:00:00'::timestamp::DATE
# MAGIC         AND c.partition_key <= '2024-06-14T21:00:00'::timestamp::DATE
# MAGIC         AND CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Content + Null

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS public.temp_mohit_r449_content_with_null_605_2024_06_26_13_production (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO public.temp_mohit_r449_content_with_null_605_2024_06_26_13_production (
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
# MAGIC     FROM detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = '605'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = '605'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM public.temp_mohit_station_distribution_blacklist
# MAGIC         WHERE client_name <> '605'
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%605%'
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
# MAGIC             CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC                  WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC     WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN '605' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
# MAGIC         WHEN (cl.client_id is not null) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         WHEN '605' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
# MAGIC         ELSE schedule.airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL                                                                                                 -- 2024-06-26 Peacock Addition
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN '605' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN '605' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         ELSE map.inscape_call_sign 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         WHEN '605' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN '605' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
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
# MAGIC         CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL                                                                                                      -- 2024-06-26 Peacock Addition
# MAGIC         WHEN '605' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN '605' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC         WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC         THEN station.station_affil ELSE NULL END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC         WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         WHEN '605' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL 
# MAGIC         WHEN '605' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
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
# MAGIC     stage.detection.viewing_content_firehose AS c
# MAGIC     JOIN detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         -- AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN stage.detection.tv AS tv ON c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN stage.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC     JOIN stage.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     JOIN stage.detection.tv_populations AS u
# MAGIC         ON c.fk_tvid = u.fk_tvid 
# MAGIC     JOIN detection.populations AS pop
# MAGIC         ON u.fk_population_id = pop.population_id 
# MAGIC         AND pop.population_name = 'opted_in'
# MAGIC     JOIN detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     LEFT OUTER JOIN detection.dma AS dma
# MAGIC         ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN detection.input_source inps 
# MAGIC         ON c.fk_input_source_id = inps.input_source_id
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS map
# MAGIC         ON map.inscape_station_id = c.fk_station_id
# MAGIC         AND map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS station
# MAGIC         ON map.mapped_vendor_station_id = station.station_id 
# MAGIC         AND station.vendor_name = map.mapped_vendor  
# MAGIC     -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
# MAGIC       ON c.fk_station_id = station_blacklist.station_id
# MAGIC       AND station_blacklist.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS station_obfs
# MAGIC       ON c.fk_station_id = station_obfs.station_id
# MAGIC       AND station_obfs.vendor_name = map.mapped_vendor
# MAGIC -- End of new code
# MAGIC     LEFT OUTER JOIN detection.epg_schedule_latest AS schedule
# MAGIC         ON map.mapped_vendor_station_id = schedule.fk_station_id
# MAGIC         AND schedule.vendor_name = map.mapped_vendor
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) >= schedule.airdate  
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) <  schedule.airdate_end
# MAGIC         AND schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS show
# MAGIC         ON schedule.fk_show_id = show.show_id
# MAGIC         AND show.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = c.fk_show_id 
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS vizio_station 
# MAGIC         ON c.vizio_epg_station = vizio_station.station_id
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_program_aggregate AS vizio_program
# MAGIC         ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
# MAGIC         AND c.vizio_epg_program != '0'
# MAGIC     JOIN detection.content_ids_firehose AS cid
# MAGIC         ON cid.content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS m
# MAGIC         ON m.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.clients cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC         AND cl.client_name <> '605'
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = '605'
# MAGIC     JOIN stage.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN stage.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-06-14T21:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-06-13T21:00:00'::timestamp::double)::timestamp 
# MAGIC     LEFT OUTER JOIN activity_obfuscation AS appb
# MAGIC         ON tis.app_name = appb.app_name
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb 
# MAGIC         ON tis.app_name = acrb.app_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist chanb 
# MAGIC         ON vizio_station.name = chanb.channel_name
# MAGIC     LEFT OUTER JOIN
# MAGIC         detection.tv_ip_address AS ip
# MAGIC         ON c.session_start >= ip.create_timestamp
# MAGIC         AND c.session_start < ip.next_create_timestamp
# MAGIC         AND ip.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         and c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         and c.session_start < nielsen_blacklist.blacklist_end  
# MAGIC         AND '605' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     LEFT OUTER JOIN detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND backup_show.show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC         AND '605' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     LEFT OUTER JOIN detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND backup_show.show_id  = rep_nyc_nat.fk_show_id 
# MAGIC         AND '605' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-06-14T21:00:00'::timestamp
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
# MAGIC CREATE TABLE IF NOT EXISTS public.temp_mohit_r449_content_with_null_nbc_2024_06_26_13_production (
# MAGIC     tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO public.temp_mohit_r449_content_with_null_nbc_2024_06_26_13_production (
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
# MAGIC     FROM detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_activity_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'NBC'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , viewing_obfuscation AS (
# MAGIC     SELECT blocked_apps.app_name
# MAGIC     FROM detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC     LEFT JOIN detection.app_customer_viewing_distribution_override override
# MAGIC         ON blocked_apps.app_name = override.app_name
# MAGIC         AND override.client_name = 'NBC'
# MAGIC     WHERE override.app_name IS NULL
# MAGIC )
# MAGIC , station_distribution_blacklist AS (
# MAGIC     WITH agg AS (
# MAGIC         SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
# MAGIC         FROM public.temp_mohit_station_distribution_blacklist
# MAGIC         WHERE client_name <> 'NBC'
# MAGIC         GROUP BY 1, 2)
# MAGIC     SELECT station_id, vendor_name
# MAGIC     FROM agg
# MAGIC     WHERE cl_list NOT ILIKE '%NBC%'
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
# MAGIC             CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC                  WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
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
# MAGIC     WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC     WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC     ELSE CASE WHEN c.file_ingested THEN NULL 
# MAGIC         WHEN 'NBC' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
# MAGIC         ELSE REPLACE(show.title, ',', '') END 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
# MAGIC         WHEN (cl.client_id is not null) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         WHEN 'NBC' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
# MAGIC         ELSE schedule.airdate
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL                                                                                                 -- 2024-06-26 Peacock Addition
# MAGIC         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC         WHEN 'NBC' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'NBC' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         ELSE map.inscape_call_sign 
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
# MAGIC         WHEN (cl.client_id is not null) THEN NUll
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         WHEN 'NBC' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'NBC' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
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
# MAGIC         CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_obfs.station_id IS NOT NULL THEN NULL                                                                                                      -- 2024-06-26 Peacock Addition
# MAGIC         WHEN 'NBC' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
# MAGIC         WHEN 'NBC' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
# MAGIC         WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
# MAGIC         WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC         THEN station.station_affil ELSE NULL END
# MAGIC     END, 
# MAGIC     CASE
# MAGIC         WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
# MAGIC         WHEN c.vizio_epg_station IS NOT NULL THEN 't'
# MAGIC         WHEN (cl.client_id IS NOT NULL) THEN NULL
# MAGIC         WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
# MAGIC         WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL -- 2024-06-26 techdebt change
# MAGIC         WHEN station_blacklist.station_id IS NOT NULL THEN NULL                                                                                            -- 2024-06-26 Peacock Addition
# MAGIC         WHEN 'NBC' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL 
# MAGIC         WHEN 'NBC' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
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
# MAGIC     stage.detection.viewing_content_firehose AS c
# MAGIC     JOIN detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
# MAGIC         -- AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC     JOIN stage.detection.tv AS tv ON c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv.tvid 
# MAGIC         AND tv.oem = 'VIZIO'   
# MAGIC     JOIN stage.detection.tv_settings AS tv_settings
# MAGIC         ON c.session_start >= tv_settings.create_timestamp
# MAGIC         AND c.session_start < tv_settings.next_create_timestamp
# MAGIC         AND tv_settings.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tv_settings.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND c.fk_tvid = tv_settings.fk_tvid
# MAGIC     JOIN stage.detection.settings AS settings
# MAGIC         ON tv_settings.fk_settings_id = settings.settings_id
# MAGIC         AND UPPER(settings.country_name) = 'USA'
# MAGIC     JOIN stage.detection.tv_populations AS u
# MAGIC         ON c.fk_tvid = u.fk_tvid 
# MAGIC     JOIN detection.populations AS pop
# MAGIC         ON u.fk_population_id = pop.population_id 
# MAGIC         AND pop.population_name = 'opted_in'
# MAGIC     JOIN detection.location AS location
# MAGIC         ON c.fk_location_id = location.location_id
# MAGIC         AND UPPER(location.country_code) = 'US'
# MAGIC     LEFT OUTER JOIN detection.dma AS dma
# MAGIC         ON c.fk_dma_id = dma.dma_id
# MAGIC     LEFT OUTER JOIN detection.input_source inps 
# MAGIC         ON c.fk_input_source_id = inps.input_source_id
# MAGIC     LEFT OUTER JOIN stage.detection.inscape_station_map AS map
# MAGIC         ON map.inscape_station_id = c.fk_station_id
# MAGIC         AND map.mapped_vendor = 'TMS'
# MAGIC     LEFT OUTER JOIN stage.detection.epg_station AS station
# MAGIC         ON map.mapped_vendor_station_id = station.station_id 
# MAGIC         AND station.vendor_name = map.mapped_vendor  
# MAGIC     -- 2024-06-26 Peacock new joins addition code start
# MAGIC     LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
# MAGIC       ON c.fk_station_id = station_blacklist.station_id
# MAGIC       AND station_blacklist.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN public.temp_mohit_station_metadata_obfuscation AS station_obfs
# MAGIC       ON c.fk_station_id = station_obfs.station_id
# MAGIC       AND station_obfs.vendor_name = map.mapped_vendor
# MAGIC -- End of new code
# MAGIC     LEFT OUTER JOIN detection.epg_schedule_latest AS schedule
# MAGIC         ON map.mapped_vendor_station_id = schedule.fk_station_id
# MAGIC         AND schedule.vendor_name = map.mapped_vendor
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) >= schedule.airdate  
# MAGIC         AND timestampadd(SECOND, c.media_time_start, c.airdate) <  schedule.airdate_end
# MAGIC         AND schedule.airdate >= '2024-06-13T21:00:00'::timestamp - interval '60' day
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS show
# MAGIC         ON schedule.fk_show_id = show.show_id
# MAGIC         AND show.vendor_name = map.mapped_vendor
# MAGIC     LEFT OUTER JOIN stage.detection.epg_show AS backup_show
# MAGIC         ON backup_show.show_id = c.fk_show_id 
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_station AS vizio_station 
# MAGIC         ON c.vizio_epg_station = vizio_station.station_id
# MAGIC     LEFT OUTER JOIN detection.vizio_epg_program_aggregate AS vizio_program
# MAGIC         ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
# MAGIC         AND c.vizio_epg_program != '0'
# MAGIC     JOIN detection.content_ids_firehose AS cid
# MAGIC         ON cid.content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS m
# MAGIC         ON m.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.clients cl
# MAGIC         ON m.fk_client_id = cl.client_id
# MAGIC         AND cl.client_name <> 'NBC'
# MAGIC     LEFT OUTER JOIN detection.content_id_external_firehose AS md
# MAGIC         ON md.fk_content_id = c.fk_content_id
# MAGIC     LEFT OUTER JOIN detection.clients cli
# MAGIC         ON md.fk_client_id = cli.client_id
# MAGIC         AND cli.client_name = 'NBC'
# MAGIC     JOIN stage.detection.tv_input_stats_firehose  tvis 
# MAGIC         ON c.session_start >= tvis.create_timestamp
# MAGIC         AND c.session_start < tvis.next_create_timestamp
# MAGIC         AND tvis.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND tvis.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND  c.fk_tvid = tvis.fk_tvid
# MAGIC         AND  c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC     LEFT OUTER JOIN stage.detection.tv_inputsource tis
# MAGIC         ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
# MAGIC         AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC         AND c.fk_tvid = tis.fk_tvid
# MAGIC         AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC         AND tis.create_timestamp <= ('2024-06-14T21:00:00'::timestamp::double)::timestamp 
# MAGIC         AND tis.next_create_timestamp >= ('2024-06-13T21:00:00'::timestamp::double)::timestamp 
# MAGIC     LEFT OUTER JOIN activity_obfuscation AS appb
# MAGIC         ON tis.app_name = appb.app_name
# MAGIC     LEFT OUTER JOIN viewing_obfuscation AS acrb 
# MAGIC         ON tis.app_name = acrb.app_name 
# MAGIC     LEFT OUTER JOIN 
# MAGIC         detection.free_channels_distribution_blacklist chanb 
# MAGIC         ON vizio_station.name = chanb.channel_name
# MAGIC     LEFT OUTER JOIN
# MAGIC         detection.tv_ip_address AS ip
# MAGIC         ON c.session_start >= ip.create_timestamp
# MAGIC         AND c.session_start < ip.next_create_timestamp
# MAGIC         AND ip.create_timestamp <= '2024-06-14T21:00:00'::timestamp
# MAGIC         AND ip.next_create_timestamp >= '2024-06-13T21:00:00'::timestamp
# MAGIC         AND tv.tvid = ip.fk_tvid
# MAGIC     LEFT OUTER JOIN detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC         ON nielsen_blacklist.station_id = map.inscape_station_id 
# MAGIC         and c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC         and c.session_start < nielsen_blacklist.blacklist_end  
# MAGIC         AND 'NBC' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     LEFT OUTER JOIN detection.nielsen_replacement_local AS rep_local
# MAGIC         ON map.inscape_station_id = rep_local.station_id 
# MAGIC         AND c.airdate = rep_local.airdate
# MAGIC         AND backup_show.show_id = rep_local.fk_show_id 
# MAGIC         AND c.fk_dma_id = rep_local.dma_id
# MAGIC         AND 'NBC' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     LEFT OUTER JOIN detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC         ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC         AND c.airdate = rep_nyc_nat.airdate
# MAGIC         AND backup_show.show_id  = rep_nyc_nat.fk_show_id 
# MAGIC         AND 'NBC' != 'nielsen' -- 2024-06-26 techdebt change
# MAGIC     WHERE
# MAGIC         c.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC     AND c.session_start < '2024-06-14T21:00:00'::timestamp
# MAGIC     AND CASE c.file_ingested
# MAGIC         WHEN true THEN
# MAGIC             CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
# MAGIC             WHEN true THEN SPLIT(cid.content_cid, '_')[1]
# MAGIC             ELSE NULL
# MAGIC             END
# MAGIC         ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
# MAGIC         END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)

# COMMAND ----------

# MAGIC %md
# MAGIC 4. All Channels

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS public.temp_mohit_r319_all_channels_nielsen_2024_06_25_07_production (
# MAGIC     all_channel_callsign string, all_channel_name string, all_channel_dma string, all_channel_affil string, all_channel_time_zone string, all_channel_local_national string, nielsen_ingested string, nielsen_hd string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO public.temp_mohit_r319_all_channels_nielsen_2024_06_25_07_production (
# MAGIC     all_channel_callsign, 
# MAGIC     all_channel_name, 
# MAGIC     all_channel_dma, 
# MAGIC     all_channel_affil, 
# MAGIC     all_channel_time_zone, 
# MAGIC     all_channel_local_national, 
# MAGIC     nielsen_ingested, 
# MAGIC     nielsen_hd
# MAGIC )
# MAGIC WITH ism AS(
# MAGIC     SELECT
# MAGIC         inscape_station_id,
# MAGIC         inscape_call_sign,
# MAGIC         mapped_vendor,
# MAGIC         mapped_vendor_call_sign,
# MAGIC         mapped_vendor_station_id,
# MAGIC         to_lmdb,
# MAGIC         created_at,
# MAGIC         updated_at,
# MAGIC         inscape_station_num,
# MAGIC         mapped_vendor_station_num
# MAGIC     FROM stage.detection.inscape_station_map
# MAGIC     WHERE mapped_vendor = 'TMS'
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC         inscape_station_id,
# MAGIC         inscape_call_sign,
# MAGIC         mapped_vendor,
# MAGIC         mapped_vendor_call_sign,
# MAGIC         mapped_vendor_station_id,
# MAGIC         to_lmdb,
# MAGIC         created_at,
# MAGIC         updated_at,
# MAGIC         inscape_station_num,
# MAGIC         mapped_vendor_station_num
# MAGIC     FROM (
# MAGIC         SELECT *, row_number() over (partition by mp.mapped_vendor_station_id order by mp.created_at desc) as rn
# MAGIC         FROM stage.detection.inscape_station_map mp
# MAGIC         WHERE mp.mapped_vendor = 'TIVO'
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC     REPLACE(tms_station.station_call_sign, ',', ''), 
# MAGIC     REPLACE(station.station_name, ',', ''), 
# MAGIC     REPLACE(tms_dma.dma_name, ',', ''), 
# MAGIC     CASE WHEN (station.inscape_station_name IS NOT NULL)
# MAGIC         THEN station.inscape_station_name
# MAGIC         WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC         THEN station.station_affil ELSE NULL END, 
# MAGIC     REPLACE(station.station_time_zone, ',', ''), 
# MAGIC     station.local_or_national, 
# MAGIC     CASE WHEN station.ingested = 'TRUE' THEN 'Ingested and Attributed'
# MAGIC  WHEN station.ingested <> 'TRUE' AND map.to_lmdb = 'TRUE' THEN 'Attributed'
# MAGIC  WHEN map.to_lmdb = 'FALSE' THEN 'Tuner' 
# MAGIC  END, 
# MAGIC     CASE WHEN station.hd = 'TRUE' THEN 'High Definition'
# MAGIC         ELSE 'Standard Definition' END
# MAGIC     FROM stage.detection.epg_station AS station
# MAGIC     JOIN 
# MAGIC     (
# MAGIC         SELECT DISTINCT station_call_sign
# MAGIC         FROM 
# MAGIC         (
# MAGIC             SELECT st.*, row_number() over (partition by st.station_num order by st.created_at desc, coalesce(st.lmdb, '1900-01-01 00:00:00'::timestamp) desc) AS rn
# MAGIC             FROM stage.detection.epg_station st
# MAGIC             INNER JOIN stage.detection.epg_schedule AS sch
# MAGIC                 ON st.station_id = sch.fk_station_id
# MAGIC             WHERE sch.vendor_name = 'TMS' 
# MAGIC                 AND st.vendor_name = 'TMS'
# MAGIC                 AND sch.airdate > CURRENT_DATE + INTERVAL '7 days'
# MAGIC         ) WHERE rn = 1
# MAGIC     ) AS call_signs ON station.station_call_sign = call_signs.station_call_sign
# MAGIC     JOIN ism AS map
# MAGIC         ON map.mapped_vendor_station_id = station.station_id
# MAGIC         AND station.vendor_name = map.mapped_vendor
# MAGIC         AND map.mapped_vendor = 'TMS'
# MAGIC     LEFT JOIN stage.detection.inscape_station_map AS tms_map
# MAGIC         ON map.inscape_station_id = tms_map.inscape_station_id
# MAGIC         AND tms_map.mapped_vendor = 'TMS'
# MAGIC     LEFT JOIN (
# MAGIC         SELECT DISTINCT tms_station.*
# MAGIC         FROM stage.detection.epg_station AS tms_station
# MAGIC         JOIN (
# MAGIC             SELECT DISTINCT station_call_sign
# MAGIC             FROM (
# MAGIC                 SELECT st.*, row_number() over (partition by st.station_num order by st.created_at desc, coalesce(st.lmdb, '1900-01-01 00:00:00'::timestamp) desc) AS rn
# MAGIC                 FROM stage.detection.epg_station st
# MAGIC                 INNER JOIN stage.detection.epg_schedule AS sch
# MAGIC                     ON st.station_id = sch.fk_station_id
# MAGIC                 WHERE sch.vendor_name = 'TMS'
# MAGIC                     AND st.vendor_name = 'TMS'
# MAGIC                     AND sch.airdate > CURRENT_DATE + INTERVAL '13 days'
# MAGIC             ) WHERE rn = 1
# MAGIC         ) AS call_signs ON tms_station.station_call_sign = call_signs.station_call_sign
# MAGIC     ) AS tms_station
# MAGIC         ON tms_map.mapped_vendor_station_id = tms_station.station_id
# MAGIC         AND tms_map.mapped_vendor = tms_station.vendor_name
# MAGIC     LEFT JOIN detection.dma AS tms_dma 
# MAGIC         ON tms_station.fk_dma_id = tms_dma.dma_id 
# MAGIC     LEFT JOIN public.temp_mohit_station_metadata_obfuscation AS station_obfs    -- 2024-06-26 Peacock Addition
# MAGIC       ON map.inscape_station_id = station_obfs.station_id               -- 2024-06-26 Peacock Addition
# MAGIC       AND station_obfs.vendor_name = 'TMS'                              -- 2024-06-26 Peacock Addition
# MAGIC     WHERE 
# MAGIC         map.to_lmdb = 'TRUE'
# MAGIC         AND station_obfs.station_id IS NULL                             -- 2024-06-26 Peacock Addition
# MAGIC     ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS public.temp_mohit_r247_all_channels_comscore_2024_06_25_07_production (
# MAGIC     all_channel_callsign string, all_channel_name string, all_channel_dma string, all_channel_affil string, all_channel_time_zone string, all_channel_local_national string, all_channel_ingested string, all_channel_hd string
# MAGIC     );
# MAGIC
# MAGIC INSERT INTO public.temp_mohit_r247_all_channels_comscore_2024_06_25_07_production (
# MAGIC     all_channel_callsign, 
# MAGIC     all_channel_name, 
# MAGIC     all_channel_dma, 
# MAGIC     all_channel_affil, 
# MAGIC     all_channel_time_zone, 
# MAGIC     all_channel_local_national, 
# MAGIC     all_channel_ingested, 
# MAGIC     all_channel_hd
# MAGIC )
# MAGIC WITH ism AS(
# MAGIC     SELECT
# MAGIC         inscape_station_id,
# MAGIC         inscape_call_sign,
# MAGIC         mapped_vendor,
# MAGIC         mapped_vendor_call_sign,
# MAGIC         mapped_vendor_station_id,
# MAGIC         to_lmdb,
# MAGIC         created_at,
# MAGIC         updated_at,
# MAGIC         inscape_station_num,
# MAGIC         mapped_vendor_station_num
# MAGIC     FROM stage.detection.inscape_station_map
# MAGIC     WHERE mapped_vendor = 'TMS'
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC         inscape_station_id,
# MAGIC         inscape_call_sign,
# MAGIC         mapped_vendor,
# MAGIC         mapped_vendor_call_sign,
# MAGIC         mapped_vendor_station_id,
# MAGIC         to_lmdb,
# MAGIC         created_at,
# MAGIC         updated_at,
# MAGIC         inscape_station_num,
# MAGIC         mapped_vendor_station_num
# MAGIC     FROM (
# MAGIC         SELECT *, row_number() over (partition by mp.mapped_vendor_station_id order by mp.created_at desc) as rn
# MAGIC         FROM stage.detection.inscape_station_map mp
# MAGIC         WHERE mp.mapped_vendor = 'TIVO'
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC     REPLACE(tms_station.station_call_sign, ',', ''), 
# MAGIC     REPLACE(station.station_name, ',', ''), 
# MAGIC     REPLACE(tms_dma.dma_name, ',', ''), 
# MAGIC     CASE WHEN (station.inscape_station_name IS NOT NULL)
# MAGIC         THEN station.inscape_station_name
# MAGIC         WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%independent%'
# MAGIC             OR LOWER(station.station_affil) LIKE '%low power%')
# MAGIC         THEN station.station_affil ELSE NULL END, 
# MAGIC     REPLACE(station.station_time_zone, ',', ''), 
# MAGIC     station.local_or_national, 
# MAGIC     CASE WHEN 'comscore' != 'nielsen' AND nodb.station_id IS NOT NULL THEN 'Attributed'
# MAGIC  WHEN station.ingested = 'TRUE' THEN 'Ingested and Attributed'
# MAGIC  WHEN station.ingested <> 'TRUE' AND map.to_lmdb = 'TRUE' THEN 'Attributed'
# MAGIC  WHEN map.to_lmdb = 'FALSE' THEN 'Tuner'
# MAGIC  END, 
# MAGIC     CASE WHEN 'comscore' != 'nielsen' THEN
# MAGIC         CASE WHEN (station.ingested='TRUE' AND nodb.station_id IS NULL) THEN
# MAGIC             CASE WHEN station.hd = 'TRUE' THEN 'High Definition'
# MAGIC                 ELSE 'Standard Definition'
# MAGIC             END
# MAGIC         END
# MAGIC         WHEN 'comscore' = 'nielsen' THEN
# MAGIC             CASE WHEN station.hd = 'TRUE' THEN 'High Definition'
# MAGIC                 ELSE 'Standard Definition' 
# MAGIC     END END
# MAGIC     FROM stage.detection.epg_station AS station
# MAGIC     JOIN 
# MAGIC     (
# MAGIC         SELECT DISTINCT station_call_sign
# MAGIC         FROM 
# MAGIC         (
# MAGIC             SELECT st.*, row_number() over (partition by st.station_num order by st.created_at desc, coalesce(st.lmdb, '1900-01-01 00:00:00'::timestamp) desc) AS rn
# MAGIC             FROM stage.detection.epg_station st
# MAGIC             INNER JOIN stage.detection.epg_schedule AS sch
# MAGIC                 ON st.station_id = sch.fk_station_id
# MAGIC             WHERE sch.vendor_name = 'TIVO' 
# MAGIC                 AND st.vendor_name = 'TIVO'
# MAGIC                 AND sch.airdate > CURRENT_DATE + INTERVAL '7 days'
# MAGIC         ) WHERE rn = 1
# MAGIC     ) AS call_signs ON station.station_call_sign = call_signs.station_call_sign
# MAGIC     JOIN ism AS map
# MAGIC         ON map.mapped_vendor_station_id = station.station_id
# MAGIC         AND station.vendor_name = map.mapped_vendor
# MAGIC         AND map.mapped_vendor = 'TIVO'
# MAGIC     LEFT JOIN stage.detection.inscape_station_map AS tms_map
# MAGIC         ON map.inscape_station_id = tms_map.inscape_station_id
# MAGIC         AND tms_map.mapped_vendor = 'TMS'
# MAGIC     LEFT JOIN detection.nielsen_only_distribution_blacklist nodb
# MAGIC         ON nodb.station_id = tms_map.mapped_vendor_station_id
# MAGIC         AND nodb.blacklist_start <= CURRENT_DATE
# MAGIC         AND nodb.blacklist_end > CURRENT_DATE
# MAGIC     LEFT JOIN (
# MAGIC         SELECT DISTINCT tms_station.*
# MAGIC         FROM stage.detection.epg_station AS tms_station
# MAGIC         JOIN (
# MAGIC             SELECT DISTINCT station_call_sign
# MAGIC             FROM (
# MAGIC                 SELECT st.*, row_number() over (partition by st.station_num order by st.created_at desc, coalesce(st.lmdb, '1900-01-01 00:00:00'::timestamp) desc) AS rn
# MAGIC                 FROM stage.detection.epg_station st
# MAGIC                 INNER JOIN stage.detection.epg_schedule AS sch
# MAGIC                     ON st.station_id = sch.fk_station_id
# MAGIC                 WHERE sch.vendor_name = 'TMS'
# MAGIC                     AND st.vendor_name = 'TMS'
# MAGIC                     AND sch.airdate > CURRENT_DATE + INTERVAL '13 days'
# MAGIC             ) WHERE rn = 1
# MAGIC         ) AS call_signs ON tms_station.station_call_sign = call_signs.station_call_sign
# MAGIC     ) AS tms_station
# MAGIC         ON tms_map.mapped_vendor_station_id = tms_station.station_id
# MAGIC         AND tms_map.mapped_vendor = tms_station.vendor_name
# MAGIC     LEFT JOIN detection.dma AS tms_dma 
# MAGIC         ON tms_station.fk_dma_id = tms_dma.dma_id
# MAGIC     LEFT JOIN public.temp_mohit_station_metadata_obfuscation AS station_obfs    -- 2024-06-26 Peacock Addition
# MAGIC       ON map.inscape_station_id = station_obfs.station_id               -- 2024-06-26 Peacock Addition
# MAGIC       AND station_obfs.vendor_name = map.mapped_vendor                  -- 2024-06-26 Peacock Addition
# MAGIC     WHERE 
# MAGIC         CASE WHEN nodb.station_id IS NOT NULL THEN nodb.ingest_time IS NULL ELSE TRUE END 
# MAGIC         AND 
# MAGIC         map.to_lmdb = 'TRUE'
# MAGIC         AND station_obfs.station_id IS NULL                             -- 2024-06-26 Peacock Addition
# MAGIC     ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM stage.detection.viewing_content_firehose vc
# MAGIC JOIN (SELECT station_id FROM public.temp_mohit_station_distribution_blacklist bl
# MAGIC        GROUP BY 1) bl
# MAGIC   ON bl.station_id = vc.fk_station_id
# MAGIC WHERE vc.session_start >= '2024-06-13T21:00:00'::timestamp
# MAGIC   AND vc.session_start < '2024-06-14T00:00:00'::timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM stage.detection.station_metadata_obfuscation;
# MAGIC SELECT * FROM stage.detection.station_distribution_obfuscation_overwrite;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM dev.detection.station_distribution_obfuscation_overwrite

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT INITCAP(REPLACE(c.name, '_', ' ')) AS report_type
# MAGIC , INITCAP(REPLACE(REPLACE(SPLIT_PART(REPLACE(a.period, '"'), ':', 2), '\\', ''), '}', '')) AS period
# MAGIC , a.epg_vendor
# MAGIC , COUNT(*) AS num_reports
# MAGIC , COUNT(DISTINCT b.name) AS num_customers
# MAGIC FROM dev.mohit_gangwani.rm_reports_dbricks a
# MAGIC JOIN dev.mohit_gangwani.rm_report_types c
# MAGIC   ON a.reporttype = c.id
# MAGIC JOIN dev.mohit_gangwani.rm_customers b
# MAGIC   ON a.customer_id = b.id
# MAGIC WHERE frequency = 'recurring'
# MAGIC   AND enabled = true
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM detection.tv
# MAGIC WHERE token = '0fbe39830d42934aec04205428765d64'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.detection.viewing_content_firehose vc
# MAGIC WHERE fk_tvid = 164555016
# MAGIC AND session_start >= '2024-10-01' 
# MAGIC         AND session_start < '2024-11-01' 
# MAGIC         AND vc.partition_key >= '2024-10-01' 
# MAGIC         AND vc.partition_key < '2024-11-01' 

# COMMAND ----------


