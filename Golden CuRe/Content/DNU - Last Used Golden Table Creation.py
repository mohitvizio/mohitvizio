# Databricks notebook source
schema = 'dev.mohit_gangwani'
report_name = 'cure_viewing_content_golden_table'
start_time = '2025-05-05T00:00:00'
end_time = '2025-05-05T02:00:00'

# COMMAND ----------

spark.sql(f"""DROP TABLE IF EXISTS {schema}.{report_name};""")

# COMMAND ----------

# with title_sum as (
# select tun.dt_min
#        , sh.title
#        , sum(detected - accurate_live) as inaccurate_live
#        , sum(detected - accurate_live)/sum(sum(detected - accurate_live)) over(partition by dt_min) as perc_inaccurate_live
# from dev.public.tuner_acr_min_st_aggs tun
# join prod.detection.epg_station st 
# on tun.tuner_fk_station_id = st.station_id
# join prod.detection.epg_schedule_latest sch
# on tun.tuner_fk_station_id = sch.fk_station_id
# and tun.dt_min >= sch.airdate
# and tun.dt_min < timestampadd(second, sch.duration, sch.airdate)
# join prod.detection.epg_show sh
# on sch.fk_show_id = sh.show_id
# where tun.dt_min >= '2025-04-26 06:00:00'
# and tun.dt_min < '2025-04-26 12:00:00'
# group by 1, 2
# ), rn as (
# select *
#        , row_number() over (partition by dt_min order by inaccurate_live desc) as rank
# from title_sum
# )
# select *
# from rn
# where rank <= 3

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {schema}.{report_name} AS
WITH activity_obfuscation AS (
  SELECT blocked_apps.app_name, override.client_name
  FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
  LEFT JOIN prod.detection.app_customer_activity_distribution_override override
    ON blocked_apps.app_name = override.app_name
  GROUP BY 1, 2
),
viewing_obfuscation AS (
  SELECT blocked_apps.app_name, override.client_name
  FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
  LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
    ON blocked_apps.app_name = override.app_name
  GROUP BY 1, 2
)
, nielsen_replacement_national_nyc_alias AS (
    SELECT rl.station_id, rl.fk_show_id, rl.tuner_channel_id, rl.tuner_program_id, rl.airdate
    FROM prod.detection.nielsen_replacement_national_nyc AS rl
    JOIN prod.detection.nielsen_only_distribution_blacklist AS bl
      ON bl.station_id = rl.station_id
     AND bl.blacklist_end >= '{start_time}'
    GROUP BY ALL
)
, nielsen_replacement_local_alias AS (
    SELECT rl.station_id, rl.fk_show_id, rl.dma_id, rl.tuner_channel_id, rl.tuner_program_id, rl.airdate
    FROM prod.detection.nielsen_replacement_local AS rl
    JOIN prod.detection.nielsen_only_distribution_blacklist AS bl
      ON bl.station_id = rl.station_id
     AND bl.blacklist_end >= '{start_time}'
    GROUP BY ALL
)
, vod_stations AS (
  SELECT station_id, vendor_name
  FROM prod.detection.station_distribution_obfuscation_overwrite
  GROUP BY 1, 2
)
, inscape_station_map_dedupe AS (
  SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
  FROM (
    SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor,   mapped_vendor_station_id ORDER BY created_at DESC) AS rn
    FROM detection.inscape_station_map) ism
  WHERE ism.rn = 1
)
SELECT tvid, fk_tvid, zipcode, dma
, tms_episode_id, tivo_episode_id
, tms_title, tivo_title
, tms_airdate, tivo_airdate
, tms_channel_callsign, tivo_channel_callsign
, mt_start, session_start, session_end
, tms_channel_affiliate, tivo_channel_affiliate
, is_live
, ip_address, input_category, input_device, app_service
, audio_acr, dma_code, vizio_epg_channel_id, vizio_epg_program_id
, vizio_epg_not_null
, nielsen_exclusive
, content_only_condition
, vod_station
, '|'||array_join(collect_set(acrb_client), '|')||'|' AS acrb_clients
, '|'||array_join(collect_set(appb_client), '|')||'|' AS appb_clients
, '|'||array_join(collect_set(client_id), '|')||'|' AS client_id_not_null
FROM (
  SELECT DISTINCT COALESCE(tv.long_tvid, tv.vizio_tvid) AS tvid
  , c.fk_tvid
  , NULLIF(location.zipcode, '') AS zipcode
  , REPLACE(dma.dma_name, ',', '') AS dma
  ------------------ Episode ID -------------------
  , CASE WHEN c.vizio_epg_station IS NOT NULL THEN
          CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL 
               ELSE vizio_program.program_tms_id END
         WHEN c.file_ingested = true THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
         ELSE tms_show.database_key
    END AS tms_episode_id
  
  , CASE WHEN c.vizio_epg_station IS NOT NULL THEN NULL
         WHEN c.file_ingested = true THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
         ELSE tivo_show.database_key
    END AS tivo_episode_id
  ------------------------------------------------------------
  ------------------ Show Title -------------------
  , CASE WHEN c.vizio_epg_station IS NOT NULL THEN
          CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL 
               WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != '' THEN vizio_program.  series_aggregate_title
               ELSE vizio_program.title END
         WHEN c.file_ingested THEN NULL
         ELSE REPLACE(tms_show.title, ',', '')
    END AS tms_title
  
  , CASE WHEN c.vizio_epg_station IS NOT NULL THEN
          CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL 
               WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != '' THEN vizio_program.  series_aggregate_title
               ELSE vizio_program.title END
         WHEN c.file_ingested THEN NULL
         ELSE REPLACE(tivo_show.title, ',', '')
    END AS tivo_title
  ------------------------------------------------------------
  ------------------ Airdate -------------------
  , CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL
         WHEN c.vizio_epg_station IS NOT NULL THEN c.tms_airdate
         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
         ELSE c.tms_airdate
    END AS tms_airdate
  , CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL
         WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
         ELSE c.airdate
    END AS tivo_airdate
  ------------------------------------------------------------
  ------------------ Channel Call Sign -------------------
  , CASE WHEN tms_station_obfs.station_id IS NOT NULL THEN NULL
         WHEN c.vizio_epg_station IS NOT NULL THEN tms_map.inscape_call_sign
         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
         ELSE tms_map.inscape_call_sign
    END AS tms_channel_callsign
  , CASE WHEN tivo_station_obfs.station_id IS NOT NULL THEN NULL
         WHEN c.vizio_epg_station IS NOT NULL THEN tivo_map.inscape_call_sign
         WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
         ELSE tivo_map.inscape_call_sign
    END AS tivo_channel_callsign
  ------------------------------------------------------------
  ------------------ Media Time Start -------------------
  , CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL 
         WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
         ELSE LEAST(c.media_time_start, c.runtime)
    END AS mt_start
  ------------------------------------------------------------
  , c.session_start
  , c.session_end
  ------------------------------------------------------------
  ------------------ Station Affiliate -------------------
  , CASE WHEN c.vizio_epg_station IS NOT NULL THEN
             CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN 'OBFUSCATED' 
                  ELSE vizio_station.name END
         WHEN tms_station_obfs.station_id IS NOT NULL THEN NULL
         WHEN tms_station.inscape_station_name IS NOT NULL THEN tms_station.inscape_station_name
         WHEN LOWER(tms_station.station_affil) LIKE '%affiliate%'
              OR LOWER(tms_station.station_affil) LIKE '%independent%'
              OR LOWER(tms_station.station_affil) LIKE '%low power%' THEN tms_station.station_affil
    END AS tms_channel_affiliate
  
  , CASE WHEN c.vizio_epg_station IS NOT NULL THEN
             CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN 'OBFUSCATED' 
                  ELSE vizio_station.name END
         WHEN tivo_station_obfs.station_id IS NOT NULL THEN NULL
         WHEN tivo_station.inscape_station_name IS NOT NULL THEN tivo_station.inscape_station_name
         WHEN LOWER(tivo_station.station_affil) LIKE '%affiliate%'
           OR LOWER(tivo_station.station_affil) LIKE '%independent%'
           OR LOWER(tivo_station.station_affil) LIKE '%low power%' THEN tivo_station.station_affil
    END AS tivo_channel_affiliate
  ------------------------------------------------------------
  ------------------ Live -------------------
  , CASE WHEN c.vizio_epg_station IS NOT NULL THEN 't'
         WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
         WHEN c.is_live = TRUE THEN 't'
         WHEN c.is_live = FALSE THEN 'f'
    END AS is_live
  ------------------------------------------------------------
  , ip.ip_address
  , tvis.category AS input_category
  , tvis.input_device
  , CASE WHEN UPPER(tvis.category) = 'APPS' THEN
          CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
               WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
               WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora',  'tv games') AND cid.content_cid <> 'unknown' THEN NULL
               WHEN lower(tis.app_name) = 'unknown' THEN NULL
               ELSE tis.app_name
          END
         WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4',  'playstation 5','roku') THEN 'vMVPD'
    END AS app_service
  ------------------------------------------------------------
  ---------------Additional Fields ---------------------------
  , CASE WHEN settings.enableaudioacr = 1 THEN 't' ELSE 'f' END AS audio_acr
  , dma.dma_code AS dma_code
  , CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL
         ELSE vizio_station.station_id
    END AS vizio_epg_channel_id
  , CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN NULL
         ELSE vizio_program.program_aggregate_id
    END AS vizio_epg_program_id
  ------------------ Conditions -------------------
  ------Bools-------
  , CASE WHEN c.vizio_epg_station IS NOT NULL THEN TRUE ELSE FALSE END AS vizio_epg_not_null
  , CASE WHEN COALESCE(tivo_nielsen_blacklist.station_id, tms_nielsen_blacklist.station_id) IS NOT NULL
          AND (COALESCE(tivo_rep_local.station_id, tivo_rep_nyc_nat.station_id, tms_rep_local.station_id, tms_rep_nyc_nat.station_id) IS NULL
               OR COALESCE(tms_nielsen_blacklist.ingest_time, tivo_nielsen_blacklist.ingest_time) IS NOT NULL) THEN TRUE
         ELSE FALSE
    END AS nielsen_exclusive
  , CASE WHEN cid.content_cid = 'unknown' AND vizio_station.name IS NULL THEN TRUE ELSE FALSE
    END AS content_only_condition
  , CASE WHEN COALESCE(tivo_vod_stations.station_id, tms_vod_stations.station_id) IS NOT NULL THEN TRUE ELSE FALSE
    END AS vod_station
  ------Later Aggs-------
  , CASE WHEN acrb.app_name IS NOT NULL AND c.vizio_epg_station IS NULL THEN CASE WHEN acrb.client_name IS NULL THEN 'ALL' ELSE acrb.client_name END
    END AS acrb_client
  , CASE WHEN appb.app_name IS NOT NULL AND c.vizio_epg_station IS NULL THEN CASE WHEN appb.client_name IS NULL THEN 'ALL' ELSE appb.client_name END
    END AS appb_client
  , cl.client_name AS client_id
  ------------------------------------------------------------
  -- Joins that do not need to be modified
  FROM prod.detection.viewing_content_firehose AS c
  JOIN prod.detection.zoo AS z
    ON c.fk_zoo_id = z.zoo_id
   AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
  JOIN prod.detection.tv AS tv
    ON c.fk_tvid = tv.tvid
   AND tv.oem = 'VIZIO'
  -- Location
  JOIN prod.detection.tv_settings AS tv_settings
    ON c.session_start >= tv_settings.create_timestamp
   AND c.session_start < tv_settings.next_create_timestamp
   AND tv_settings.create_timestamp <= '{end_time}'::timestamp
   AND tv_settings.next_create_timestamp >= '{start_time}'::timestamp
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
  -- Content
  JOIN prod.detection.content_ids_firehose AS cid
    ON cid.content_id = c.fk_content_id
  -- IP Address
  LEFT OUTER JOIN prod.detection.tv_ip_address AS ip
    ON c.session_start >= ip.create_timestamp
   AND c.session_start < ip.next_create_timestamp
   AND ip.create_timestamp <= '{end_time}'::timestamp
   AND ip.next_create_timestamp >= '{start_time}'::timestamp
   AND tv.tvid = ip.fk_tvid
  -- Vizio Joins
  LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station
    ON TRY_CAST(c.vizio_epg_station AS STRING) = TRY_CAST(vizio_station.station_id AS STRING)
  LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
    ON TRY_CAST(c.vizio_epg_program AS STRING) = TRY_CAST(vizio_program.program_aggregate_id AS STRING)
   AND TRY_CAST(c.vizio_epg_program AS STRING) NOT IN ('0', '', '-1')
  LEFT OUTER JOIN prod.detection.free_channels_distribution_blacklist AS chanb
    ON vizio_station.name = chanb.channel_name
  -- Input Joins
  LEFT OUTER JOIN prod.detection.input_source AS inps
    ON c.fk_input_source_id = inps.input_source_id
  JOIN prod.detection.tv_input_stats_firehose AS tvis
    ON c.session_start >= tvis.create_timestamp
   AND c.session_start < tvis.next_create_timestamp
   AND tvis.create_timestamp <= '{end_time}'::timestamp
   AND tvis.next_create_timestamp >= '{start_time}'::timestamp
   AND  c.fk_tvid = tvis.fk_tvid
   AND  c.fk_input_source_id = tvis.fk_input_source_id
  LEFT OUTER JOIN prod.detection.tv_inputsource AS tis
    ON c.session_start >= (tis.create_timestamp::double)::timestamp
   AND c.session_start < (tis.next_create_timestamp::double)::timestamp
   AND c.fk_tvid = tis.fk_tvid
   AND c.fk_input_source_id = tis.fk_input_source_id
   AND tis.create_timestamp <= ('{end_time}'::timestamp::double)::timestamp
   AND tis.next_create_timestamp >= ('{start_time}'::timestamp::double)::timestamp
  -- App blacklist
  LEFT OUTER JOIN activity_obfuscation AS appb
    ON tis.app_name = appb.app_name
  LEFT OUTER JOIN viewing_obfuscation AS acrb
    ON tis.app_name = acrb.app_name
  ---------------------------------------------
  -- Client Specific
  LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
    ON m.fk_content_id = c.fk_content_id
  LEFT OUTER JOIN prod.detection.clients AS cl
    ON m.fk_client_id = cl.client_id
  LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
    ON md.fk_content_id = c.fk_content_id
  LEFT OUTER JOIN prod.detection.clients AS cli
    ON md.fk_client_id = cli.client_id
  ---------------------------------------------
  -- TiVo TMS specific joins
  LEFT OUTER JOIN inscape_station_map_dedupe AS tivo_map
    ON tivo_map.mapped_vendor_station_id = c.fk_station_id
   AND tivo_map.mapped_vendor = 'TIVO'
  LEFT OUTER JOIN prod.detection.epg_station AS tivo_station
    ON tivo_station.station_id = c.fk_station_id
   AND tivo_station.vendor_name = 'TIVO'
  LEFT OUTER JOIN prod.detection.epg_show AS tivo_show
    ON tivo_show.show_id = c.fk_show_id
   AND tivo_show.vendor_name = 'TIVO'
  
  LEFT OUTER JOIN prod.detection.epg_show AS tms_show
    ON tms_show.show_id = c.tms_show_id
   AND tms_show.vendor_name = 'TMS'
  LEFT OUTER JOIN prod.detection.epg_station AS tms_station
    ON tms_station.station_id = c.tms_station_id
   AND tms_station.vendor_name = 'TMS'
  LEFT OUTER JOIN inscape_station_map_dedupe AS tms_map
    ON tms_map.mapped_vendor_station_id = c.tms_station_id
   AND tms_map.mapped_vendor = 'TMS'
  ---------------------------------------------
  -- Blacklist Joins
  LEFT OUTER JOIN vod_stations AS tivo_vod_stations
    ON tivo_vod_stations.station_id = tivo_map.inscape_station_id
   AND tivo_vod_stations.vendor_name = 'TIVO'
  LEFT OUTER JOIN vod_stations AS tms_vod_stations
    ON tms_vod_stations.station_id = tms_map.inscape_station_id
   AND tms_vod_stations.vendor_name = 'TMS'

  LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS tivo_station_obfs
    ON tivo_station_obfs.vendor_station_id = tivo_map.inscape_station_id
  LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS tms_station_obfs
    ON tms_station_obfs.vendor_station_id = tms_map.inscape_station_id

  LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS tivo_nielsen_blacklist
    ON tivo_nielsen_blacklist.station_id = tivo_map.inscape_station_id
   AND c.session_start >= tivo_nielsen_blacklist.blacklist_start
   AND c.session_start < tivo_nielsen_blacklist.blacklist_end
  LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS tms_nielsen_blacklist
    ON tms_nielsen_blacklist.station_id = tms_map.inscape_station_id
   AND c.session_start >= tms_nielsen_blacklist.blacklist_start
   AND c.session_start < tms_nielsen_blacklist.blacklist_end

  LEFT OUTER JOIN nielsen_replacement_local_alias AS tivo_rep_local
    ON tivo_rep_local.station_id = tivo_map.inscape_station_id
   AND tivo_rep_local.airdate = c.airdate
   AND tivo_rep_local.fk_show_id = c.fk_show_id
   AND tivo_rep_local.dma_id = c.fk_dma_id
  LEFT OUTER JOIN nielsen_replacement_local_alias AS tms_rep_local
    ON tms_rep_local.station_id = tms_map.inscape_station_id
   AND tms_rep_local.airdate = c.tms_airdate
   AND tms_rep_local.fk_show_id = c.tms_show_id
   AND tms_rep_local.dma_id = c.fk_dma_id

  LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS tivo_rep_nyc_nat
    ON tivo_rep_nyc_nat.station_id = tivo_map.inscape_station_id
   AND tivo_rep_nyc_nat.airdate = c.airdate
   AND tivo_rep_nyc_nat.fk_show_id = c.fk_show_id
  LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS tms_rep_nyc_nat
    ON tms_rep_nyc_nat.station_id = tms_map.inscape_station_id
   AND tms_rep_nyc_nat.airdate = c.tms_airdate
   AND tms_rep_nyc_nat.fk_show_id = c.tms_show_id
  ---------------------------------------------
  WHERE c.session_start >= '{start_time}'::timestamp
    AND c.session_start < '{end_time}'::timestamp
    AND CASE c.file_ingested
      WHEN true THEN
          CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT_PART(cid.content_cid, '_', 3), '') IS NULL
          WHEN true THEN SPLIT(cid.content_cid, '_')[1]
          ELSE NULL
          END
      ELSE COALESCE(tivo_map.inscape_call_sign, tms_map.inscape_call_sign, 'KeepSessionForNullReport')
      END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
)
GROUP BY tvid, fk_tvid, zipcode, dma, tms_episode_id, tivo_episode_id, tms_title, tivo_title, tms_airdate, tivo_airdate, tms_channel_callsign, tivo_channel_callsign, mt_start, session_start, session_end, tms_channel_affiliate, tivo_channel_affiliate, is_live, ip_address, input_category, input_device, app_service, audio_acr, dma_code, vizio_epg_channel_id, vizio_epg_program_id, vizio_epg_not_null, nielsen_exclusive, content_only_condition, vod_station;
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.mohit_gangwani.cure_viewing_content_golden_table
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.detection.viewing_content_golden
# MAGIC WHERE session_start >= CURRENT_DATE
# MAGIC AND tms_epi_title IS NOT NULL
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM detection.tv
# MAGIC ORDER BY joined_date desc
# MAGIC limit 199
