# Databricks notebook source
start_time = '2025-04-10 00:00:00'
end_time = '2025-04-10 02:00:00'
schema_table_name = 'dev.mohit_gangwani.testing_golden_commercials_table'

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS {schema_table_name};
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {schema_table_name} AS
WITH commercial_id_external_firehose AS (
  SELECT m.fk_commercial_id, m.external_id, m.brand_name, m.title, m.duration, cl.client_name
  FROM prod.detection.commercial_id_external_firehose AS m
  JOIN prod.detection.clients cl
    ON m.fk_client_id = cl.client_id
  WHERE cl.client_name IN ('adimpact', 'kinetiq', 'nielsen', 'comscore')
  GROUP BY ALL
)
, nielsen_replacement_national_nyc_alias AS (
    SELECT rl.station_id, rl.fk_show_id, rl.tuner_channel_id, rl.tuner_program_id
    FROM prod.detection.nielsen_replacement_national_nyc AS rl
    JOIN prod.detection.nielsen_only_distribution_blacklist AS bl
      ON bl.station_id = rl.station_id
     AND bl.blacklist_end >= '{start_time}'
    GROUP BY 1,2,3,4
)
, nielsen_replacement_local_alias AS (
    SELECT rl.station_id, rl.fk_show_id, rl.dma_id, rl.tuner_channel_id, rl.tuner_program_id
    FROM prod.detection.nielsen_replacement_local AS rl
    JOIN prod.detection.nielsen_only_distribution_blacklist AS bl
      ON bl.station_id = rl.station_id
     AND bl.blacklist_end >= '{start_time}'
    GROUP BY 1,2,3,4,5
)
, activity_obfuscation AS (
  SELECT blocked_apps.app_name, override.client_name
  FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
  LEFT JOIN prod.detection.app_customer_activity_distribution_override AS override
    ON blocked_apps.app_name = override.app_name
  GROUP BY 1, 2
)
, viewing_obfuscation AS (
  SELECT blocked_apps.app_name, override.client_name
  FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
  LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
  ON blocked_apps.app_name = override.app_name
  GROUP BY 1, 2
)
, epg_program_aggregate AS (
  SELECT DISTINCT *
  FROM prod.detection.vizio_epg_program_aggregate
)
, viewing_content_firehose AS (
  SELECT DISTINCT fk_tvid, session_start, session_end, fk_content_id, is_live
  FROM prod.detection.viewing_content_firehose AS content
  WHERE content.session_start >= '{start_time}'::timestamp
      AND content.session_start < '{end_time}'::timestamp
      AND content.partition_key >= '{start_time}'::timestamp::DATE
      AND content.partition_key <= '{end_time}'::timestamp::DATE
)
, content_ids_firehose AS (
  SELECT * FROM detection.content_ids_firehose AS cid
  WHERE content_id IN (
      SELECT DISTINCT c.fk_content_id
      FROM viewing_content_firehose AS c
  )
)
-- , station_distribution_blacklist AS (
--   SELECT station_id, vendor_name
--   FROM prod.detection.station_distribution_obfuscation_overwrite
--   GROUP BY 1, 2
-- )
, inscape_map_deduped AS (
  WITH station_distribution_blacklist AS (
    SELECT station_id, vendor_name
    FROM prod.detection.station_distribution_obfuscation_overwrite
    GROUP BY 1, 2
  )
  SELECT ism.inscape_station_id
    , ism.inscape_call_sign
    , ism.mapped_vendor
    , ism.mapped_vendor_station_id
    , st.inscape_station_name
    , st.station_affil
    , station_blacklist.vendor_name IS NOT NULL AS station_blacklisted
    , station_obfs.vendor_name IS NOT NULL AS station_obfuscated
    , nielsen_blacklist.station_id IS NOT NULL AS nielsen_blacklisted_station
    , nielsen_blacklist.ingest_time IS NOT NULL AS nielsen_blacklisted_ingest_not_null
    FROM (
        SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
        FROM prod.detection.inscape_station_map
    ) ism
    JOIN prod.detection.epg_station st
      ON st.station_id = ism.mapped_vendor_station_id
     AND st.vendor_name = ism.mapped_vendor
    LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
      ON station_blacklist.station_id = ism.inscape_station_id
     AND station_blacklist.vendor_name = ism.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
      ON station_obfs.station_id = ism.inscape_station_id
     AND station_obfs.vendor_name = ism.mapped_vendor
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
      ON nielsen_blacklist.station_id = ism.inscape_station_id
     AND '{start_time}'::timestamp >= nielsen_blacklist.blacklist_start
     AND '{start_time}'::timestamp < nielsen_blacklist.blacklist_end
    WHERE ism.rn = 1
    GROUP BY ALL
)
SELECT tvid, fk_tvid, zipcode, dma, external_id, mt_start, session_start, session_end
, NULLIF(tms_prev_episode_id, '') AS tms_prev_episode_id
, NULLIF(tivo_prev_episode_id, '') AS tivo_prev_episode_id
, NULLIF(tms_next_episode_id, '') AS tms_next_episode_id
, NULLIF(tivo_next_episode_id, '') AS tivo_next_episode_id
, tms_prev_show_title, tivo_prev_show_title
, tms_next_show_title, tivo_next_show_title
, tms_prev_channel_callsign, tivo_prev_channel_callsign
, tms_next_channel_callsign, tivo_next_channel_callsign
, tms_prev_network_affiliate, tivo_prev_network_affiliate
, tms_next_network_affiliate, tivo_next_network_affiliate
, prev_ts_start, prev_ts_end
, next_ts_start, next_ts_end
, live, brand_name, title, duration
, ip, input_category, input_device, app_service
, audio_acr, dma_code, vizio_epg_channel_id, vizio_epg_program_id
, prev_vizio_epg_not_null
, prev_nielsen_exclusive, next_nielsen_exclusive
, prev_station_vod, next_station_vod
, commercial_client
, '|'||array_join(collect_set(acrb_client), '|')||'|' AS acrb_clients
, '|'||array_join(collect_set(appb_client), '|')||'|' AS appb_clients
, '|'||array_join(collect_set(excluded_client), '|')||'|' AS excluded_client_list
FROM (
  SELECT DISTINCT COALESCE(tv.long_tvid, tv.vizio_tvid) AS tvid
  , c.fk_tvid
  , NULLIF(location.zipcode, '') AS zipcode
  , REPLACE(dma.dma_name, ',', '') AS dma
  , m.external_id
  , c.media_time_start AS mt_start
  , c.session_start
  , c.session_end
  ------------------ Episode ID -------------------
  , CASE WHEN COALESCE(c.tms_prev_station_id, c.prev_station_id) = 0 THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0])
         WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = '98989898989898' THEN NULL
         WHEN tms_prev_program.database_key IS NOT NULL THEN tms_prev_program.database_key
         WHEN prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
    END AS tms_prev_episode_id
  , CASE WHEN COALESCE(c.tms_prev_station_id, c.prev_station_id) = 0 THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0])
         WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = '98989898989898' THEN NULL
         WHEN tivo_prev_program.database_key IS NOT NULL THEN tivo_prev_program.database_key
    END AS tivo_prev_episode_id

  , CASE WHEN COALESCE(c.next_station_id, c.tms_next_station_id) = 0 THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0])
         WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = '98989898989898' THEN NULL
         WHEN tms_next_program.database_key IS NOT NULL THEN tms_next_program.database_key
         WHEN next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
    END AS tms_next_episode_id
  , CASE WHEN COALESCE(c.next_station_id, c.tms_next_station_id) = 0 THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0])
         WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = '98989898989898' THEN NULL
         WHEN tivo_next_program.database_key IS NOT NULL THEN tivo_next_program.database_key
    END AS tivo_next_episode_id
  ------------------ Show Title -------------------
  , CASE WHEN COALESCE(c.tms_prev_station_id, c.prev_station_id) = 0 THEN prev_filecontent.title
         WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = '98989898989898' THEN NULL
         WHEN tms_prev_program.title IS NOT NULL THEN tms_prev_program.title
         WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
         ELSE prev_vizio_program.title
    END AS tms_prev_show_title
  , CASE WHEN COALESCE(c.tms_next_station_id, c.next_station_id) = 0 THEN next_filecontent.title
         WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = '98989898989898' THEN NULL
         WHEN tms_next_program.title IS NOT NULL THEN tms_next_program.title
         WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
         ELSE next_vizio_program.title
    END AS tms_next_show_title

  , CASE WHEN COALESCE(c.prev_station_id, c.tms_prev_station_id) = 0 THEN prev_filecontent.title
         WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = '98989898989898' THEN NULL
         WHEN tivo_prev_program.title IS NOT NULL THEN tivo_prev_program.title
         WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
         ELSE prev_vizio_program.title
    END AS tivo_prev_show_title
  , CASE WHEN COALESCE(c.next_station_id, c.tms_next_station_id) = 0 THEN next_filecontent.title
         WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = '98989898989898' THEN NULL
         WHEN tivo_next_program.title IS NOT NULL THEN tivo_next_program.title
         WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
         ELSE next_vizio_program.title
    END AS tivo_next_show_title
  ------------------ Channel Call Sign -------------------
  , CASE WHEN tms_prev_station.station_obfuscated THEN NULL
         WHEN c.tms_prev_station_id IS NOT NULL THEN tms_prev_station.inscape_call_sign
         WHEN c.tms_prev_station_id = 0 THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE')
    END AS tms_prev_channel_callsign
  , CASE WHEN tms_next_station.station_obfuscated THEN NULL
         WHEN c.tms_next_station_id IS NOT NULL THEN tms_next_station.inscape_call_sign
         WHEN c.tms_next_station_id = 0 THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
    END AS tms_next_channel_callsign

  , CASE WHEN tivo_prev_station.station_obfuscated THEN NULL
         WHEN c.prev_station_id IS NOT NULL THEN tivo_prev_station.inscape_call_sign
         WHEN c.prev_station_id = 0 THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE')
    END AS tivo_prev_channel_callsign
  , CASE WHEN tivo_next_station.station_obfuscated THEN NULL
         WHEN c.next_station_id IS NOT NULL THEN tivo_next_station.inscape_call_sign
         WHEN c.next_station_id = 0 THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
    END AS tivo_next_channel_callsign
  ------------------------------------------------------------
  ------------------ Station Affiliate -----------------------
  , CASE WHEN tms_prev_station.station_obfuscated THEN NULL
         WHEN c.tms_prev_station_id IS NOT NULL THEN
             CASE WHEN (tms_prev_station.inscape_station_name IS NOT NULL) THEN tms_prev_station.inscape_station_name
                  WHEN LOWER(tms_prev_station.station_affil) LIKE '%affiliate%'
                    OR LOWER(tms_prev_station.station_affil) LIKE '%independent%'
                    OR LOWER(tms_prev_station.station_affil) LIKE '%low power%' THEN tms_prev_station.station_affil END
         WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = '98989898989898' THEN 'OBFUSCATED'
         WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
    END AS tms_prev_network_affiliate
  , CASE WHEN tms_next_station.station_obfuscated THEN NULL
         WHEN c.tms_next_station_id IS NOT NULL THEN
             CASE WHEN (tms_next_station.inscape_station_name IS NOT NULL) THEN tms_next_station.inscape_station_name
                  WHEN LOWER(tms_next_station.station_affil) LIKE '%affiliate%'
                    OR LOWER(tms_next_station.station_affil) LIKE '%independent%'
                    OR LOWER(tms_next_station.station_affil) LIKE '%low power%' THEN tms_next_station.station_affil END
         WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = '98989898989898' THEN 'OBFUSCATED'
         WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
    END AS tms_next_network_affiliate

  , CASE WHEN tivo_prev_station.station_obfuscated THEN NULL
         WHEN c.prev_station_id IS NOT NULL THEN tivo_prev_station.inscape_station_name
         WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = '98989898989898' THEN 'OBFUSCATED'
         WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
    END AS tivo_prev_network_affiliate
  , CASE WHEN tivo_next_station.station_obfuscated THEN NULL
         WHEN c.next_station_id IS NOT NULL THEN tivo_next_station.inscape_station_name
         WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = '98989898989898' THEN 'OBFUSCATED'
         WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
    END AS tivo_next_network_affiliate
  ------------ Prev and Next Session Times -------------------
  , c.prev_session_start AS prev_ts_start
  , c.prev_session_end AS prev_ts_end
  , c.next_session_start AS next_ts_start
  , c.next_session_end AS next_ts_end
  --------------------------------------------------------------
  -- Liveness, App Service, IP Address, Input Category/Device --
  , CASE WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED' AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
         ELSE CASE WHEN prev_content.is_live = TRUE THEN 't'
                   WHEN prev_content.is_live = FALSE THEN 'f'
              END
    END AS live
  , ip.ip_address AS ip
  , tvis.category AS input_category
  , tvis.input_device AS input_device
  , CASE WHEN UPPER(tvis.category) = 'APPS' THEN
          CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
               WHEN c.prev_vizio_epg_station IS NULL AND tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
               WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND COALESCE(c.prev_show_id, c.tms_prev_show_id) IS NOT NULL THEN NULL
               WHEN LOWER(tis.app_name) = 'unknown' THEN NULL
               ELSE tis.app_name
          END
         WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
    END AS app_service
  ----------------- Comm Metadata ----------------------------
  , REPLACE(m.brand_name, ',', '') AS brand_name
  , REPLACE(m.title, ',', '') AS title
  , m.duration AS duration
  ---------------Additional Fields ---------------------------
  , CASE WHEN settings.enableaudioacr = 1 THEN 't' ELSE 'f'
    END AS audio_acr
  , dma.dma_code AS dma_code
  , CASE WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = '98989898989898' THEN NULL
         ELSE prev_vizio_station.station_id
    END AS vizio_epg_channel_id
  , CASE WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = '98989898989898' THEN NULL
         ELSE prev_vizio_program.program_aggregate_id
    END AS vizio_epg_program_id
  ------------------ Conditions -------------------
  ------Bools-------
  , CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN TRUE ELSE FALSE END AS prev_vizio_epg_not_null
  , CASE WHEN c.next_vizio_epg_station IS NOT NULL THEN TRUE ELSE FALSE END AS next_vizio_epg_not_null
  , CASE WHEN (tms_prev_station.nielsen_blacklisted_station OR tivo_prev_station.nielsen_blacklisted_station)
          AND (COALESCE(tms_rep_local_prev.station_id, tms_rep_nyc_nat_prev.station_id,
                        tivo_rep_local_prev.station_id, tivo_rep_nyc_nat_prev.station_id) IS NULL
               OR (tms_prev_station.nielsen_blacklisted_ingest_not_null OR tivo_prev_station.nielsen_blacklisted_ingest_not_null)) THEN TRUE
         ELSE FALSE
    END AS prev_nielsen_exclusive
  , CASE WHEN (tms_next_station.nielsen_blacklisted_station OR tivo_next_station.nielsen_blacklisted_station)
         AND (COALESCE(tms_rep_local_next.station_id, tms_rep_nyc_nat_next.station_id,
                       tivo_rep_local_next.station_id, tivo_rep_nyc_nat_next.station_id) IS NULL
              OR (tms_next_station.nielsen_blacklisted_ingest_not_null OR tivo_next_station.nielsen_blacklisted_ingest_not_null)) THEN TRUE
         ELSE FALSE
    END AS next_nielsen_exclusive
  , CASE WHEN tivo_prev_station.station_blacklisted OR tms_prev_station.station_blacklisted THEN TRUE
         ELSE FALSE
    END AS prev_station_vod
  , CASE WHEN tivo_next_station.station_blacklisted OR tms_next_station.station_blacklisted THEN TRUE
         ELSE FALSE
    END AS next_station_vod
  ------Later Aggs-------
  , CASE WHEN UPPER(tvis.category) = 'APPS'
              AND prev_vizio_station.name IS NULL
              AND acrb.app_name IS NOT NULL THEN CASE WHEN acrb.client_name IS NULL THEN 'ALL'
                                                      ELSE acrb.client_name END
    END AS acrb_client
  , CASE WHEN UPPER(tvis.category) = 'APPS' THEN
          CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN NULL
               WHEN c.prev_vizio_epg_station IS NULL AND tis.app_name = 'WatchFree+' THEN NULL
               WHEN appb.app_name IS NOT NULL THEN CASE WHEN appb.client_name IS NULL THEN 'ALL'
                                                        ELSE appb.client_name END
          END
    END AS appb_client
  , m.client_name AS commercial_client
  , cl2.client_name AS excluded_client
  -----------------------------------------------------------------
  FROM prod.detection.viewing_commercials_firehose AS c
  -- Commercials Metadata
  JOIN commercial_id_external_firehose AS m
    ON c.fk_commercial_id = m.fk_commercial_id
  -- Joins that do not need to be modified
  JOIN prod.detection.zoo AS z
    ON c.fk_zoo_id = z.zoo_id
   AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
  JOIN prod.detection.tv AS tv
    ON c.fk_tvid = tv.tvid
   AND tv.oem = 'VIZIO'
  JOIN prod.detection.tv_populations AS tp
    ON c.fk_tvid = tp.fk_tvid
  JOIN prod.detection.populations AS pop
    ON tp.fk_population_id = pop.population_id
   AND LOWER(pop.population_name) = 'opted_in'
  JOIN prod.detection.tv_settings AS tv_settings
    ON c.session_start < tv_settings.next_create_timestamp
   AND c.session_start >= tv_settings.create_timestamp
   AND c.fk_tvid = tv_settings.fk_tvid
   AND tv_settings.create_timestamp <= '{end_time}'::timestamp
   AND tv_settings.next_create_timestamp >= '{start_time}'::timestamp
  JOIN  prod.detection.settings AS settings
    ON tv_settings.fk_settings_id = settings.settings_id
   AND UPPER(settings.country_name) = 'USA'
  -- Location 
  JOIN prod.detection.location AS location
    ON c.fk_location_id = location.location_id
   AND UPPER(location.country_code) = 'US'
  LEFT OUTER JOIN prod.detection.dma AS dma
    ON c.fk_dma_id = dma.dma_id
  -- Input Joins
  JOIN prod.detection.tv_input_stats_firehose  tvis   
    ON c.session_start >= tvis.create_timestamp
   AND c.session_start < tvis.next_create_timestamp
   AND tvis.create_timestamp <= '{end_time}'::timestamp
   AND tvis.next_create_timestamp >= '{start_time}'::timestamp
   AND c.fk_tvid = tvis.fk_tvid  
   AND c.fk_input_source_id = tvis.fk_input_source_id
  LEFT OUTER JOIN prod.detection.tv_inputsource tis   
    ON c.session_start >=  (tis.create_timestamp::double)::timestamp
   AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
   AND tis.create_timestamp <= ('{end_time}'::timestamp::double)::timestamp
   AND tis.next_create_timestamp >= ('{start_time}'::timestamp::double)::timestamp
   AND c.fk_tvid = tis.fk_tvid
   AND c.fk_input_source_id = tis.fk_input_source_id
  -- Prev Content
  LEFT OUTER JOIN viewing_content_firehose AS prev_content
    ON c.fk_tvid = prev_content.fk_tvid
   AND prev_content.session_start = c.prev_session_start
  -- IP Address
  LEFT OUTER JOIN prod.detection.tv_ip_address AS ip
    ON c.session_start >= ip.create_timestamp
   AND c.session_start < ip.next_create_timestamp
   AND ip.create_timestamp <= '{end_time}'::timestamp
   AND ip.next_create_timestamp >= '{start_time}'::timestamp
   AND c.fk_tvid = ip.fk_tvid
  --------------- WF+ Prev/Next Metadata Joins ---------------
  LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
    ON TRY_CAST(c.prev_vizio_epg_station AS STRING) <=> TRY_CAST(prev_vizio_station.station_id AS STRING)
  LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program
    ON TRY_CAST(c.prev_vizio_epg_program AS STRING) <=> TRY_CAST(prev_vizio_program.program_aggregate_id AS STRING)
   AND TRY_CAST(c.prev_vizio_epg_program AS STRING) NOT IN ('0', '', '-1')
   AND c.prev_vizio_epg_program IS NOT NULL
  LEFT OUTER JOIN prod.detection.vizio_epg_station AS next_vizio_station
    ON TRY_CAST(c.next_vizio_epg_station AS STRING) <=> TRY_CAST(next_vizio_station.station_id AS STRING)
  LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program
    ON TRY_CAST(c.next_vizio_epg_program AS STRING) <=> TRY_CAST(next_vizio_program.program_aggregate_id AS STRING)
   AND TRY_CAST(c.next_vizio_epg_program AS STRING) NOT IN ('0', '', '-1')
   AND c.next_vizio_epg_program IS NOT NULL
  ------------------------------------------------------------
  -- TiVo TMS specific joins
  LEFT OUTER JOIN inscape_map_deduped AS tivo_prev_station 
    ON tivo_prev_station.mapped_vendor_station_id = c.prev_station_id
   AND tivo_prev_station.mapped_vendor = 'TIVO'
  LEFT OUTER JOIN prod.detection.epg_show AS tivo_prev_program
    ON tivo_prev_program.show_id = c.prev_show_id
   AND tivo_prev_program.vendor_name = 'TIVO'

  LEFT OUTER JOIN inscape_map_deduped AS tms_prev_station
    ON tms_prev_station.mapped_vendor_station_id = c.tms_prev_station_id
   AND tms_prev_station.mapped_vendor = 'TMS'
  LEFT OUTER JOIN prod.detection.epg_show AS tms_prev_program
    ON tms_prev_program.show_id = c.tms_prev_show_id
   AND tms_prev_program.vendor_name = 'TMS'

  LEFT OUTER JOIN inscape_map_deduped AS tivo_next_station 
    ON tivo_next_station.mapped_vendor_station_id = c.next_station_id
   AND tivo_next_station.mapped_vendor = 'TIVO'
  LEFT OUTER JOIN prod.detection.epg_show AS tivo_next_program
    ON tivo_next_program.show_id = c.next_show_id
   AND tivo_next_program.vendor_name = 'TIVO'

  LEFT OUTER JOIN inscape_map_deduped AS tms_next_station
    ON tms_next_station.mapped_vendor_station_id = c.tms_next_station_id
   AND tms_next_station.mapped_vendor = 'TMS'
  LEFT OUTER JOIN prod.detection.epg_show AS tms_next_program
    ON tms_next_program.show_id = c.tms_next_show_id
   AND tms_next_program.vendor_name = 'TMS'
  ------------------------------------------------------------
  -- File Content Joins
  LEFT OUTER JOIN prod.detection.content_id_external_firehose AS prev_filecontent
    ON c.prev_show_id = prev_filecontent.fk_content_id
  LEFT OUTER JOIN prod.detection.content_id_external_firehose AS next_filecontent
    ON c.next_show_id = next_filecontent.fk_content_id
  LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m_filter
    ON m_filter.fk_content_id = prev_content.fk_content_id
  LEFT OUTER JOIN content_ids_firehose as prev_cid
    ON c.prev_show_id = prev_cid.content_id
  LEFT OUTER JOIN content_ids_firehose as next_cid
    ON c.next_show_id = next_cid.content_id
  LEFT OUTER JOIN prod.detection.clients cl2
    ON m_filter.fk_client_id = cl2.client_id
  ------------------------------------------------------------
  -- Blacklist Joins
  LEFT OUTER JOIN activity_obfuscation appb
    ON tis.app_name = appb.app_name
  LEFT OUTER JOIN viewing_obfuscation AS acrb
    ON tis.app_name = acrb.app_name

  LEFT OUTER JOIN prod.detection.free_channels_distribution_blacklist prev_chanb
    ON prev_vizio_station.name = prev_chanb.channel_name
  LEFT OUTER JOIN prod.detection.free_channels_distribution_blacklist next_chanb
    ON next_vizio_station.name = next_chanb.channel_name

  LEFT OUTER JOIN nielsen_replacement_local_alias AS tivo_rep_local_prev
    ON tivo_rep_local_prev.station_id = tivo_prev_station.inscape_station_id
   AND tivo_rep_local_prev.fk_show_id = c.prev_show_id
   AND tivo_rep_local_prev.dma_id = c.fk_dma_id
  LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS tivo_rep_nyc_nat_prev
    ON tivo_rep_nyc_nat_prev.station_id = tivo_prev_station.inscape_station_id
   AND tivo_rep_nyc_nat_prev.fk_show_id = c.prev_show_id
  LEFT OUTER JOIN nielsen_replacement_local_alias AS tms_rep_local_prev
    ON tms_rep_local_prev.station_id = tms_next_station.inscape_station_id
   AND tms_rep_local_prev.fk_show_id = c.tms_prev_show_id
   AND tms_rep_local_prev.dma_id = c.fk_dma_id
  LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS tms_rep_nyc_nat_prev
    ON tms_rep_nyc_nat_prev.station_id = tms_next_station.inscape_station_id
   AND tms_rep_nyc_nat_prev.fk_show_id = c.tms_prev_show_id

  LEFT OUTER JOIN nielsen_replacement_local_alias AS tivo_rep_local_next
    ON tivo_rep_local_next.station_id = tivo_next_station.inscape_station_id
   AND tivo_rep_local_next.fk_show_id = c.next_show_id
   AND tivo_rep_local_next.dma_id = c.fk_dma_id
  LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS tivo_rep_nyc_nat_next
    ON tivo_rep_nyc_nat_next.station_id = tivo_next_station.inscape_station_id
   AND tivo_rep_nyc_nat_next.fk_show_id = c.next_show_id
  LEFT OUTER JOIN nielsen_replacement_local_alias AS tms_rep_local_next
    ON tms_rep_local_next.station_id = tms_next_station.inscape_station_id
   AND tms_rep_local_next.fk_show_id = c.tms_next_show_id
   AND tms_rep_local_next.dma_id = c.fk_dma_id
  LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS tms_rep_nyc_nat_next
    ON tms_rep_nyc_nat_next.station_id = tms_next_station.inscape_station_id
   AND tms_rep_nyc_nat_next.fk_show_id = c.tms_next_show_id
  ------------------------------------------------------------
  WHERE c.session_start >= '{start_time}'::timestamp
    AND c.session_start < '{end_time}'::timestamp
    AND c.partition_key >= '{start_time}'::timestamp::DATE
    AND c.partition_key <= '{end_time}'::timestamp::DATE
    AND MOD(c.fk_tvid, 10) = 1
)
GROUP BY tvid, fk_tvid, zipcode, dma, external_id, mt_start, session_start, session_end
, tms_prev_episode_id, tivo_prev_episode_id, tms_next_episode_id, tivo_next_episode_id
, tms_prev_show_title, tivo_prev_show_title, tms_next_show_title, tivo_next_show_title
, tms_prev_channel_callsign, tivo_prev_channel_callsign, tms_next_channel_callsign, tivo_next_channel_callsign
, tms_prev_network_affiliate, tivo_prev_network_affiliate, tms_next_network_affiliate, tivo_next_network_affiliate
, prev_ts_start, prev_ts_end, next_ts_start, next_ts_end
, live, brand_name, title, duration
, ip, input_category, input_device, app_service
, audio_acr, dma_code, vizio_epg_channel_id, vizio_epg_program_id
, prev_vizio_epg_not_null, prev_nielsen_exclusive, next_nielsen_exclusive, prev_station_vod, next_station_vod
, commercial_client;
""")

# COMMAND ----------

x

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dev.mohit_gangwani.testing_golden_commercials_table
# MAGIC WHERE commercial_client = 'kinetiq'
# MAGIC ORDER BY tvid, session_start
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT app_service, appb_clients, COUNT(*)
# MAGIC FROM dev.mohit_gangwani.testing_golden_commercials_table
# MAGIC WHERE appb_clients != '' AND appb_clients IS NOT NULL AND NOT prev_vizio_epg_not_null
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1, 2
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC SELECT TRUE AS a, TRUE AS b
# MAGIC UNION
# MAGIC SELECT FALSE AS a, TRUE AS b
# MAGIC UNION
# MAGIC SELECT NULL AS a, TRUE AS b)
# MAGIC WHERE a OR b
