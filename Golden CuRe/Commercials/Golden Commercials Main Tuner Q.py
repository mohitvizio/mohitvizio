# Databricks notebook source
start_time = '2025-04-10 00:00:00'
end_time = '2025-04-10 02:00:00'
schema_table_name = 'dev.mohit_gangwani.testing_golden_commercials_tuner_table'

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS {schema_table_name};
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dev.mohit_gangwani.testing_golden_commercials_tuner_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dev.mohit_gangwani.testing_golden_commercials_table;

# COMMAND ----------



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
, inscape_map_deduped AS (
    SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
    FROM (
        SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
        FROM prod.detection.inscape_station_map) ism
    WHERE ism.rn = 1
)
SELECT tvid, fk_tvid, external_id, mt_start, session_start, session_end
, NULLIF(tms_prev_episode_id, '') AS tms_prev_episode_id
, NULLIF(tivo_prev_episode_id, '') AS tivo_prev_episode_id
, NULLIF(tms_next_episode_id, '') AS tms_next_episode_id
, NULLIF(tivo_next_episode_id, '') AS tivo_next_episode_id
, NULLIF(tms_prev_show_title, '') AS tms_prev_show_title
, NULLIF(tivo_prev_show_title, '') AS tivo_prev_show_title
, NULLIF(tms_next_show_title, '') AS tms_next_show_title
, NULLIF(tivo_next_show_title, '') AS tivo_next_show_title
, NULLIF(tms_prev_channel_callsign, '') AS tms_prev_channel_callsign
, NULLIF(tivo_prev_channel_callsign, '') AS tivo_prev_channel_callsign
, NULLIF(tms_next_channel_callsign, '') AS tms_next_channel_callsign
, NULLIF(tivo_next_channel_callsign, '') AS tivo_next_channel_callsign
, NULLIF(tms_prev_network_affiliate, '') AS tms_prev_network_affiliate
, NULLIF(tivo_prev_network_affiliate, '') AS tivo_prev_network_affiliate
, NULLIF(tms_next_network_affiliate, '') AS tms_next_network_affiliate
, NULLIF(tivo_next_network_affiliate, '') AS tivo_next_network_affiliate
, live, ip, input_category, input_device, app_service, tuner_channel_number
, commercial_client
, '|'||array_join(collect_set(acrb_client), '|')||'|' AS acrb_clients
, '|'||array_join(collect_set(appb_client), '|')||'|' AS appb_clients
, '|'||array_join(collect_set(excluded_client), '|')||'|' AS excluded_client_list
FROM (
  SELECT DISTINCT COALESCE(tv.long_tvid, tv.vizio_tvid) AS tvid
  , c.fk_tvid
  , m.external_id
  , c.media_time_start AS mt_start
  , c.session_start
  , c.session_end
  ------------------ Episode ID -------------------
  , tms_prev_program.database_key AS tms_prev_episode_id
  , tivo_prev_program.database_key AS tivo_prev_episode_id
  , tms_next_program.database_key AS tms_next_episode_id
  , tivo_next_program.database_key AS tivo_next_episode_id
  ------------------ Show Title -------------------
  , tms_prev_program.title AS tms_prev_show_title
  , tms_next_program.title AS tms_next_show_title
  , tivo_prev_program.title AS tivo_prev_show_title
  , tivo_next_program.title AS tivo_next_show_title
  ------------------ Channel Call Sign -------------------
  , tms_prev_map.inscape_call_sign AS tms_prev_channel_callsign
  , tms_next_map.inscape_call_sign AS tms_next_channel_callsign
  , tivo_prev_map.inscape_call_sign AS tivo_prev_channel_callsign
  , tivo_next_map.inscape_call_sign AS tivo_next_channel_callsign
  ------------------ Station Affiliate -----------------------
  , tms_prev_station.inscape_station_name AS tms_prev_network_affiliate
  , tms_next_station.inscape_station_name AS tms_next_network_affiliate
  , tivo_prev_station.inscape_station_name AS tivo_prev_network_affiliate
  , tivo_next_station.inscape_station_name AS tivo_next_network_affiliate
  --------------------------------------------------------------
  -- Liveness, App Service, IP Address, Input Category/Device --
  , CASE WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED' AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
         WHEN COALESCE(NULLIF(c.tms_prev_tuner_channel_id,98989898), c.prev_tuner_channel_id, NULLIF(c.tms_next_tuner_channel_id,98989898), c.prev_tuner_channel_id) IS NOT NULL THEN 't'
         ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
    END AS live
  , ip.ip_address AS ip
  , CASE WHEN COALESCE(c.prev_tuner_channel_id, c.tms_prev_tuner_channel_id) IS NOT NULL AND UPPER(tvis.category) in ('APPS', 'OTHER', 'OTT') THEN 'HD TV'
         WHEN UPPER(tvis.category) = 'OTT' AND tis.app_name ='WatchFree+' THEN 'APPS'
         ELSE tvis.category
    END AS input_category
  , CASE WHEN COALESCE(c.prev_tuner_channel_id, c.tms_prev_tuner_channel_id) IS NOT NULL THEN 'OTA'
         WHEN inps.input_source = 'DTV' THEN 'OTA'
         ELSE tvis.input_device
    END AS input_device
  , CASE WHEN COALESCE(c.prev_tuner_channel_id, c.tms_prev_tuner_channel_id) is not null AND (tvis.input_device IS NULL OR tvis.input_device = 'OTA') THEN 'WatchFree+'
         WHEN UPPER(tvis.category) = 'APPS' THEN
            CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
               WHEN c.prev_vizio_epg_station IS NULL AND tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
               WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND COALESCE(c.prev_show_id, c.tms_prev_show_id) IS NOT NULL THEN NULL
               WHEN LOWER(tis.app_name) = 'unknown' THEN NULL
               ELSE tis.app_name
            END
          WHEN prev_content.is_live = true
           AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
           AND COALESCE(c.prev_tuner_channel_id, c.next_tuner_channel_id, c.tms_prev_tuner_channel_id, c.tms_next_tuner_channel_id) is NULL THEN 'vMVPD'
          WHEN inps.input_source IN ('DTV', 'TUNER', 'COAXIAL', 'ATV') AND (tvis.input_device IS NULL OR tvis.input_device = 'OTA') 
           AND tis.app_name ='WatchFree+' AND prev_content.is_live = TRUE THEN 'WatchFree+'
          WHEN COALESCE(c.prev_tuner_channel_id, c.next_tuner_channel_id, c.tms_prev_tuner_channel_id, c.tms_next_tuner_channel_id) is not null THEN 'OBFUSCATED'
   END AS app_service
   , c.tuner_channel_number
  ------Later Aggs-------
  , CASE WHEN UPPER(tvis.category) = 'APPS'
              AND acrb.app_name IS NOT NULL THEN CASE WHEN acrb.client_name IS NULL THEN 'ALL'
                                                      ELSE acrb.client_name END
    END AS acrb_client
  , CASE WHEN UPPER(tvis.category) = 'APPS' THEN
          CASE WHEN tis.app_name = 'WatchFree+' THEN NULL
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
  JOIN prod.detection.settings AS settings
    ON tv_settings.fk_settings_id = settings.settings_id
   AND UPPER(settings.country_name) = 'USA'
  -- Location 
 JOIN prod.detection.location AS location
   ON c.fk_location_id = location.location_id
  AND UPPER(location.country_code) = 'US'
  -- Input Joins
  JOIN prod.detection.tv_input_stats_firehose tvis
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
  LEFT OUTER JOIN prod.detection.input_source inps
    ON c.fk_input_source_id = inps.input_source_id
  -- IP Address
  LEFT OUTER JOIN prod.detection.tv_ip_address AS ip
    ON c.session_start >= ip.create_timestamp
   AND c.session_start < ip.next_create_timestamp
   AND ip.create_timestamp <= '{end_time}'::timestamp
   AND ip.next_create_timestamp >= '{start_time}'::timestamp
   AND c.fk_tvid = ip.fk_tvid
  ------------------------------------------------------------
  -- TiVo TMS specific joins
  LEFT OUTER JOIN prod.detection.epg_station AS tivo_prev_station 
    ON tivo_prev_station.station_id = c.prev_tuner_channel_id
   AND tivo_prev_station.vendor_name = 'TIVO'
  LEFT OUTER JOIN inscape_map_deduped AS tivo_prev_map
    ON tivo_prev_map.mapped_vendor_station_id = c.prev_tuner_channel_id
   AND tivo_prev_map.mapped_vendor = 'TIVO'
  LEFT OUTER JOIN prod.detection.epg_show AS tivo_prev_program
    ON tivo_prev_program.show_id = c.prev_tuner_program_id
   AND tivo_prev_program.vendor_name = 'TIVO'

  LEFT OUTER JOIN prod.detection.epg_station AS tms_prev_station
    ON tms_prev_station.station_id = c.tms_prev_tuner_channel_id
   AND tms_prev_station.vendor_name = 'TMS'
  LEFT OUTER JOIN inscape_map_deduped AS tms_prev_map
    ON tms_prev_map.mapped_vendor_station_id = c.tms_prev_tuner_channel_id
   AND tms_prev_map.mapped_vendor = 'TMS'
  LEFT OUTER JOIN prod.detection.epg_show AS tms_prev_program
    ON tms_prev_program.show_id = c.tms_prev_tuner_program_id
   AND tms_prev_program.vendor_name = 'TMS'

  LEFT OUTER JOIN prod.detection.epg_station AS tivo_next_station 
    ON tivo_next_station.station_id = c.next_tuner_channel_id
   AND tivo_next_station.vendor_name = 'TIVO'
  LEFT OUTER JOIN inscape_map_deduped AS tivo_next_map
    ON tivo_next_map.mapped_vendor_station_id = c.next_tuner_channel_id
   AND tivo_next_map.mapped_vendor = 'TIVO'
  LEFT OUTER JOIN prod.detection.epg_show AS tivo_next_program
    ON tivo_next_program.show_id = c.next_tuner_program_id
   AND tivo_next_program.vendor_name = 'TIVO'

  LEFT OUTER JOIN prod.detection.epg_station AS tms_next_station
    ON tms_next_station.station_id = c.tms_next_tuner_channel_id
   AND tms_next_station.vendor_name = 'TMS'
  LEFT OUTER JOIN inscape_map_deduped AS tms_next_map
    ON tms_next_map.mapped_vendor_station_id = c.tms_next_tuner_channel_id
   AND tms_next_map.mapped_vendor = 'TMS'
  LEFT OUTER JOIN prod.detection.epg_show AS tms_next_program
    ON tms_next_program.show_id = c.tms_next_tuner_program_id
   AND tms_next_program.vendor_name = 'TMS'
  ------------------------------------------------------------
  -- File Content Joins
  LEFT OUTER JOIN viewing_content_firehose AS prev_content
    ON c.fk_tvid = prev_content.fk_tvid
   AND prev_content.session_start = c.prev_session_start
  LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m_filter
    ON m_filter.fk_content_id = prev_content.fk_content_id
  LEFT OUTER JOIN prod.detection.clients cl2
    ON m_filter.fk_client_id = cl2.client_id
  ------------------------------------------------------------
  -- Blacklist Joins
  LEFT OUTER JOIN activity_obfuscation appb
    ON tis.app_name = appb.app_name
  LEFT OUTER JOIN viewing_obfuscation AS acrb
    ON tis.app_name = acrb.app_name
  ------------------------------------------------------------
  WHERE c.session_start >= '{start_time}'::timestamp
    AND c.session_start < '{end_time}'::timestamp
    AND c.partition_key >= '{start_time}'::timestamp::DATE
    AND c.partition_key <= '{end_time}'::timestamp::DATE
    AND (
        COALESCE(c.prev_tuner_channel_id, c.next_tuner_channel_id, c.tms_prev_tuner_channel_id, c.tms_next_tuner_channel_id) IS NOT NULL
        OR NVL(tvis.input_device, 'OTA') = 'OTA'
        OR inps.input_source = 'DTV'
    )
    AND MOD(c.fk_tvid, 10) = 1
)
GROUP BY tvid, fk_tvid, external_id, mt_start, session_start, session_end
, tms_prev_episode_id, tivo_prev_episode_id, tms_next_episode_id, tivo_next_episode_id
, tms_prev_show_title, tivo_prev_show_title, tms_next_show_title, tivo_next_show_title
, tms_prev_channel_callsign, tivo_prev_channel_callsign, tms_next_channel_callsign, tivo_next_channel_callsign
, tms_prev_network_affiliate, tivo_prev_network_affiliate, tms_next_network_affiliate, tivo_next_network_affiliate
, live
, ip, input_category, input_device, app_service, tuner_channel_number
, commercial_client;
""")

# COMMAND ----------

x

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dev.mohit_gangwani.testing_golden_commercials_tuner_table
# MAGIC WHERE tivo_prev_channel_callsign IS NOT NULL
# MAGIC   AND commercial_client = 'kinetiq'
# MAGIC ORDER BY tvid, session_start

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT input_category, input_device, app_service
# MAGIC , commercial_client
# MAGIC , acrb_clients
# MAGIC , appb_clients
# MAGIC , excluded_client_list
# MAGIC , COUNT(*)
# MAGIC FROM dev.mohit_gangwani.testing_golden_commercials_tuner_table
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6, 7

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH inscape_station_map_dedupe AS (
# MAGIC     SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
# MAGIC     FROM (
# MAGIC         SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
# MAGIC         FROM detection.inscape_station_map) ism
# MAGIC     WHERE ism.rn = 1
# MAGIC )
# MAGIC SELECT CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'vizio_epg_station not Null' END AS vizio_epg_test
# MAGIC , CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in ('98989898989898', '9898989898', '-1') THEN 'Chanb not Null' END chanb_test
# MAGIC , CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR nielsen_blacklist.ingest_time IS NOT NULL) THEN 'Nielsen Not Null' END AS nielsen_blacklist_test
# MAGIC , COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose AS c
# MAGIC LEFT OUTER JOIN inscape_station_map_dedupe AS map
# MAGIC   ON map.mapped_vendor_station_id = c.tms_tuner_channel_id
# MAGIC   AND map.mapped_vendor = 'TMS'
# MAGIC LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
# MAGIC   ON c.vizio_epg_station = vizio_station.station_id
# MAGIC LEFT OUTER JOIN prod.detection.free_channels_distribution_blacklist chanb 
# MAGIC   ON vizio_station.name = chanb.channel_name
# MAGIC LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
# MAGIC   ON nielsen_blacklist.station_id = map.inscape_station_id
# MAGIC   AND c.session_start >= nielsen_blacklist.blacklist_start 
# MAGIC   AND c.session_start < nielsen_blacklist.blacklist_end
# MAGIC LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
# MAGIC   ON map.inscape_station_id = rep_local.station_id 
# MAGIC   AND c.tms_airdate = rep_local.airdate
# MAGIC   AND c.tms_tuner_program_id = rep_local.fk_show_id
# MAGIC   AND c.fk_dma_id = rep_local.dma_id
# MAGIC LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
# MAGIC   ON map.inscape_station_id = rep_nyc_nat.station_id 
# MAGIC   AND c.tms_airdate = rep_nyc_nat.airdate
# MAGIC   AND c.tms_tuner_program_id = rep_nyc_nat.fk_show_id
# MAGIC WHERE c.session_start >= CURRENT_DATE - 7
# MAGIC   AND c.tms_tuner_channel_id IS NOT NULL
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN c.fk_station_id = 0 THEN 'tivo station id = 0' END AS tivo_station_id_zero_test
# MAGIC , CASE WHEN c.tms_station_id = 0 THEN 'tms station id = 0' END AS tms_station_id_zero_test
# MAGIC , COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose AS c
# MAGIC WHERE c.session_start >= CURRENT_DATE - 7
# MAGIC   AND c.tms_tuner_channel_id IS NOT NULL
# MAGIC GROUP BY 1, 2
