# Databricks notebook source
schema = 'dev.mohit_gangwani'
report_name = ''
start_time = '2025-05-05T00:00:00'
end_time = '2025-05-05T02:00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dev.mohit_gangwani.cure_viewing_content_golden_table_tuner;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dev.mohit_gangwani.cure_viewing_content_golden_table;

# COMMAND ----------

spark.sql(f"""DROP TABLE IF EXISTS {schema}.{report_name};""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {schema}.{report_name} AS
-- WITH activity_obfuscation AS (
--   SELECT blocked_apps.app_name, override.client_name
--   FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
--   LEFT JOIN prod.detection.app_customer_activity_distribution_override override
--     ON blocked_apps.app_name = override.app_name
--   GROUP BY 1, 2
-- ),
-- viewing_obfuscation AS (
--   SELECT blocked_apps.app_name, override.client_name
--   FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
--   LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
--     ON blocked_apps.app_name = override.app_name
--   GROUP BY 1, 2
-- )
WITH inscape_station_map_dedupe AS (
  SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
  FROM (
    SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor,   mapped_vendor_station_id ORDER BY created_at DESC) AS rn
    FROM detection.inscape_station_map) ism
  WHERE ism.rn = 1
)
SELECT tvid, fk_tvid
, tms_episode_id, tivo_episode_id
, tms_title, tivo_title
, tms_airdate, tivo_airdate
, tms_channel_callsign, tivo_channel_callsign
, mt_start, session_start, session_end
, tms_channel_affiliate, tivo_channel_affiliate
, is_live
, ip_address, input_category, input_device, app_service, tuner_channel_number
, content_only_condition
-- , '|'||array_join(collect_set(acrb_client), '|')||'|' AS acrb_clients
-- , '|'||array_join(collect_set(appb_client), '|')||'|' AS appb_clients
, '|'||array_join(collect_set(client_id), '|')||'|' AS client_id_not_null
FROM (
  SELECT DISTINCT COALESCE(tv.long_tvid, tv.vizio_tvid) AS tvid
  , c.fk_tvid
  ---------------- Episode ID ----------------
  , tms_show.database_key AS tms_episode_id
  , tivo_show.database_key AS tivo_episode_id
  ---------------- Show Title ----------------
  , REPLACE(tms_show.title, ',', '') AS tms_title
  , REPLACE(tivo_show.title, ',', '') AS tivo_title
  ------------------ Airdate ------------------
  , c.tms_airdate AS tms_airdate
  , c.airdate AS tivo_airdate
  ------------- Channel Call Sign -------------
  , tms_map.inscape_call_sign AS tms_channel_callsign
  , tivo_map.inscape_call_sign AS tivo_channel_callsign
  ------------- Media Time Start -------------
  , CASE WHEN COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL
             THEN LEAST((unix_timestamp(c.session_start)-unix_timestamp(COALESCE(c.airdate, c.tms_airdate))), c.runtime) 
         ELSE LEAST(c.media_time_start, c.runtime)
    END AS mt_start
  ------------------------------------------------------------
  , c.session_start
  , c.session_end
  ------------------ Station Affiliate -------------------
  , CASE WHEN tms_station.inscape_station_name IS NOT NULL THEN tms_station.inscape_station_name
         ELSE tms_station.station_affil
    END AS tms_channel_affiliate
  , CASE WHEN tivo_station.inscape_station_name IS NOT NULL THEN tivo_station.inscape_station_name
         ELSE tivo_station.station_affil
    END AS tivo_channel_affiliate
  ------------------ Live -------------------
  , CASE WHEN COALESCE(c.tuner_channel_id, NULLIF(c.tms_tuner_channel_id,98989898)) IS NOT NULL THEN 't'
         WHEN c.is_live = TRUE THEN 't'
         WHEN c.is_live = FALSE THEN 'f'
    END AS is_live
  ------------------------------------------------------------
  , ip.ip_address
  , CASE WHEN COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL AND UPPER(tvis.category) in ('APPS', 'OTHER', 'OTT') THEN 'HD TV'
         WHEN UPPER(tvis.category) = 'OTHER' AND tis.app_name = 'WatchFree+' AND cid.content_cid != 'unknown' THEN 'HD TV' 
         WHEN UPPER(tvis.category) = 'OTT' AND tis.app_name = 'WatchFree+' THEN 'APPS'
         ELSE tvis.category
    END AS input_category
  , CASE WHEN COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN 'OTA'
         WHEN inps.input_source = 'DTV' THEN 'OTA'
         ELSE tvis.input_device
    END AS input_device
  , CASE WHEN COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL
         AND NVL(tvis.input_device, 'OTA') = 'OTA' THEN 'WatchFree+'
        WHEN UPPER(tvis.category) = 'APPS' THEN
            CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
                 WHEN c.vizio_epg_station IS NULL AND tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
                 WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
                 WHEN lower(tis.app_name) = 'unknown' THEN NULL
                 ELSE tis.app_name
            END
        WHEN c.is_live = true
         AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
         AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NULL THEN 'vMVPD'
        WHEN inps.input_source IN ('DTV', 'TUNER', 'COAXIAL', 'ATV')
         AND NVL(tvis.input_device, 'OTA') = 'OTA'
         AND tis.app_name ='WatchFree+'
         AND c.is_live = TRUE THEN 'WatchFree+'
    END AS app_service
  , c.tuner_channel_number
  ------------------ Conditions -------------------
  ------Bools-------
  , CASE WHEN COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NULL THEN TRUE ELSE FALSE
    END AS content_only_condition
  ------Later Aggs-------
  -- , CASE WHEN acrb.app_name IS NOT NULL AND c.vizio_epg_station IS NULL THEN CASE WHEN acrb.client_name IS NULL THEN 'ALL' ELSE acrb.client_name END
  --   END AS acrb_client
  -- , CASE WHEN appb.app_name IS NOT NULL AND c.vizio_epg_station IS NULL THEN CASE WHEN appb.client_name IS NULL THEN 'ALL' ELSE appb.client_name END
  --   END AS appb_client
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
  -- LEFT OUTER JOIN activity_obfuscation AS appb
  --   ON tis.app_name = appb.app_name
  -- LEFT OUTER JOIN viewing_obfuscation AS acrb
  --   ON tis.app_name = acrb.app_name
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
    ON tivo_map.mapped_vendor_station_id = c.tuner_channel_id
   AND tivo_map.mapped_vendor = 'TIVO'
  LEFT OUTER JOIN prod.detection.epg_station AS tivo_station
    ON tivo_station.station_id = c.tuner_channel_id
   AND tivo_station.vendor_name = 'TIVO'
  LEFT OUTER JOIN prod.detection.epg_show AS tivo_show
    ON tivo_show.show_id = c.tuner_program_id
   AND tivo_show.vendor_name = 'TIVO'
  
  LEFT OUTER JOIN prod.detection.epg_show AS tms_show
    ON tms_show.show_id = c.tms_tuner_program_id
   AND tms_show.vendor_name = 'TMS'
  LEFT OUTER JOIN prod.detection.epg_station AS tms_station
    ON tms_station.station_id = c.tms_tuner_channel_id
   AND tms_station.vendor_name = 'TMS'
  LEFT OUTER JOIN inscape_station_map_dedupe AS tms_map
    ON tms_map.mapped_vendor_station_id = c.tms_tuner_channel_id
   AND tms_map.mapped_vendor = 'TMS'
  ---------------------------------------------
  WHERE c.session_start >= '{start_time}'::timestamp
    AND c.session_start < '{end_time}'::timestamp
    AND (
        COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL
        OR NVL(tvis.input_device, 'OTA') = 'OTA'
        OR inps.input_source = 'DTV'
    )
    AND CASE c.file_ingested
      WHEN true THEN
          CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT_PART(cid.content_cid, '_', 3), '') IS NULL
          WHEN true THEN SPLIT(cid.content_cid, '_')[1]
          ELSE NULL
          END
      ELSE COALESCE(tivo_map.inscape_call_sign, tms_map.inscape_call_sign, 'KeepSessionForNullReport')
      END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
)
GROUP BY tvid, fk_tvid, tms_episode_id, tivo_episode_id, tms_title, tivo_title, tms_airdate, tivo_airdate, tms_channel_callsign, tivo_channel_callsign, mt_start, session_start, session_end, tms_channel_affiliate, tivo_channel_affiliate, is_live, ip_address, input_category, input_device, app_service, tuner_channel_number, content_only_condition;
""")

# COMMAND ----------

spark.sql(f"""SELECT COUNT(*) FROM {schema}.cure_viewing_content_golden_table;""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NVL(tivo_channel_callsign, tms_channel_callsign) IS NULL, input_device, COUNT(*)
# MAGIC FROM dev.mohit_gangwani.cure_viewing_content_golden_table_tuner
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NVL(tivo_channel_callsign, tms_channel_callsign) IS NULL, input_category, COUNT(*)
# MAGIC FROM dev.mohit_gangwani.cure_viewing_content_golden_table_tuner
# MAGIC -- WHERE input_device != 'OTA'
# MAGIC GROUP BY 1, 2
# MAGIC order by 3 desc
# MAGIC limit 1000

# COMMAND ----------

spark.sql(f"""SELECT COUNT(*) FROM {schema}.{report_name};""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT CASE WHEN tuner_channel_id IS NULL THEN 'Null' ELSE 'Not Null' END AS tuner_channel_id
# MAGIC -- , CASE WHEN tuner_program_id IS NULL THEN 'Null' ELSE 'Not Null' END AS tuner_program_id
# MAGIC -- , CASE WHEN tuner_schedule_id IS NULL THEN 'Null' ELSE 'Not Null' END AS tuner_schedule_id
# MAGIC -- , CASE WHEN tms_tuner_channel_id IS NULL THEN 'Null' ELSE 'Not Null' END AS tms_tuner_channel_id
# MAGIC -- , CASE WHEN tms_tuner_program_id IS NULL THEN 'Null' ELSE 'Not Null' END AS tms_tuner_program_id
# MAGIC -- , CASE WHEN tms_tuner_schedule_id IS NULL THEN 'Null' ELSE 'Not Null' END AS tms_tuner_schedule_id
# MAGIC -- SELECT CASE WHEN vizio_epg_airing IS NULL THEN 'Null' ELSE 'Not Null' END AS vizio_epg_airing
# MAGIC -- , CASE WHEN vizio_epg_program IS NULL THEN 'Null' ELSE 'Not Null' END AS vizio_epg_program
# MAGIC -- , CASE WHEN vizio_epg_station IS NULL THEN 'Null' ELSE 'Not Null' END AS vizio_epg_station
# MAGIC -- SELECT CASE WHEN file_ingested THEN 'Yes' ELSE 'No' END AS file_ingested
# MAGIC SELECT fk_content_id = 3468026
# MAGIC , COUNT(*)
# MAGIC FROM detection.viewing_content_firehose
# MAGIC WHERE session_start > CURRENT_DATE
# MAGIC   AND COALESCE(tuner_channel_id, tms_tuner_channel_id) IS NOT NULL
# MAGIC GROUP BY 1--, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH activity_obfuscation AS (
# MAGIC   SELECT blocked_apps.app_name, override.client_name
# MAGIC   FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC   LEFT JOIN prod.detection.app_customer_activity_distribution_override override
# MAGIC     ON blocked_apps.app_name = override.app_name
# MAGIC   GROUP BY 1, 2
# MAGIC ),
# MAGIC viewing_obfuscation AS (
# MAGIC   SELECT blocked_apps.app_name, override.client_name
# MAGIC   FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC   LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC     ON blocked_apps.app_name = override.app_name
# MAGIC   GROUP BY 1, 2
# MAGIC )
# MAGIC SELECT tvis.category AS input_category
# MAGIC , tvis.input_device
# MAGIC , CASE WHEN UPPER(tvis.category) = 'APPS' THEN
# MAGIC         CASE WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora',  'tv games') AND cid.content_cid <> 'unknown' THEN NULL
# MAGIC               WHEN lower(tis.app_name) = 'unknown' THEN NULL
# MAGIC               ELSE tis.app_name
# MAGIC         END
# MAGIC         WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4',  'playstation 5','roku') THEN 'vMVPD'
# MAGIC   END AS app_service
# MAGIC , CASE WHEN acrb.app_name IS NOT NULL THEN CASE WHEN acrb.client_name IS NULL THEN 'ALL' ELSE acrb.client_name END
# MAGIC   END AS acrb_client
# MAGIC , CASE WHEN appb.app_name IS NOT NULL THEN CASE WHEN appb.client_name IS NULL THEN 'ALL' ELSE appb.client_name END
# MAGIC   END AS appb_client
# MAGIC , COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose AS c
# MAGIC JOIN prod.detection.content_ids_firehose AS cid
# MAGIC   ON cid.content_id = c.fk_content_id
# MAGIC LEFT OUTER JOIN prod.detection.input_source inps
# MAGIC   ON c.fk_input_source_id = inps.input_source_id
# MAGIC JOIN prod.detection.tv_input_stats_firehose  tvis
# MAGIC   ON c.session_start >= tvis.create_timestamp
# MAGIC  AND c.session_start < tvis.next_create_timestamp
# MAGIC  AND tvis.next_create_timestamp >= CURRENT_DATE - 2
# MAGIC  AND c.fk_tvid = tvis.fk_tvid
# MAGIC  AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC LEFT OUTER JOIN prod.detection.tv_inputsource tis
# MAGIC   ON c.session_start >= (tis.create_timestamp::double)::timestamp
# MAGIC  AND c.session_start < (tis.next_create_timestamp::double)::timestamp
# MAGIC  AND c.fk_tvid = tis.fk_tvid
# MAGIC  AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC  AND tis.next_create_timestamp >= CURRENT_DATE - 2
# MAGIC -- App blacklist
# MAGIC LEFT OUTER JOIN activity_obfuscation AS appb
# MAGIC   ON tis.app_name = appb.app_name
# MAGIC LEFT OUTER JOIN viewing_obfuscation AS acrb
# MAGIC   ON tis.app_name = acrb.app_name
# MAGIC WHERE session_start > CURRENT_DATE - 2
# MAGIC   AND COALESCE(tuner_channel_id, tms_tuner_channel_id) IS NOT NULL
# MAGIC GROUP BY 1, 2, 3, 4, 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cid.content_cid = 'unknown', COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose AS c
# MAGIC JOIN prod.detection.content_ids_firehose AS cid
# MAGIC   ON cid.content_id = c.fk_content_id
# MAGIC WHERE session_start > CURRENT_DATE - 2
# MAGIC   AND COALESCE(tuner_channel_id, tms_tuner_channel_id) IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.detection.content_ids_firehose
# MAGIC WHERE content_cid = 'unknown'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ingested, attributed, COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose AS c
# MAGIC JOIN prod.detection.epg_station st
# MAGIC   ON st.station_id = c.tuner_channel_id
# MAGIC  AND st.vendor_name = 'TIVO'
# MAGIC WHERE session_start > CURRENT_DATE - 2
# MAGIC   AND COALESCE(tuner_channel_id, tms_tuner_channel_id) IS NOT NULL
# MAGIC GROUP BY 1, 2
