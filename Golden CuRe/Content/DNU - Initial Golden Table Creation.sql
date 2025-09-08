-- Databricks notebook source
DROP TABLE IF EXISTS dev.mohit_gangwani.cure_activity_obfuscation;
CREATE TABLE dev.mohit_gangwani.cure_activity_obfuscation AS
SELECT blocked_apps.app_name, override.client_name
FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
LEFT JOIN prod.detection.app_customer_activity_distribution_override override
  ON blocked_apps.app_name = override.app_name
WHERE override.app_name IS NULL
GROUP BY 1, 2;

DROP TABLE IF EXISTS dev.mohit_gangwani.cure_viewing_obfuscation;
CREATE TABLE dev.mohit_gangwani.cure_viewing_obfuscation AS
SELECT blocked_apps.app_name, override.client_name
FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
  ON blocked_apps.app_name = override.app_name
WHERE override.app_name IS NULL
GROUP BY 1, 2;

DROP TABLE IF EXISTS dev.mohit_gangwani.cure_station_distribution_blacklist;
CREATE TABLE dev.mohit_gangwani.cure_station_distribution_blacklist AS
SELECT station_id, vendor_name, client_name
FROM prod.detection.station_distribution_obfuscation_overwrite
GROUP BY 1, 2, 3;

DROP TABLE IF EXISTS dev.mohit_gangwani.cure_inscape_station_map_dedupe;
CREATE TABLE dev.mohit_gangwani.cure_inscape_station_map_dedupe AS
SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
FROM (
  SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
  FROM detection.inscape_station_map) ism
WHERE ism.rn = 1;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.cure_viewing_content_final_version;
CREATE TABLE dev.mohit_gangwani.cure_viewing_content_final_version AS
SELECT tvid, zipcode, dma, tms_episode_id, tivo_episode_id, tms_title, tivo_title, tms_air_date, tivo_air_date
, tms_channel_callsign, tivo_channel_callsign, mt_start, session_start, session_end, tms_channel_affiliate, tivo_channel_affiliate
, is_live, ip_address, input_category, input_device, app_service, nielsen_check, vizio_station_check, chanb_check, cid_content_null_check, station_obfs_check, file_ingest_dbkey
, '|'||array_join(collect_set(acrb_client), '|')||'|' AS acrb_clients
, '|'||array_join(collect_set(appb_client), '|')||'|' AS appb_clients
, '|'||array_join(collect_set(station_blacklist_client), '|')||'|' AS station_blacklist_clients
, '|'||array_join(collect_set(client_id), '|')||'|' AS client_id_not_null
FROM (
SELECT DISTINCT COALESCE(tv.long_tvid, tv.vizio_tvid) AS tvid
, NULLIF(location.zipcode, '') AS zipcode
, REPLACE(dma.dma_name, ',', '') AS dma

, tms_show.database_key AS tms_episode_id
, tivo_show.database_key AS tivo_episode_id --5

, CASE WHEN c.vizio_epg_station IS NOT NULL THEN
       CASE WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != '' THEN vizio_program.series_aggregate_title
            ELSE vizio_program.title END
     WHEN c.file_ingested THEN NULL
     ELSE REPLACE(tms_show.title, ',', '')
  END AS tms_title
, CASE WHEN c.vizio_epg_station IS NOT NULL THEN
       CASE WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != '' THEN vizio_program.series_aggregate_title
            ELSE vizio_program.title END
     WHEN c.file_ingested THEN NULL
     ELSE REPLACE(tivo_show.title, ',', '')
  END AS tivo_title

, c.tms_airdate AS tms_air_date
, c.airdate AS tivo_air_date

, CASE WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
       ELSE tms_map.inscape_call_sign
  END AS tms_channel_callsign
, CASE WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
       ELSE tivo_map.inscape_call_sign
  END AS tivo_channel_callsign --11

, CASE WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
       ELSE LEAST(c.media_time_start, c.runtime)
  END AS mt_start

, c.session_start
, c.session_end

, CASE WHEN c.vizio_epg_station IS NOT NULL THEN vizio_station.name
       WHEN c.tms_station_id IS NOT NULL THEN
            CASE WHEN tms_station.inscape_station_name IS NOT NULL THEN tms_station.inscape_station_name
                 WHEN LOWER(tms_station.station_affil) LIKE '%affiliate%'
                   OR LOWER(tms_station.station_affil) LIKE '%independent%'
                   OR LOWER(tms_station.station_affil) LIKE '%low power%'
                      THEN tms_station.station_affil
            END
  END AS tms_channel_affiliate

, CASE WHEN c.vizio_epg_station IS NOT NULL THEN vizio_station.name
       WHEN c.fk_station_id IS NOT NULL THEN
            CASE WHEN tivo_station.inscape_station_name IS NOT NULL THEN tivo_station.inscape_station_name
                 WHEN LOWER(tivo_station.station_affil) LIKE '%affiliate%'
                   OR LOWER(tivo_station.station_affil) LIKE '%independent%'
                   OR LOWER(tivo_station.station_affil) LIKE '%low power%'
                      THEN tivo_station.station_affil
            END
  END AS tivo_channel_affiliate

, CASE WHEN c.vizio_epg_station IS NOT NULL THEN 't'
    WHEN c.is_live = TRUE THEN 't'
    WHEN c.is_live = FALSE THEN 'f'
  END AS is_live --17

, ip.ip_address
, tvis.category AS input_category
, tvis.input_device
, CASE WHEN UPPER(tvis.category) = 'APPS' THEN
        CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
             WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
            --  WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
             WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
             WHEN lower(tis.app_name) = 'unknown' THEN NULL
             ELSE tis.app_name
        END
       WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
  END AS app_service -- 21

, CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN TRUE ELSE FALSE END AS nielsen_check
, CASE WHEN c.vizio_epg_station IS NOT NULL THEN TRUE ELSE FALSE END AS vizio_station_check
, CASE WHEN c.vizio_epg_station IS NOT NULL AND (chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1)) THEN TRUE ELSE FALSE END AS chanb_check
, CASE WHEN cid.content_cid = 'unknown' THEN CASE WHEN COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN 2 ELSE 1 END ELSE 0 END AS cid_content_null_check
, CASE WHEN station_obfs.station_id IS NOT NULL THEN TRUE ELSE FALSE END AS station_obfs_check --25
, CASE WHEN c.file_ingested = true THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0]) END AS file_ingest_dbkey
, CASE WHEN acrb.app_name IS NOT NULL THEN acrb.client_name END AS acrb_client
, CASE WHEN appb.app_name IS NOT NULL THEN appb.client_name END AS appb_client
, CASE WHEN station_blacklist.station_id IS NOT NULL THEN station_blacklist.client_name END AS station_blacklist_client
, cl.client_name AS client_id
-- Joins that do not need to be modified
FROM prod.detection.viewing_content_firehose AS c
JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
    AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
JOIN prod.detection.tv AS tv ON c.session_start >= '2024-12-14 00:00:00'::timestamp
    AND c.session_start < '2024-12-14 02:00:00'::timestamp
    AND c.fk_tvid = tv.tvid
    AND tv.oem = 'VIZIO'
-- Location
JOIN prod.detection.tv_settings AS tv_settings
    ON c.session_start >= tv_settings.create_timestamp
    AND c.session_start < tv_settings.next_create_timestamp
    AND tv_settings.create_timestamp <= '2024-12-14 02:00:00'::timestamp
    AND tv_settings.next_create_timestamp >= '2024-12-14 00:00:00'::timestamp
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
LEFT OUTER JOIN
    prod.detection.tv_ip_address AS ip
    ON c.session_start >= ip.create_timestamp
    AND c.session_start < ip.next_create_timestamp
    AND ip.create_timestamp <= '2024-12-14 02:00:00'::timestamp
    AND ip.next_create_timestamp >= '2024-12-14 00:00:00'::timestamp
    AND tv.tvid = ip.fk_tvid
-- Vizio Joins
LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station
    ON CAST(c.vizio_epg_station AS string) = CAST(vizio_station.station_id AS string)
LEFT OUTER JOIN
    prod.detection.free_channels_distribution_blacklist chanb
    ON vizio_station.name = chanb.channel_name
LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
    ON CAST(c.vizio_epg_program AS string) <=> CAST(vizio_program.program_aggregate_id AS string)
    AND c.vizio_epg_program != '0'
-- Input Joins
LEFT OUTER JOIN prod.detection.input_source inps
    ON c.fk_input_source_id = inps.input_source_id
JOIN prod.detection.tv_input_stats_firehose  tvis
    ON c.session_start >= tvis.create_timestamp
    AND c.session_start < tvis.next_create_timestamp
    AND tvis.create_timestamp <= '2024-12-14 02:00:00'::timestamp
    AND tvis.next_create_timestamp >= '2024-12-14 00:00:00'::timestamp
    AND  c.fk_tvid = tvis.fk_tvid
    AND  c.fk_input_source_id = tvis.fk_input_source_id
LEFT OUTER JOIN prod.detection.tv_inputsource tis
    ON  c.session_start >= (tis.create_timestamp::double)::timestamp
    AND c.session_start < (tis.next_create_timestamp::double)::timestamp
    AND c.fk_tvid = tis.fk_tvid
    AND c.fk_input_source_id = tis.fk_input_source_id
    AND tis.create_timestamp <= ('2024-12-14 02:00:00'::timestamp::double)::timestamp
    AND tis.next_create_timestamp >= ('2024-12-14 00:00:00'::timestamp::double)::timestamp
-- App blacklist
LEFT OUTER JOIN dev.mohit_gangwani.cure_activity_obfuscation AS appb
    ON tis.app_name = appb.app_name
LEFT OUTER JOIN dev.mohit_gangwani.cure_viewing_obfuscation AS acrb
    ON tis.app_name = acrb.app_name
---------------------------------------------
-- Need to modify
LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
    ON m.fk_content_id = c.fk_content_id
LEFT OUTER JOIN prod.detection.clients cl
    ON m.fk_client_id = cl.client_id
LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
    ON md.fk_content_id = c.fk_content_id
LEFT OUTER JOIN prod.detection.clients cli
    ON md.fk_client_id = cli.client_id
---------------------------------------------
-- TiVo TMS specific joins
LEFT OUTER JOIN dev.mohit_gangwani.cure_inscape_station_map_dedupe AS tivo_map
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
LEFT OUTER JOIN dev.mohit_gangwani.cure_inscape_station_map_dedupe AS tms_map
    ON tms_map.mapped_vendor_station_id = c.tms_station_id
    AND tms_map.mapped_vendor = 'TMS'
---------------------------------------------
-- Blacklist Joins
LEFT OUTER JOIN dev.mohit_gangwani.cure_station_distribution_blacklist AS station_blacklist
    ON station_blacklist.station_id = COALESCE(tivo_map.inscape_station_id, tms_map.inscape_station_id)
    -- AND station_blacklist.vendor_name = 'TIVO'
LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
    ON station_obfs.vendor_station_id = COALESCE(tivo_map.inscape_station_id, tms_map.inscape_station_id)
    -- AND station_obfs.vendor_name = 'TIVO'
LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
    ON nielsen_blacklist.station_id = COALESCE(tivo_map.inscape_station_id, tms_map.inscape_station_id)
    AND c.session_start >= nielsen_blacklist.blacklist_start
    AND c.session_start < nielsen_blacklist.blacklist_end
LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
    ON rep_local.station_id = COALESCE(tivo_map.inscape_station_id, tms_map.inscape_station_id)
    AND rep_local.airdate = COALESCE(c.airdate, c.tms_airdate)
    AND rep_local.fk_show_id = COALESCE(c.fk_show_id, c.tms_show_id)
    AND rep_local.dma_id = c.fk_dma_id
LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
    ON rep_nyc_nat.station_id = COALESCE(tivo_map.inscape_station_id, tms_map.inscape_station_id)
    AND rep_nyc_nat.airdate = COALESCE(c.airdate, c.tms_airdate)
    AND rep_nyc_nat.fk_show_id = COALESCE(c.fk_show_id, c.tms_show_id)
---------------------------------------------
WHERE c.session_start >= '2024-12-14 00:00:00'::timestamp
AND c.session_start < '2024-12-14 02:00:00'::timestamp
    AND CASE c.file_ingested
        WHEN true THEN
            CASE WHEN NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NULL THEN SPLIT(cid.content_cid, '_')[1]
            ELSE NULL
            END
        ELSE COALESCE(tms_station.station_call_sign, tivo_station.station_call_sign, 'KeepSessionForNullReport')
        END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
)
GROUP BY tvid, zipcode, dma, tms_episode_id, tivo_episode_id, tms_title, tivo_title, tms_air_date, tivo_air_date, tms_channel_callsign, tivo_channel_callsign, mt_start, session_start, session_end, tms_channel_affiliate, tivo_channel_affiliate, is_live, ip_address, input_category, input_device, app_service, nielsen_check, vizio_station_check, chanb_check, cid_content_null_check, station_obfs_check, file_ingest_dbkey

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.post_goldencure_viewing_content_first_iteration;
CREATE TABLE dev.mohit_gangwani.post_goldencure_viewing_content_first_iteration AS
SELECT mt.tvid
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_station_check THEN CASE WHEN mt.chanb_check OR '{vendor_name}' != 'TMS' THEN NULL END
       ELSE CASE WHEN mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
                 WHEN mt.client_id_not_null LIKE '%|{client_name}|%' THEN NULL
                 ELSE CASE WHEN mt.nielsen_check THEN NULL
                           WHEN mt.station_blacklist_clients NOT LIKE '%|{client_name}|%' THEN NULL 
                           WHEN file_ingest_dbkey
FROM dev.mohit_gangwani.cure_viewing_content_first_iteration AS mt

-- COMMAND ----------

SELECT * FROM dev.mohit_gangwani.cure_viewing_content_first_iteration
WHERE client_id_not_null != '||'
ORDER BY tvid, session_start, session_end
LIMIT 1000

-- COMMAND ----------

SELECT c.*
FROM (
SELECT tvid, session_start, session_end, COUNT(DISTINCT client_id) AS cl_count
FROM dev.mohit_gangwani.cure_viewing_content_first_iteration
GROUP BY 1, 2, 3) a
JOIN dev.mohit_gangwani.cure_viewing_content_first_iteration c
  ON c.tvid = a.tvid
 AND c.session_start = a.session_start
 AND c.session_end = a.session_end
WHERE cl_count > 1

-- COMMAND ----------


