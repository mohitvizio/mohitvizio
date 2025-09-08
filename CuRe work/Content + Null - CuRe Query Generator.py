# Databricks notebook source
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("client_name", "")

start_time = dbutils.widgets.get("start_date")
end_time = dbutils.widgets.get("end_date")
client_name = dbutils.widgets.get("client_name")
table_name = 'content_with_null_'

table_name_existing = f'{table_name}{client_name}_existing_table'
table_name_new = f'{table_name}{client_name}_new_table'

print(f"Running Report from {start_time} to {end_time} for {client_name}")
print(f"loading in {table_name_existing} and {table_name_new}")

# COMMAND ----------

tivo_content_null_existing_q = f"""
DROP TABLE IF EXISTS dev.mohit_gangwani.{table_name_existing}_tivo;

CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.{table_name_existing}_tivo (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.{table_name_existing}_tivo (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
WITH activity_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_activity_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{client_name}'
    WHERE override.app_name IS NULL
)
, viewing_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{client_name}'
    WHERE override.app_name IS NULL
)
, station_distribution_blacklist AS (
    WITH agg AS (
        SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
        FROM prod.detection.station_distribution_obfuscation_overwrite
        GROUP BY 1, 2)
    SELECT station_id, vendor_name
    FROM agg
    WHERE cl_list NOT ILIKE '%{client_name}%'
)
SELECT /*+ BROADCAST(cid) */  DISTINCT
    COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
    '', 
    NULLIF(location.zipcode, ''), 
    REPLACE(dma.dma_name, ',', ''), 
    NULLIF(CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TIVO' != 'TMS' THEN NULL 
            ELSE vizio_program.program_tms_id
        END
    ELSE
        CASE WHEN acrb.app_name IS NOT NULL THEN NULL
            WHEN (cl.client_id is not null) THEN NULL
        ELSE
            CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
                WHEN station_blacklist.station_id IS NOT NULL THEN NULL
                WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
            ELSE show.database_key
        END
    END
    END,''), 
    CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
            WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
                THEN vizio_program.series_aggregate_title
            ELSE vizio_program.title END
    WHEN acrb.app_name IS NOT NULL THEN NULL
    WHEN (cl.client_id is not null) THEN NULL
    WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
    WHEN station_blacklist.station_id IS NOT NULL THEN NULL
    ELSE CASE WHEN c.file_ingested THEN NULL 
        WHEN '{client_name}' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
        ELSE REPLACE(show.title, ',', '') END 
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
        WHEN (cl.client_id is not null) THEN NULL
        WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
        WHEN '{client_name}' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
        ELSE schedule.airdate
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
        WHEN (cl.client_id is not null) THEN NUll
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_obfs.station_id IS NOT NULL THEN NULL
        WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
        WHEN '{client_name}' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
        WHEN '{client_name}' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
        ELSE map.inscape_call_sign 
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
        WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
        WHEN (cl.client_id is not null) THEN NUll
        WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
        WHEN '{client_name}' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
        WHEN '{client_name}' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
        ELSE LEAST(c.media_time_start, schedule.duration) 
    END, 
    c.session_start, 
    c.session_end, 
    CASE WHEN c.vizio_epg_station IS NOT NULL
    THEN CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED' 
        ELSE vizio_station.name END
    WHEN acrb.app_name IS NOT NULL THEN NULL
    WHEN (cl.client_id IS NOT NULL) THEN NULL
    ELSE
        CASE 
            WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
            WHEN station_obfs.station_id IS NOT NULL THEN NULL 
            WHEN '{client_name}' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
            WHEN '{client_name}' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
            WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
            WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
                OR LOWER(station.station_affil) LIKE '%independent%'
                OR LOWER(station.station_affil) LIKE '%low power%')
            THEN station.station_affil ELSE NULL END
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN 't'
        WHEN (cl.client_id IS NOT NULL) THEN NULL
        WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL
        WHEN '{client_name}' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL 
        WHEN '{client_name}' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
    ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
    END, 
    ip.ip_address, 
    tvis.category, 
    tvis.input_device, 
    CASE WHEN UPPER(tvis.category) = 'APPS' THEN
      CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
         WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
          WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
          WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
         WHEN lower(tis.app_name) = 'unknown' THEN NULL
          ELSE tis.app_name END
    WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
        THEN 'vMVPD'
    END
FROM
    prod.detection.viewing_content_firehose AS c
    JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    JOIN prod.detection.tv AS tv ON c.session_start >= '{start_time}'::timestamp
        AND c.session_start < '{end_time}'::timestamp
        AND c.fk_tvid = tv.tvid 
        AND tv.oem = 'VIZIO'   
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
    LEFT OUTER JOIN prod.detection.input_source inps 
        ON c.fk_input_source_id = inps.input_source_id
    LEFT OUTER JOIN prod.detection.inscape_station_map AS map
        ON map.inscape_station_id = c.fk_station_id
        AND map.mapped_vendor = 'TIVO'
    LEFT OUTER JOIN prod.detection.epg_station AS station
        ON map.mapped_vendor_station_id = station.station_id 
        AND station.vendor_name = map.mapped_vendor
    LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
        ON c.fk_station_id = station_blacklist.station_id
        AND station_blacklist.vendor_name = map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
        ON c.fk_station_id = station_obfs.station_id
        AND station_obfs.vendor_name = map.mapped_vendor  
    LEFT OUTER JOIN prod.detection.epg_schedule_latest AS schedule
        ON map.mapped_vendor_station_id = schedule.fk_station_id
        AND schedule.vendor_name = map.mapped_vendor
        AND timestampadd(SECOND, c.media_time_start, c.airdate) >= schedule.airdate  
        AND timestampadd(SECOND, c.media_time_start, c.airdate) <  schedule.airdate_end
        AND schedule.airdate >= '{start_time}'::timestamp - interval '60' day
        AND schedule.airdate <= '{end_time}'::timestamp + interval '2' day
    LEFT OUTER JOIN prod.detection.epg_show AS show
        ON schedule.fk_show_id = show.show_id
        AND show.vendor_name = map.mapped_vendor
    LEFT OUTER JOIN prod.detection.epg_show AS backup_show
        ON backup_show.show_id = c.fk_show_id 
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
        ON c.vizio_epg_station = vizio_station.station_id
    LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
        ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
        AND c.vizio_epg_program != '0'
    JOIN prod.detection.content_ids_firehose AS cid
        ON cid.content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
        ON m.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cl
        ON m.fk_client_id = cl.client_id
        AND cl.client_name <> '{client_name}'
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
        ON md.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cli
        ON md.fk_client_id = cli.client_id
        AND cli.client_name = '{client_name}'
    JOIN prod.detection.tv_input_stats_firehose  tvis 
        ON c.session_start >= tvis.create_timestamp
        AND c.session_start < tvis.next_create_timestamp
        AND tvis.create_timestamp <= '{end_time}'::timestamp
        AND tvis.next_create_timestamp >= '{start_time}'::timestamp
        AND  c.fk_tvid = tvis.fk_tvid
        AND  c.fk_input_source_id = tvis.fk_input_source_id
    LEFT OUTER JOIN prod.detection.tv_inputsource tis
        ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
        AND c.session_start < (tis.next_create_timestamp::double)::timestamp
        AND c.fk_tvid = tis.fk_tvid
        AND c.fk_input_source_id = tis.fk_input_source_id
        AND tis.create_timestamp <= ('{end_time}'::timestamp::double)::timestamp 
        AND tis.next_create_timestamp >= ('{start_time}'::timestamp::double)::timestamp 
    LEFT OUTER JOIN activity_obfuscation AS appb
        ON tis.app_name = appb.app_name
    LEFT OUTER JOIN viewing_obfuscation AS acrb 
        ON tis.app_name = acrb.app_name 
    LEFT OUTER JOIN 
        prod.detection.free_channels_distribution_blacklist chanb 
        ON vizio_station.name = chanb.channel_name
    LEFT OUTER JOIN
        prod.detection.tv_ip_address AS ip
        ON c.session_start >= ip.create_timestamp
        AND c.session_start < ip.next_create_timestamp
        AND ip.create_timestamp <= '{end_time}'::timestamp
        AND ip.next_create_timestamp >= '{start_time}'::timestamp
        AND tv.tvid = ip.fk_tvid
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
        ON nielsen_blacklist.station_id = map.inscape_station_id 
        AND c.session_start >= nielsen_blacklist.blacklist_start 
        AND c.session_start < nielsen_blacklist.blacklist_end
        AND '{client_name}' != 'nielsen'  
    LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
        ON map.inscape_station_id = rep_local.station_id 
        AND c.airdate = rep_local.airdate
        AND c.fk_show_id = rep_local.fk_show_id 
        AND c.fk_dma_id = rep_local.dma_id
    LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
        ON map.inscape_station_id = rep_nyc_nat.station_id 
        AND c.airdate = rep_nyc_nat.airdate
        AND c.fk_show_id  = rep_nyc_nat.fk_show_id 
    WHERE
        c.session_start >= '{start_time}'::timestamp
    AND c.session_start < '{end_time}'::timestamp
    AND c.partition_key >= '{start_time.split('T', 1)[0]}'
    AND c.partition_key <= '{end_time.split('T', 1)[0]}'
    AND ABS(MOD(c.fk_tvid, 10)) = 0
    AND CASE c.file_ingested
        WHEN true THEN
            CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
            WHEN true THEN SPLIT(cid.content_cid, '_')[1]
            ELSE NULL
            END
        ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
        END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign);
"""

# COMMAND ----------

tms_content_null_existing_q = f"""
DROP TABLE IF EXISTS dev.mohit_gangwani.{table_name_existing}_tms;

CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.{table_name_existing}_tms (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.{table_name_existing}_tms (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
WITH activity_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_activity_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{client_name}'
    WHERE override.app_name IS NULL
)
, viewing_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{client_name}'
    WHERE override.app_name IS NULL
)
, station_distribution_blacklist AS (
    WITH agg AS (
        SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
        FROM prod.detection.station_distribution_obfuscation_overwrite
        GROUP BY 1, 2)
    SELECT station_id, vendor_name
    FROM agg
    WHERE cl_list NOT ILIKE '%{client_name}%'
)
SELECT /*+ BROADCAST(cid) */  DISTINCT
    COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
    '', 
    NULLIF(location.zipcode, ''), 
    REPLACE(dma.dma_name, ',', ''), 
    NULLIF(CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TMS' != 'TMS' THEN NULL 
            ELSE vizio_program.program_tms_id
        END
    ELSE
        CASE WHEN acrb.app_name IS NOT NULL THEN NULL
            WHEN (cl.client_id is not null) THEN NULL
        ELSE
            CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
                WHEN station_blacklist.station_id IS NOT NULL THEN NULL
                WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
            ELSE show.database_key
        END
    END
    END,''), 
    CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
            WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
                THEN vizio_program.series_aggregate_title
            ELSE vizio_program.title END
    WHEN acrb.app_name IS NOT NULL THEN NULL
    WHEN (cl.client_id is not null) THEN NULL
    WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
    WHEN station_blacklist.station_id IS NOT NULL THEN NULL
    ELSE CASE WHEN c.file_ingested THEN NULL 
        WHEN '{client_name}' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
        ELSE REPLACE(show.title, ',', '') END 
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN c.airdate
        WHEN (cl.client_id is not null) THEN NULL
        WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
        WHEN '{client_name}' != 'nielsen' THEN COALESCE(schedule.airdate, c.airdate)
        ELSE schedule.airdate
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
        WHEN (cl.client_id is not null) THEN NUll
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_obfs.station_id IS NOT NULL THEN NULL
        WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
        WHEN '{client_name}' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
        WHEN '{client_name}' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
        ELSE map.inscape_call_sign 
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
        WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
        WHEN (cl.client_id is not null) THEN NUll
        WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
        WHEN '{client_name}' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
        WHEN '{client_name}' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
        ELSE LEAST(c.media_time_start, schedule.duration) 
    END, 
    c.session_start, 
    c.session_end, 
    CASE WHEN c.vizio_epg_station IS NOT NULL
    THEN CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED' 
        ELSE vizio_station.name END
    WHEN acrb.app_name IS NOT NULL THEN NULL
    WHEN (cl.client_id IS NOT NULL) THEN NULL
    ELSE
        CASE 
            WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
            WHEN station_obfs.station_id IS NOT NULL THEN NULL 
            WHEN '{client_name}' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL
            WHEN '{client_name}' = 'nielsen' AND schedule.airdate IS NULL THEN NULL
            WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
            WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
                OR LOWER(station.station_affil) LIKE '%independent%'
                OR LOWER(station.station_affil) LIKE '%low power%')
            THEN station.station_affil ELSE NULL END
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN 't'
        WHEN (cl.client_id IS NOT NULL) THEN NULL
        WHEN cid.content_cid = 'unknown' AND c.tuner_channel_id IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL
        WHEN '{client_name}' != 'nielsen' AND COALESCE(schedule.airdate, c.airdate) IS NULL THEN NULL 
        WHEN '{client_name}' = 'nielsen' AND schedule.airdate IS NULL THEN NULL 
    ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
    END, 
    ip.ip_address, 
    tvis.category, 
    tvis.input_device, 
    CASE WHEN UPPER(tvis.category) = 'APPS' THEN
      CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
         WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
          WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
          WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
         WHEN lower(tis.app_name) = 'unknown' THEN NULL
          ELSE tis.app_name END
    WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
        THEN 'vMVPD'
    END
FROM
    detection.viewing_content_firehose AS c
    JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    JOIN prod.detection.tv AS tv ON c.session_start >= '{start_time}'::timestamp
        AND c.session_start < '{end_time}'::timestamp
        AND c.fk_tvid = tv.tvid 
        AND tv.oem = 'VIZIO'   
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
    LEFT OUTER JOIN prod.detection.input_source inps 
        ON c.fk_input_source_id = inps.input_source_id
    LEFT OUTER JOIN prod.detection.inscape_station_map AS map
        ON map.inscape_station_id = c.fk_station_id
        AND map.mapped_vendor = 'TMS'
    LEFT OUTER JOIN prod.detection.epg_station AS station
        ON map.mapped_vendor_station_id = station.station_id 
        AND station.vendor_name = map.mapped_vendor
    LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
        ON c.fk_station_id = station_blacklist.station_id
        AND station_blacklist.vendor_name = map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
        ON c.fk_station_id = station_obfs.station_id
        AND station_obfs.vendor_name = map.mapped_vendor  
    LEFT OUTER JOIN prod.detection.epg_schedule_latest AS schedule
        ON map.mapped_vendor_station_id = schedule.fk_station_id
        AND schedule.vendor_name = map.mapped_vendor
        AND timestampadd(SECOND, c.media_time_start, c.airdate) >= schedule.airdate  
        AND timestampadd(SECOND, c.media_time_start, c.airdate) <  schedule.airdate_end
        AND schedule.airdate >= '{start_time}'::timestamp - interval '60' day
        AND schedule.airdate <= '{end_time}'::timestamp + interval '2' day
    LEFT OUTER JOIN prod.detection.epg_show AS show
        ON schedule.fk_show_id = show.show_id
        AND show.vendor_name = map.mapped_vendor
    LEFT OUTER JOIN prod.detection.epg_show AS backup_show
        ON backup_show.show_id = c.fk_show_id 
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
        ON c.vizio_epg_station = vizio_station.station_id
    LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
        ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
        AND c.vizio_epg_program != '0'
    JOIN prod.detection.content_ids_firehose AS cid
        ON cid.content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
        ON m.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cl
        ON m.fk_client_id = cl.client_id
        AND cl.client_name <> '{client_name}'
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
        ON md.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cli
        ON md.fk_client_id = cli.client_id
        AND cli.client_name = '{client_name}'
    JOIN prod.detection.tv_input_stats_firehose  tvis 
        ON c.session_start >= tvis.create_timestamp
        AND c.session_start < tvis.next_create_timestamp
        AND tvis.create_timestamp <= '{end_time}'::timestamp
        AND tvis.next_create_timestamp >= '{start_time}'::timestamp
        AND  c.fk_tvid = tvis.fk_tvid
        AND  c.fk_input_source_id = tvis.fk_input_source_id
    LEFT OUTER JOIN prod.detection.tv_inputsource tis
        ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
        AND c.session_start < (tis.next_create_timestamp::double)::timestamp
        AND c.fk_tvid = tis.fk_tvid
        AND c.fk_input_source_id = tis.fk_input_source_id
        AND tis.create_timestamp <= ('{end_time}'::timestamp::double)::timestamp 
        AND tis.next_create_timestamp >= ('{start_time}'::timestamp::double)::timestamp 
    LEFT OUTER JOIN activity_obfuscation AS appb
        ON tis.app_name = appb.app_name
    LEFT OUTER JOIN viewing_obfuscation AS acrb 
        ON tis.app_name = acrb.app_name 
    LEFT OUTER JOIN 
        prod.detection.free_channels_distribution_blacklist chanb 
        ON vizio_station.name = chanb.channel_name
    LEFT OUTER JOIN
        prod.detection.tv_ip_address AS ip
        ON c.session_start >= ip.create_timestamp
        AND c.session_start < ip.next_create_timestamp
        AND ip.create_timestamp <= '{end_time}'::timestamp
        AND ip.next_create_timestamp >= '{start_time}'::timestamp
        AND tv.tvid = ip.fk_tvid
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
        ON nielsen_blacklist.station_id = map.inscape_station_id 
        AND c.session_start >= nielsen_blacklist.blacklist_start 
        AND c.session_start < nielsen_blacklist.blacklist_end
        AND '{client_name}' != 'nielsen'  
    LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
        ON map.inscape_station_id = rep_local.station_id 
        AND c.airdate = rep_local.airdate
        AND c.fk_show_id = rep_local.fk_show_id 
        AND c.fk_dma_id = rep_local.dma_id
    LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
        ON map.inscape_station_id = rep_nyc_nat.station_id 
        AND c.airdate = rep_nyc_nat.airdate
        AND c.fk_show_id  = rep_nyc_nat.fk_show_id 
    WHERE
        c.session_start >= '{start_time}'::timestamp
    AND c.session_start < '{end_time}'::timestamp
    AND c.partition_key >= '{start_time.split('T', 1)[0]}'
    AND c.partition_key <= '{end_time.split('T', 1)[0]}'
    AND ABS(MOD(c.fk_tvid, 10)) = 0
    AND CASE c.file_ingested
        WHEN true THEN
            CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
            WHEN true THEN SPLIT(cid.content_cid, '_')[1]
            ELSE NULL
            END
        ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
        END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
"""

# COMMAND ----------

tivo_content_null_new_q = f"""
DROP TABLE IF EXISTS dev.mohit_gangwani.{table_name_new}_tivo;
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.{table_name_new}_tivo (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer,
    ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.{table_name_new}_tivo (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
WITH activity_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_activity_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{client_name}'
    WHERE override.app_name IS NULL
)
, viewing_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{client_name}'
    WHERE override.app_name IS NULL
)
, station_distribution_blacklist AS (
    WITH agg AS (
        SELECT vendor_station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
        FROM prod.detection.station_distribution_obfuscation_overwrite
        GROUP BY 1, 2)
    SELECT vendor_station_id AS station_id, vendor_name
    FROM agg
    WHERE cl_list NOT ILIKE '%{client_name}%'
)
, inscape_station_map_dedupe AS (
    SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
    FROM (
        SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
        FROM detection.inscape_station_map) ism
    WHERE ism.rn = 1
)
SELECT /*+ BROADCAST(cid) */  DISTINCT
    COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
    '', 
    NULLIF(location.zipcode, ''), 
    REPLACE(dma.dma_name, ',', ''), 
    NULLIF(CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TIVO' != 'TMS' THEN NULL 
            ELSE vizio_program.program_tms_id
        END
    ELSE
        CASE WHEN acrb.app_name IS NOT NULL THEN NULL
            WHEN (cl.client_id is not null) THEN NULL
        ELSE
            CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
                WHEN station_blacklist.station_id IS NOT NULL THEN NULL
                WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
            ELSE show.database_key
        END
    END
    END,''), 
    CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
            WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
                THEN vizio_program.series_aggregate_title
            ELSE vizio_program.title END
    WHEN acrb.app_name IS NOT NULL THEN NULL
    WHEN (cl.client_id is not null) THEN NULL
    WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
    WHEN station_blacklist.station_id IS NOT NULL THEN NULL
    ELSE CASE WHEN c.file_ingested THEN NULL 
        WHEN 'nielsen' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
        ELSE REPLACE(show.title, ',', '') END 
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(c.airdate, c.tms_airdate)
        WHEN (cl.client_id is not null) THEN NULL
        WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
        WHEN '{client_name}' != 'nielsen' THEN COALESCE(c.airdate, c.tms_airdate)
        WHEN '{client_name}' = 'nielsen' THEN c.airdate
        ELSE NULL
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
        WHEN (cl.client_id is not null) THEN NUll
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_obfs.station_id IS NOT NULL THEN NULL
        WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
        WHEN '{client_name}' != 'nielsen' THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
        WHEN '{client_name}' = 'nielsen' AND c.airdate IS NOT NULL THEN map.inscape_call_sign
    END,
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
        WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
        WHEN (cl.client_id is not null) THEN NUll
        WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL
        WHEN '{client_name}' != 'nielsen' THEN LEAST(c.media_time_start, c.runtime)
        WHEN '{client_name}' = 'nielsen' AND c.airdate IS NULL THEN NULL
        ELSE LEAST(c.media_time_start, c.runtime)
    END, 
    c.session_start, 
    c.session_end, 
    CASE WHEN c.vizio_epg_station IS NOT NULL
    THEN CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED' 
        ELSE vizio_station.name END
    WHEN acrb.app_name IS NOT NULL THEN NULL
    WHEN (cl.client_id IS NOT NULL) THEN NULL
    ELSE
        CASE 
            WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
            WHEN station_obfs.station_id IS NOT NULL THEN NULL
            WHEN '{client_name}' = 'nielsen' AND c.airdate IS NULL THEN NULL
            WHEN c.fk_station_id IS NOT NULL THEN
                CASE WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
                     WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
                           OR LOWER(station.station_affil) LIKE '%independent%'
                           OR LOWER(station.station_affil) LIKE '%low power%')
                        THEN station.station_affil ELSE NULL END
            WHEN c.fk_station_id IS NULL AND c.tms_station_id IS NOT NULL THEN
                CASE WHEN (backup_station.inscape_station_name IS NOT NULL) THEN backup_station.inscape_station_name
                     WHEN (LOWER(backup_station.station_affil) LIKE '%affiliate%'
                          OR LOWER(backup_station.station_affil) LIKE '%independent%'
                          OR LOWER(backup_station.station_affil) LIKE '%low power%')
                        THEN backup_station.station_affil ELSE NULL END END
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN 't'
        WHEN (cl.client_id IS NOT NULL) THEN NULL
        WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL
        WHEN '{client_name}' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
        WHEN '{client_name}' = 'nielsen' AND c.airdate IS NULL THEN NULL
    ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
    END, 
    ip.ip_address, 
    tvis.category, 
    tvis.input_device, 
    CASE WHEN UPPER(tvis.category) = 'APPS' THEN
      CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
         WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
          WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
          WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
         WHEN lower(tis.app_name) = 'unknown' THEN NULL
          ELSE tis.app_name END
    WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
        THEN 'vMVPD'
    END
FROM
    dev_temp.detection.viewing_content_firehose_historic_0731 AS c
    JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    JOIN prod.detection.tv AS tv ON c.session_start >= '{start_time}'::timestamp
        AND c.session_start < '{end_time}'::timestamp
        AND c.fk_tvid = tv.tvid 
        AND tv.oem = 'VIZIO'   
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
    LEFT OUTER JOIN prod.detection.input_source inps 
        ON c.fk_input_source_id = inps.input_source_id
-- Historical change start
    LEFT OUTER JOIN inscape_station_map_dedupe AS map
        ON map.mapped_vendor_station_id = c.fk_station_id
        AND map.mapped_vendor = 'TIVO'
    LEFT OUTER JOIN detection.epg_station AS station
        ON station.station_id = c.fk_station_id
        AND station.vendor_name = 'TIVO'
    LEFT OUTER JOIN detection.epg_show AS show
        ON show.show_id = c.fk_show_id
        AND show.vendor_name = 'TIVO'
    -- Backup values
    LEFT OUTER JOIN detection.epg_show AS backup_show
        ON backup_show.show_id = c.tms_show_id
       AND c.fk_show_id IS NULL
       AND backup_show.vendor_name = 'TMS'
       AND '{client_name}' != 'nielsen'
    LEFT OUTER JOIN detection.epg_station AS backup_station
        ON backup_station.station_id = c.tms_station_id
       AND c.fk_station_id IS NULL
       AND backup_station.vendor_name = 'TMS'
       AND '{client_name}' != 'nielsen'
    LEFT OUTER JOIN inscape_station_map_dedupe AS backup_map
        ON backup_map.mapped_vendor_station_id = c.tms_station_id
        AND c.fk_station_id IS NULL
        AND backup_map.mapped_vendor = 'TMS'
        AND '{client_name}' != 'nielsen'
-- Historical change End
    LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
        ON c.fk_station_id = station_blacklist.station_id
        AND station_blacklist.vendor_name = map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
        ON c.fk_station_id = station_obfs.vendor_station_id
        AND station_obfs.vendor_name = map.mapped_vendor  
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
        ON c.vizio_epg_station = vizio_station.station_id
    LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
        ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
        AND c.vizio_epg_program != '0'
    JOIN prod.detection.content_ids_firehose AS cid
        ON cid.content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
        ON m.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cl
        ON m.fk_client_id = cl.client_id
        AND cl.client_name <> '{client_name}'
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
        ON md.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cli
        ON md.fk_client_id = cli.client_id
        AND cli.client_name = '{client_name}'
    JOIN prod.detection.tv_input_stats_firehose  tvis 
        ON c.session_start >= tvis.create_timestamp
        AND c.session_start < tvis.next_create_timestamp
        AND tvis.create_timestamp <= '{end_time}'::timestamp
        AND tvis.next_create_timestamp >= '{start_time}'::timestamp
        AND  c.fk_tvid = tvis.fk_tvid
        AND  c.fk_input_source_id = tvis.fk_input_source_id
    LEFT OUTER JOIN prod.detection.tv_inputsource tis
        ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
        AND c.session_start < (tis.next_create_timestamp::double)::timestamp
        AND c.fk_tvid = tis.fk_tvid
        AND c.fk_input_source_id = tis.fk_input_source_id
        AND tis.create_timestamp <= ('{end_time}'::timestamp::double)::timestamp 
        AND tis.next_create_timestamp >= ('{start_time}'::timestamp::double)::timestamp 
    LEFT OUTER JOIN activity_obfuscation AS appb
        ON tis.app_name = appb.app_name
    LEFT OUTER JOIN viewing_obfuscation AS acrb 
        ON tis.app_name = acrb.app_name 
    LEFT OUTER JOIN 
        prod.detection.free_channels_distribution_blacklist chanb 
        ON vizio_station.name = chanb.channel_name
    LEFT OUTER JOIN
        prod.detection.tv_ip_address AS ip
        ON c.session_start >= ip.create_timestamp
        AND c.session_start < ip.next_create_timestamp
        AND ip.create_timestamp <= '{end_time}'::timestamp
        AND ip.next_create_timestamp >= '{start_time}'::timestamp
        AND tv.tvid = ip.fk_tvid
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
        ON nielsen_blacklist.station_id = map.inscape_station_id 
        AND c.session_start >= nielsen_blacklist.blacklist_start 
        AND c.session_start < nielsen_blacklist.blacklist_end
        AND '{client_name}' != 'nielsen'
    LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
        ON map.inscape_station_id = rep_local.station_id 
        AND c.airdate = rep_local.airdate
        AND c.fk_show_id = rep_local.fk_show_id 
        AND c.fk_dma_id = rep_local.dma_id
        AND '{client_name}' != 'nielsen'
    LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
        ON map.inscape_station_id = rep_nyc_nat.station_id 
        AND c.airdate = rep_nyc_nat.airdate
        AND c.fk_show_id  = rep_nyc_nat.fk_show_id 
        AND '{client_name}' != 'nielsen'
    WHERE
        c.session_start >= '{start_time}'::timestamp
    AND c.session_start < '{end_time}'::timestamp
    AND c.partition_key >= '{start_time.split('T', 1)[0]}'
    AND c.partition_key <= '{end_time.split('T', 1)[0]}'
    AND ABS(MOD(c.fk_tvid, 10)) = 0
    AND CASE c.file_ingested
        WHEN true THEN
            CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
            WHEN true THEN SPLIT(cid.content_cid, '_')[1]
            ELSE NULL
            END
        ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
        END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign);
"""

# COMMAND ----------

tms_content_null_new_q = f"""
DROP TABLE IF EXISTS dev.mohit_gangwani.{table_name_new}_tms;
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.{table_name_new}_tms (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.{table_name_new}_tms (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
WITH activity_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_activity_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{client_name}'
    WHERE override.app_name IS NULL
)
, viewing_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{client_name}'
    WHERE override.app_name IS NULL
)
, station_distribution_blacklist AS (
    WITH agg AS (
        SELECT vendor_station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
        FROM prod.detection.station_distribution_obfuscation_overwrite
        GROUP BY 1, 2)
    SELECT vendor_station_id AS station_id, vendor_name
    FROM agg
    WHERE cl_list NOT ILIKE '%{client_name}%'
)
, inscape_station_map_dedupe AS (
    SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
    FROM (
        SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
        FROM detection.inscape_station_map) ism
    WHERE ism.rn = 1
)
SELECT /*+ BROADCAST(cid) */  DISTINCT
    COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
    '', 
    NULLIF(location.zipcode, ''), 
    REPLACE(dma.dma_name, ',', ''), 
    NULLIF(CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) OR 'TMS' != 'TMS' THEN NULL 
            ELSE vizio_program.program_tms_id
        END
    ELSE
        CASE WHEN acrb.app_name IS NOT NULL THEN NULL
            WHEN (cl.client_id is not null) THEN NULL
        ELSE
            CASE WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
                WHEN station_blacklist.station_id IS NOT NULL THEN NULL
                WHEN (c.file_ingested = true) THEN COALESCE(md.external_id,SPLIT(cid.content_cid, '_')[0])
            ELSE show.database_key
        END
    END
    END,''), 
    CASE
    WHEN c.vizio_epg_station IS NOT NULL THEN
        CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
            WHEN vizio_program.series_aggregate_title IS NOT NULL AND vizio_program.series_aggregate_title != ''
                THEN vizio_program.series_aggregate_title
            ELSE vizio_program.title END
    WHEN acrb.app_name IS NOT NULL THEN NULL
    WHEN (cl.client_id is not null) THEN NULL
    WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
    WHEN station_blacklist.station_id IS NOT NULL THEN NULL
    ELSE CASE WHEN c.file_ingested THEN NULL 
        WHEN '{client_name}' != 'nielsen' THEN REPLACE(COALESCE(show.title, backup_show.title), ',', '')
        ELSE REPLACE(show.title, ',', '') END 
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN COALESCE(c.tms_airdate, c.airdate)
        WHEN (cl.client_id is not null) THEN NULL
        WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL 
        WHEN '{client_name}' != 'nielsen' THEN COALESCE(c.tms_airdate, c.airdate)
        WHEN '{client_name}' = 'nielsen' THEN c.tms_airdate
        ELSE NULL
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN map.inscape_call_sign
        WHEN (cl.client_id is not null) THEN NUll
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_obfs.station_id IS NOT NULL THEN NULL
        WHEN c.file_ingested = true THEN SPLIT(cid.content_cid, '_')[1]
        WHEN '{client_name}' != 'nielsen' THEN COALESCE(map.inscape_call_sign, backup_map.inscape_call_sign)
        WHEN '{client_name}' = 'nielsen' AND c.tms_airdate IS NOT NULL THEN map.inscape_call_sign
    END,
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN NULL 
        WHEN c.vizio_epg_station IS NOT NULL THEN c.media_time_start
        WHEN (cl.client_id is not null) THEN NUll
        WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL
        WHEN '{client_name}' != 'nielsen' THEN LEAST(c.media_time_start, c.runtime)
        WHEN '{client_name}' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
        ELSE LEAST(c.media_time_start, c.runtime)
    END, 
    c.session_start, 
    c.session_end, 
    CASE WHEN c.vizio_epg_station IS NOT NULL
    THEN CASE WHEN chanb.channel_name IS NOT NULL OR c.vizio_epg_station in (98989898989898, 9898989898, -1) THEN 'OBFUSCATED' 
        ELSE vizio_station.name END
    WHEN acrb.app_name IS NOT NULL THEN NULL
    WHEN (cl.client_id IS NOT NULL) THEN NULL
    ELSE
        CASE 
            WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
            WHEN station_obfs.station_id IS NOT NULL THEN NULL
            -- WHEN '{client_name}' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
            WHEN '{client_name}' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
            WHEN c.tms_station_id IS NOT NULL THEN
                CASE WHEN (station.inscape_station_name IS NOT NULL) THEN station.inscape_station_name
                     WHEN (LOWER(station.station_affil) LIKE '%affiliate%'
                           OR LOWER(station.station_affil) LIKE '%independent%'
                           OR LOWER(station.station_affil) LIKE '%low power%')
                        THEN station.station_affil ELSE NULL END
            WHEN c.tms_station_id IS NULL AND c.fk_station_id IS NOT NULL THEN
                CASE WHEN (backup_station.inscape_station_name IS NOT NULL) THEN backup_station.inscape_station_name
                     WHEN (LOWER(backup_station.station_affil) LIKE '%affiliate%'
                          OR LOWER(backup_station.station_affil) LIKE '%independent%'
                          OR LOWER(backup_station.station_affil) LIKE '%low power%')
                        THEN backup_station.station_affil ELSE NULL END END
    END, 
    CASE
        WHEN c.vizio_epg_station IS NULL AND acrb.app_name IS NOT NULL THEN NULL
        WHEN c.vizio_epg_station IS NOT NULL THEN 't'
        WHEN (cl.client_id IS NOT NULL) THEN NULL
        WHEN cid.content_cid = 'unknown' AND COALESCE(c.tuner_channel_id, c.tms_tuner_channel_id) IS NOT NULL THEN NULL
        WHEN nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local.station_id, rep_nyc_nat.station_id) IS NULL OR ingest_time IS NOT NULL) THEN NULL
        WHEN station_blacklist.station_id IS NOT NULL THEN NULL
        WHEN '{client_name}' != 'nielsen' AND COALESCE(c.airdate, c.tms_airdate) IS NULL THEN NULL
        WHEN '{client_name}' = 'nielsen' AND c.tms_airdate IS NULL THEN NULL
    ELSE CASE WHEN c.is_live = TRUE THEN 't' WHEN c.is_live = FALSE THEN 'f' ELSE NULL END
    END, 
    ip.ip_address, 
    tvis.category, 
    tvis.input_device, 
    CASE WHEN UPPER(tvis.category) = 'APPS' THEN
      CASE WHEN c.vizio_epg_station IS NOT NULL THEN 'WatchFree+'
         WHEN c.vizio_epg_station IS NULL and tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
          WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
          WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND cid.content_cid <> 'unknown' THEN NULL
         WHEN lower(tis.app_name) = 'unknown' THEN NULL
          ELSE tis.app_name END
    WHEN c.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku')
        THEN 'vMVPD'
    END
FROM
    dev_temp.detection.viewing_content_firehose_historic_0731 AS c
    JOIN prod.detection.zoo AS z ON c.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    JOIN prod.detection.tv AS tv ON c.session_start >= '{start_time}'::timestamp
        AND c.session_start < '{end_time}'::timestamp
        AND c.fk_tvid = tv.tvid 
        AND tv.oem = 'VIZIO'   
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
    LEFT OUTER JOIN prod.detection.input_source inps 
        ON c.fk_input_source_id = inps.input_source_id
-- Historical change start
    LEFT OUTER JOIN inscape_station_map_dedupe AS map
        ON map.mapped_vendor_station_id = c.tms_station_id
        AND map.mapped_vendor = 'TMS'
    LEFT OUTER JOIN detection.epg_station AS station
        ON station.station_id = c.tms_station_id
        AND station.vendor_name = 'TMS'
    LEFT OUTER JOIN detection.epg_show AS show
        ON show.show_id = c.tms_show_id
        AND show.vendor_name = 'TMS'
    -- Backup values
    LEFT OUTER JOIN detection.epg_show AS backup_show
        ON backup_show.show_id = c.fk_show_id
       AND c.tms_show_id IS NULL
       AND backup_show.vendor_name = 'TIVO'
       AND '{client_name}' != '{client_name}'
    LEFT OUTER JOIN detection.epg_station AS backup_station
        ON backup_station.station_id = c.fk_station_id
       AND c.tms_station_id IS NULL
       AND backup_station.vendor_name = 'TIVO'
       AND '{client_name}' != '{client_name}'
    LEFT OUTER JOIN inscape_station_map_dedupe AS backup_map
        ON backup_map.mapped_vendor_station_id = c.fk_station_id
        AND c.tms_station_id IS NULL
        AND backup_map.mapped_vendor = 'TIVO'
        AND '{client_name}' != '{client_name}'
-- Historical change End
    LEFT OUTER JOIN station_distribution_blacklist AS station_blacklist
        ON c.tms_station_id = station_blacklist.station_id
        AND station_blacklist.vendor_name = map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS station_obfs
        ON c.tms_station_id = station_obfs.vendor_station_id
        AND station_obfs.vendor_name = map.mapped_vendor  
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS vizio_station 
        ON c.vizio_epg_station = vizio_station.station_id
    LEFT OUTER JOIN prod.detection.vizio_epg_program_aggregate AS vizio_program
        ON CAST(c.vizio_epg_program AS BIGINT) = vizio_program.program_aggregate_id
        AND c.vizio_epg_program != '0'
    JOIN prod.detection.content_ids_firehose AS cid
        ON cid.content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m
        ON m.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cl
        ON m.fk_client_id = cl.client_id
        AND cl.client_name <> '{client_name}'
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS md
        ON md.fk_content_id = c.fk_content_id
    LEFT OUTER JOIN prod.detection.clients cli
        ON md.fk_client_id = cli.client_id
        AND cli.client_name = '{client_name}'
    JOIN prod.detection.tv_input_stats_firehose  tvis 
        ON c.session_start >= tvis.create_timestamp
        AND c.session_start < tvis.next_create_timestamp
        AND tvis.create_timestamp <= '{end_time}'::timestamp
        AND tvis.next_create_timestamp >= '{start_time}'::timestamp
        AND  c.fk_tvid = tvis.fk_tvid
        AND  c.fk_input_source_id = tvis.fk_input_source_id
    LEFT OUTER JOIN prod.detection.tv_inputsource tis
        ON  c.session_start >= (tis.create_timestamp::double)::timestamp   
        AND c.session_start < (tis.next_create_timestamp::double)::timestamp
        AND c.fk_tvid = tis.fk_tvid
        AND c.fk_input_source_id = tis.fk_input_source_id
        AND tis.create_timestamp <= ('{end_time}'::timestamp::double)::timestamp 
        AND tis.next_create_timestamp >= ('{start_time}'::timestamp::double)::timestamp 
    LEFT OUTER JOIN activity_obfuscation AS appb
        ON tis.app_name = appb.app_name
    LEFT OUTER JOIN viewing_obfuscation AS acrb 
        ON tis.app_name = acrb.app_name 
    LEFT OUTER JOIN 
        prod.detection.free_channels_distribution_blacklist chanb 
        ON vizio_station.name = chanb.channel_name
    LEFT OUTER JOIN
        prod.detection.tv_ip_address AS ip
        ON c.session_start >= ip.create_timestamp
        AND c.session_start < ip.next_create_timestamp
        AND ip.create_timestamp <= '{end_time}'::timestamp
        AND ip.next_create_timestamp >= '{start_time}'::timestamp
        AND tv.tvid = ip.fk_tvid
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS nielsen_blacklist
        ON nielsen_blacklist.station_id = map.inscape_station_id 
        AND c.session_start >= nielsen_blacklist.blacklist_start 
        AND c.session_start < nielsen_blacklist.blacklist_end
        AND '{client_name}' != 'nielsen'
    LEFT OUTER JOIN prod.detection.nielsen_replacement_local AS rep_local
        ON map.inscape_station_id = rep_local.station_id 
        AND c.airdate = rep_local.airdate
        AND c.fk_show_id = rep_local.fk_show_id 
        AND c.fk_dma_id = rep_local.dma_id
        AND '{client_name}' != 'nielsen'
    LEFT OUTER JOIN prod.detection.nielsen_replacement_national_nyc AS rep_nyc_nat
        ON map.inscape_station_id = rep_nyc_nat.station_id 
        AND c.airdate = rep_nyc_nat.airdate
        AND c.fk_show_id  = rep_nyc_nat.fk_show_id 
        AND '{client_name}' != 'nielsen'
    WHERE
        c.session_start >= '{start_time}'::timestamp
    AND c.session_start < '{end_time}'::timestamp
    AND c.partition_key >= '{start_time.split('T', 1)[0]}'
    AND c.partition_key <= '{end_time.split('T', 1)[0]}'
    AND ABS(MOD(c.fk_tvid, 10)) = 0
    AND CASE c.file_ingested
        WHEN true THEN
            CASE NULLIF(SPLIT(cid.content_cid, '_')[1], '') IS NOT NULL AND NULLIF(SPLIT(cid.content_cid, '_')[2], '') IS NULL
            WHEN true THEN SPLIT(cid.content_cid, '_')[1]
            ELSE NULL
            END
        ELSE COALESCE(station.station_call_sign, 'KeepSessionForNullReport' )
        END NOT IN (SELECT DISTINCT chan_callsign FROM customer_reports.bad_chan_callsign)
"""

# COMMAND ----------

print(tivo_content_null_existing_q)

# COMMAND ----------

print(tivo_content_null_new_q)

# COMMAND ----------

print(tms_content_null_existing_q)

# COMMAND ----------

print(tms_content_null_new_q)

# COMMAND ----------


