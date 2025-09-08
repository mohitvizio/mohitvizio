# Databricks notebook source
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("client_name", "")

start_time = dbutils.widgets.get("start_date")
end_time = dbutils.widgets.get("end_date")
client_name = dbutils.widgets.get("client_name")
table_name = 'deduped_commercial_feed_'

table_name_existing = f'{table_name}{client_name}_existing_table'
table_name_new = f'{table_name}{client_name}_new_table'

print(f"Running Report from {start_time} to {end_time} for {client_name}")
print(f"loading in {table_name_existing} and {table_name_new}")

# COMMAND ----------

existing_part_one = f"""
DROP TABLE IF EXISTS dev.mohit_gangwani.{table_name_existing}_{tivo_tms};

CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.{table_name_existing}_{tivo_tms} (
    tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip_null string, input_category string, input_device string, app_service string, vizio_epg_channel_id string, vizio_epg_program_id string
    );

INSERT INTO dev.mohit_gangwani.{table_name_existing}_{tivo_tms} (
    tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip_null, input_category, input_device, app_service, vizio_epg_channel_id, vizio_epg_program_id
)
WITH clients_table_to_join AS (
    SELECT *
    FROM prod.detection.clients 
    WHERE 
        client_name = 'kinetiq'
)
,nielsen_replacement_national_nyc_alias AS (
    SELECT station_id, fk_show_id, tuner_channel_id, tuner_program_id
    FROM prod.detection.nielsen_replacement_national_nyc
    GROUP BY 1,2,3,4
)
,nielsen_replacement_local_alias AS (
    SELECT station_id, fk_show_id, dma_id, tuner_channel_id, tuner_program_id
    FROM prod.detection.nielsen_replacement_local
    GROUP BY 1,2,3,4,5
)
, activity_obfuscation AS (
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
),
epg_schedule_latest AS (
    SELECT DISTINCT *
    FROM prod.detection.epg_schedule_latest sch
    WHERE sch.airdate >= '{start_time}'::timestamp - interval '60' day
      AND sch.airdate <= '{end_time}'::timestamp + interval '2' day
),
epg_program_aggregate AS (
    SELECT DISTINCT *
    FROM prod.detection.vizio_epg_program_aggregate 
),
viewing_content_firehose AS (
    SELECT DISTINCT fk_tvid, fk_station_id, media_time_start, session_start, session_end, airdate, fk_content_id, is_live
    FROM prod.detection.viewing_content_firehose AS content
    WHERE session_start >= '{start_time}'::timestamp
        AND session_start < '{end_time}'::timestamp
),
content_ids_firehose AS (
    SELECT * FROM detection.content_ids_firehose AS cid
    WHERE content_id IN (
        SELECT DISTINCT c.fk_content_id
        FROM prod.detection.viewing_content_firehose AS c
        WHERE c.session_start >= '{start_time}'::timestamp
            AND c.session_start <= '{end_time}'::timestamp
    )
), station_distribution_blacklist AS (
    WITH agg AS (
        SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
        FROM prod.detection.station_distribution_obfuscation_overwrite
        GROUP BY 1, 2)
    SELECT station_id, vendor_name
    FROM agg
    WHERE cl_list NOT ILIKE '%{client_name}%'
)
"""

# COMMAND ----------

part_two = f"""
SELECT 
/*+ BROADCAST(m), 
  BROADCAST(cl), 
  BROADCAST(tp), 
  BROADCAST(pop),
  BROADCAST(location), 
  BROADCAST(epg_program_aggregate), 
  BROADCAST(prev_cid), 
  BROADCAST(next_cid), 
  BROADCAST(next_map),
  BROADCAST(next_vizio_station),
  BROADCAST(next_vizio_program),
  BROADCAST(prev_vizio_station),
  BROADCAST(prev_vizio_program),
  BROADCAST(next_schedule),
  BROADCAST(next_program),
  BROADCAST(next_station),
  BROADCAST(next_schedule),
  BROADCAST(next_program),
  BROADCAST(next_program_alt),
  BROADCAST(prev_program_alt),
  BROADCAST(prev_map),
  BROADCAST(prev_station),
  BROADCAST(prev_schedule),
  BROADCAST(prev_program),
  RANGE_JOIN(next_schedule, 604800),
  RANGE_JOIN(prev_schedule, 604800) */
DISTINCT 
    COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
    '', 
    NULLIF(location.zipcode, ''), 
    REPLACE(dma.dma_name, ',', ''), 
    m.external_id, 
    c.media_time_start, 
    c.session_start, 
    c.session_end, 
"""

# COMMAND ----------

existing_select = f"""
    NULLIF(CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL 
 WHEN c.prev_station_id IS NOT NULL AND prev_schedule.fk_show_id IS NULL THEN NULL
 WHEN (prev_station_id = 0) THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0]) 
 WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
 WHEN prev_program.database_key IS NOT NULL THEN prev_program.database_key 
 WHEN '{tivo_tms.upper()}' = 'TMS' AND prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
 ELSE NULL
 END,''), 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN (prev_station_id = 0) THEN prev_filecontent.title
 WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
 ELSE REPLACE(COALESCE(prev_program.title, prev_program_alt.title,
 CASE WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
 ELSE prev_vizio_program.title
 END), ',', '')
 END, 
    c.prev_session_start, 
    c.prev_session_end, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL 
 WHEN c.prev_station_id IS NOT NULL THEN prev_map.inscape_call_sign
 WHEN (prev_station_id = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
 ELSE NULL
 END, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL
 WHEN (prev_station.inscape_station_name IS NOT NULL)
 THEN prev_station.inscape_station_name
 WHEN (LOWER(prev_station.station_affil) LIKE '%affiliate%' OR LOWER(prev_station.station_affil) LIKE '%independent%' OR LOWER(prev_station.station_affil) LIKE '%low power%')
 THEN prev_station.station_affil
 WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
 WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
 ELSE NULL
 END, 
    NULLIF(CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL 
 WHEN c.next_station_id IS NOT NULL AND next_schedule.fk_show_id IS NULL THEN NULL
 WHEN (next_station_id = 0) THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0]) 
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
 WHEN next_program.database_key IS NOT NULL THEN next_program.database_key
 WHEN '{tivo_tms.upper()}' = 'TMS' AND next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
 ELSE NULL
 END,''), 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN (next_station_id = 0) THEN next_filecontent.title
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
 ELSE REPLACE(COALESCE(next_program.title, next_program_alt.title,
 CASE WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
 ELSE next_vizio_program.title
 END), ',', '')
 END, 
    c.next_session_start, 
    c.next_session_end, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN c.next_station_id IS NOT NULL THEN
 CASE WHEN NVL(next_program.show_id, next_program_alt.show_id) IS NULL THEN NULL 
 ELSE next_map.inscape_call_sign END
 WHEN (next_station_id = 0) THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
 END, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN c.next_station_id IS NOT NULL AND NVL(next_program.show_id, next_program_alt.show_id) IS NULL THEN NULL 
 WHEN (next_station.inscape_station_name IS NOT NULL)
 THEN next_station.inscape_station_name
 WHEN (LOWER(next_station.station_affil) LIKE '%affiliate%' OR LOWER(next_station.station_affil) LIKE '%independent%' OR LOWER(next_station.station_affil) LIKE '%low power%')
 THEN next_station.station_affil
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
 WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
 ELSE NULL
 END, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED'
 AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN c.prev_station_id IS NOT NULL AND NVL(prev_program.show_id, prev_program_alt.show_id) IS NULL THEN NULL
 ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
 END, 
    REPLACE(m.brand_name, ',', ''), 
    REPLACE(m.title, ',', ''), 
    m.duration, 
    NULL AS ip, 
    tvis.category, 
    tvis.input_device, 
    CASE WHEN UPPER(tvis.category) = 'APPS' THEN
        CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
        WHEN c.prev_vizio_epg_station IS NULL AND tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
            WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
            WHEN LOWER(coalesce(tis.app_name)) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND prev_show_id IS NOT NULL THEN NULL
            WHEN LOWER(coalesce(tis.app_name)) = 'unknown' THEN NULL
            ELSE coalesce(tis.app_name) END
        WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
    END, 
    prev_vizio_station.station_id, 
    prev_vizio_program.program_aggregate_id
"""

# COMMAND ----------

existing_table_with_schema = f'prod.detection.viewing_commercials_firehose_dedup'
initial_from = f"""
    FROM {existing_table_with_schema} AS c
    JOIN prod.detection.zoo AS z 
        ON c.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    INNER JOIN
        prod.detection.tv AS tv
        ON c.fk_tvid = tv.tvid
        AND tv.oem = 'VIZIO'
    JOIN
        prod.detection.tv_populations AS tp
        ON c.fk_tvid = tp.fk_tvid 
    JOIN
        prod.detection.populations AS pop
        ON tp.fk_population_id = pop.population_id 
        AND LOWER(pop.population_name) = 'opted_in'
    JOIN
        prod.detection.commercial_id_external_firehose AS m
        ON c.fk_commercial_id = m.fk_commercial_id
    JOIN
        clients_table_to_join cl
        ON m.fk_client_id = cl.client_id
    JOIN
        prod.detection.location AS location
        ON c.fk_location_id = location.location_id
        AND UPPER(location.country_code) = 'US'
    JOIN 
        prod.detection.tv_input_stats_firehose  tvis    
        ON c.session_start >= tvis.create_timestamp 
        AND c.session_start < tvis.next_create_timestamp
        AND tvis.create_timestamp <= '{end_time}'::timestamp
        AND tvis.next_create_timestamp >= '{start_time}'::timestamp
        AND c.fk_tvid = tvis.fk_tvid   
        AND c.fk_input_source_id = tvis.fk_input_source_id
    JOIN
        prod.detection.tv_settings AS tv_settings
        ON c.session_start < tv_settings.next_create_timestamp
        AND c.session_start >= tv_settings.create_timestamp
        AND c.fk_tvid = tv_settings.fk_tvid
        AND tv_settings.create_timestamp <= '{end_time}'::timestamp
        AND tv_settings.next_create_timestamp >= '{start_time}'::timestamp
    JOIN 
        prod.detection.settings AS settings
        ON tv_settings.fk_settings_id = settings.settings_id
        AND UPPER(settings.country_name) = 'USA'
    LEFT OUTER JOIN
        prod.detection.dma AS dma ON c.fk_dma_id = dma.dma_id
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
        ON c.prev_vizio_epg_station = prev_vizio_station.station_id
    LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
        ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
        AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
        AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS next_vizio_station 
        ON c.next_vizio_epg_station = next_vizio_station.station_id
    LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
        ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
        AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
        AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
"""

# COMMAND ----------

prev_dynamic = f"""
    LEFT OUTER JOIN viewing_content_firehose AS prev_content 
        ON c.fk_tvid = prev_content.fk_tvid
        AND prev_content.session_start = c.prev_session_start
    LEFT OUTER JOIN prod.detection.inscape_station_map AS prev_map
        ON prev_map.inscape_station_id = c.prev_station_id
        AND prev_map.mapped_vendor = '{tivo_tms.upper()}'
    LEFT OUTER JOIN prod.detection.epg_station AS prev_station  
        ON prev_map.mapped_vendor_station_id = prev_station.station_id  
        AND prev_station.vendor_name = prev_map.mapped_vendor 
    LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
        ON c.prev_station_id = prev_station_blacklist.station_id
        AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS prev_station_obfs
        ON c.prev_station_id = prev_station_obfs.station_id
        AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
    LEFT OUTER JOIN epg_schedule_latest AS prev_schedule 
        ON prev_map.mapped_vendor_station_id= prev_schedule.fk_station_id
        AND prev_station.vendor_name = prev_schedule.vendor_name 
        AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) >= prev_schedule.airdate  
        AND timestampadd(SECOND,  prev_content.media_time_start,  prev_content.airdate) < prev_schedule.airdate_end
    LEFT OUTER JOIN prod.detection.epg_show AS prev_program
        ON prev_schedule.fk_show_id = prev_program.show_id
    LEFT OUTER JOIN prod.detection.epg_show AS prev_program_alt
        ON c.prev_show_id = prev_program_alt.show_id
"""

# COMMAND ----------

next_dynamic = f"""
    LEFT OUTER JOIN viewing_content_firehose AS next_content 
        ON c.fk_tvid = next_content.fk_tvid
        AND next_content.session_start = c.next_session_start
        AND next_content.airdate IS NOT NULL
    LEFT OUTER JOIN prod.detection.inscape_station_map AS next_map
        ON next_map.inscape_station_id = c.next_station_id
        AND next_map.mapped_vendor = '{tivo_tms.upper()}' 
    LEFT OUTER JOIN prod.detection.epg_station AS next_station 
        ON next_map.mapped_vendor_station_id = next_station.station_id 
        AND next_station.vendor_name = next_map.mapped_vendor
    LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
        ON c.next_station_id = next_station_blacklist.station_id
        AND next_station_blacklist.vendor_name = next_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS next_station_obfs
        ON c.next_station_id = next_station_obfs.station_id
        AND next_station_obfs.vendor_name = next_map.mapped_vendor
    LEFT OUTER JOIN epg_schedule_latest AS next_schedule 
        ON next_map.mapped_vendor_station_id= next_schedule.fk_station_id 
        AND next_station.vendor_name = next_schedule.vendor_name 
        AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) >= next_schedule.airdate  
        AND timestampadd(SECOND,  next_content.media_time_start,  next_content.airdate) < next_schedule.airdate_end
    LEFT OUTER JOIN prod.detection.epg_show AS next_program
        ON next_schedule.fk_show_id = next_program.show_id
    LEFT OUTER JOIN prod.detection.epg_show AS next_program_alt
        ON c.next_show_id = next_program_alt.show_id
"""

# COMMAND ----------

misc_joins = """
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS prev_filecontent 
        ON c.prev_show_id = prev_filecontent.fk_content_id
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS next_filecontent 
        ON c.next_show_id = next_filecontent.fk_content_id
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m_filter 
        ON m_filter.fk_content_id = prev_content.fk_content_id
    LEFT OUTER JOIN content_ids_firehose as prev_cid on c.prev_show_id = prev_cid.content_id
    LEFT OUTER JOIN content_ids_firehose as next_cid on c.next_show_id = next_cid.content_id
    LEFT OUTER JOIN prod.detection.clients cl2
        ON m_filter.fk_client_id = cl2.client_id
        AND cl2.client_name <> 'kinetiq'
"""

# COMMAND ----------

final_bit = f"""
    LEFT OUTER JOIN prod.detection.tv_inputsource tis    
        ON  c.session_start >=  (tis.create_timestamp::double)::timestamp
        AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
        AND tis.create_timestamp <= ('{end_time}'::timestamp::double)::timestamp
        AND tis.next_create_timestamp >= ('{start_time}'::timestamp::double)::timestamp
        AND c.fk_tvid = tis.fk_tvid 
        AND c.fk_input_source_id = tis.fk_input_source_id
    LEFT OUTER JOIN activity_obfuscation appb 
        ON coalesce(tis.app_name) = appb.app_name 
    LEFT OUTER JOIN viewing_obfuscation AS acrb
        ON coalesce(tis.app_name) = acrb.app_name
    LEFT OUTER JOIN 
        prod.detection.free_channels_distribution_blacklist prev_chanb 
        ON prev_vizio_station.name = prev_chanb.channel_name 
    LEFT OUTER JOIN 
        prod.detection.free_channels_distribution_blacklist next_chanb 
        ON next_vizio_station.name = next_chanb.channel_name
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS prev_nielsen_blacklist
        ON prev_nielsen_blacklist.station_id = prev_map.inscape_station_id 
        AND c.session_start >= prev_nielsen_blacklist.blacklist_start 
        AND c.session_start < prev_nielsen_blacklist.blacklist_end 
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS next_nielsen_blacklist
        ON next_nielsen_blacklist.station_id = next_map.inscape_station_id 
        AND c.session_start >= next_nielsen_blacklist.blacklist_start 
        AND c.session_start < next_nielsen_blacklist.blacklist_end 
    LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_prev
        ON prev_map.inscape_station_id  = rep_local_prev.station_id
        AND c.prev_show_id = rep_local_prev.fk_show_id
        AND c.fk_dma_id = rep_local_prev.dma_id
    LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_prev
        ON prev_map.inscape_station_id  = rep_nyc_nat_prev.station_id
        AND c.prev_show_id = rep_nyc_nat_prev.fk_show_id
    LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_next
        ON next_map.inscape_station_id  = rep_local_next.station_id
        AND c.next_show_id = rep_local_next.fk_show_id
        AND c.fk_dma_id = rep_local_next.dma_id
    LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_next
        ON next_map.inscape_station_id  = rep_nyc_nat_next.station_id
        AND c.next_show_id = rep_nyc_nat_next.fk_show_id
    WHERE
        c.session_start >= '{start_time}'::timestamp
        AND c.session_start < '{end_time}'::timestamp
        AND c.partition_key >= '{start_time.split('T', 1)[0]}'
        AND c.partition_key <= '{end_time.split('T', 1)[0]}'
        AND ABS(MOD(c.fk_tvid, 10)) = 0
        AND CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END;
"""

# COMMAND ----------

def get_tivo_dedup_existing_query(start_time, end_time, client_name, tivo_tms):
    return f"""
    {existing_part_one}
    {part_two}
    {existing_select}
    {initial_from}
    {prev_dynamic}
    {misc_joins}
    {next_dynamic}
    {final_bit}"""

# COMMAND ----------

tivo_tms = 'tivo'
print(get_tivo_dedup_existing_query(start_time, end_time, client_name, tivo_tms))

# COMMAND ----------

tivo_tms = 'tms'
print(get_tivo_dedup_existing_query(start_time, end_time, client_name, tivo_tms))

# COMMAND ----------

new_part_one = f"""
DROP TABLE IF EXISTS dev.mohit_gangwani.{table_name_new}_{tivo_tms};

CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.{table_name_new}_{tivo_tms} (
    tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip_null string, input_category string, input_device string, app_service string, vizio_epg_channel_id string, vizio_epg_program_id string
    );

INSERT INTO dev.mohit_gangwani.{table_name_new}_{tivo_tms} (
    tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip_null, input_category, input_device, app_service, vizio_epg_channel_id, vizio_epg_program_id
)
WITH clients_table_to_join AS (
    SELECT *
    FROM prod.detection.clients 
    WHERE 
        client_name = 'kinetiq'
)
,nielsen_replacement_national_nyc_alias AS (
    SELECT station_id, fk_show_id, tuner_channel_id, tuner_program_id
    FROM prod.detection.nielsen_replacement_national_nyc
    GROUP BY 1,2,3,4
)
,nielsen_replacement_local_alias AS (
    SELECT station_id, fk_show_id, dma_id, tuner_channel_id, tuner_program_id
    FROM prod.detection.nielsen_replacement_local
    GROUP BY 1,2,3,4,5
)
, activity_obfuscation AS (
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
),
epg_program_aggregate AS (
    SELECT DISTINCT *
    FROM prod.detection.vizio_epg_program_aggregate 
),
viewing_content_firehose AS (
    SELECT DISTINCT fk_tvid, session_start, session_end, fk_content_id, is_live
    FROM prod.detection.viewing_content_firehose AS content
    WHERE session_start >= '{start_time}'::timestamp
        AND session_start < '{end_time}'::timestamp
),
content_ids_firehose AS (
    SELECT * FROM detection.content_ids_firehose AS cid
    WHERE content_id IN (
        SELECT DISTINCT c.fk_content_id
        FROM prod.detection.viewing_content_firehose AS c
        WHERE c.session_start >= '{start_time}'::timestamp
            AND c.session_start <= '{end_time}'::timestamp
    )
), station_distribution_blacklist AS (
    WITH agg AS (
        SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
        FROM prod.detection.station_distribution_obfuscation_overwrite
        GROUP BY 1, 2)
    SELECT station_id, vendor_name
    FROM agg
    WHERE cl_list NOT ILIKE '%{client_name}%'
)
"""

# COMMAND ----------

new_select = f"""
    NULLIF(CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL 
 WHEN COALESCE(c.{prev_station}, c.{other_prev_station}) = 0 THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0]) 
 WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
 WHEN prev_program.database_key IS NOT NULL THEN prev_program.database_key 
 WHEN '{tivo_tms.upper()}' = 'TMS' AND prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
 ELSE NULL
 END,''), 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.{prev_station}, c.{other_prev_station}) = 0 THEN prev_filecontent.title
 WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
 ELSE REPLACE(COALESCE(prev_program.title, prev_program_backup.title,
 CASE WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
 ELSE prev_vizio_program.title
 END), ',', '')
 END, 
    c.prev_session_start, 
    c.prev_session_end, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.{prev_station}, c.{other_prev_station}) IS NOT NULL THEN COALESCE(prev_map.inscape_call_sign, prev_map_backup.inscape_call_sign)
 WHEN (COALESCE(c.{prev_station}, c.{other_prev_station}) = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
 ELSE NULL
 END, 
     CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN c.{prev_station} IS NOT NULL THEN
      CASE WHEN (prev_station.inscape_station_name IS NOT NULL) THEN prev_station.inscape_station_name
           WHEN (LOWER(prev_station.station_affil) LIKE '%affiliate%'
                 OR LOWER(prev_station.station_affil) LIKE '%independent%'
                 OR LOWER(prev_station.station_affil) LIKE '%low power%')
                THEN prev_station.station_affil END
 WHEN c.{prev_station} IS NULL AND c.{other_prev_station} IS NOT NULL THEN
      CASE WHEN (prev_station_backup.inscape_station_name IS NOT NULL) THEN prev_station_backup.inscape_station_name
           WHEN (LOWER(prev_station_backup.station_affil) LIKE '%affiliate%'
                 OR LOWER(prev_station_backup.station_affil) LIKE '%independent%'
                 OR LOWER(prev_station_backup.station_affil) LIKE '%low power%')
                THEN prev_station_backup.station_affil END
 WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
 WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
 ELSE NULL
 END, 
    NULLIF(CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.{next_station}, c.{other_next_station}) = 0 THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0]) 
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
 WHEN next_program.database_key IS NOT NULL THEN next_program.database_key
 WHEN '{tivo_tms.upper()}' = 'TMS' AND next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
 ELSE NULL
 END,''), 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.{next_station}, c.{other_next_station}) = 0 THEN next_filecontent.title
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
 ELSE REPLACE(COALESCE(next_program.title, next_program_backup.title,
 CASE WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
 ELSE next_vizio_program.title
 END), ',', '')
 END, 
    c.next_session_start, 
    c.next_session_end, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.{next_station}, c.{other_next_station}) IS NOT NULL THEN COALESCE(next_map.inscape_call_sign, next_map_backup.inscape_call_sign)
 WHEN COALESCE(c.{next_station}, c.{other_next_station}) = 0 THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
 END, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN c.{next_station} IS NOT NULL THEN
      CASE WHEN (next_station.inscape_station_name IS NOT NULL) THEN next_station.inscape_station_name
           WHEN (LOWER(next_station.station_affil) LIKE '%affiliate%'
                 OR LOWER(next_station.station_affil) LIKE '%independent%'
                 OR LOWER(next_station.station_affil) LIKE '%low power%')
                THEN next_station.station_affil END
 WHEN c.{next_station} IS NULL AND c.{other_next_station} IS NOT NULL THEN
      CASE WHEN (next_station_backup.inscape_station_name IS NOT NULL) THEN next_station_backup.inscape_station_name
           WHEN (LOWER(next_station_backup.station_affil) LIKE '%affiliate%'
                 OR LOWER(next_station_backup.station_affil) LIKE '%independent%'
                 OR LOWER(next_station_backup.station_affil) LIKE '%low power%')
                THEN next_station_backup.station_affil END
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
 WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
 ELSE NULL
 END, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED'
 AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
 ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
 END, 
    REPLACE(m.brand_name, ',', ''), 
    REPLACE(m.title, ',', ''), 
    m.duration, 
    NULL AS ip, 
    tvis.category, 
    tvis.input_device, 
    CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
            CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
                WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
                WHEN LOWER(coalesce(tis.app_name)) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games')
                 AND COALESCE(c.{prev_show}, c.{other_prev_show}) IS NOT NULL THEN NULL
                WHEN LOWER(coalesce(tis.app_name)) = 'unknown' THEN NULL
                ELSE coalesce(tis.app_name) END
            WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
        END, 
    prev_vizio_station.station_id, 
    prev_vizio_program.program_aggregate_id
"""

# COMMAND ----------

new_table_with_schema = f'dev.detection.viewing_commercials_firehose_dedup'
initial_from = f"""
    FROM {new_table_with_schema} AS c
    JOIN prod.detection.zoo AS z 
        ON c.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    INNER JOIN
        prod.detection.tv AS tv
        ON c.fk_tvid = tv.tvid
        AND tv.oem = 'VIZIO'
    JOIN
        prod.detection.tv_populations AS tp
        ON c.fk_tvid = tp.fk_tvid 
    JOIN
        prod.detection.populations AS pop
        ON tp.fk_population_id = pop.population_id 
        AND LOWER(pop.population_name) = 'opted_in'
    JOIN
        prod.detection.commercial_id_external_firehose AS m
        ON c.fk_commercial_id = m.fk_commercial_id
    JOIN
        clients_table_to_join cl
        ON m.fk_client_id = cl.client_id
    JOIN
        prod.detection.location AS location
        ON c.fk_location_id = location.location_id
        AND UPPER(location.country_code) = 'US'
    JOIN 
        prod.detection.tv_input_stats_firehose  tvis    
        ON c.session_start >= tvis.create_timestamp 
        AND c.session_start < tvis.next_create_timestamp
        AND tvis.create_timestamp <= '{end_time}'::timestamp
        AND tvis.next_create_timestamp >= '{start_time}'::timestamp
        AND c.fk_tvid = tvis.fk_tvid   
        AND c.fk_input_source_id = tvis.fk_input_source_id
    JOIN
        prod.detection.tv_settings AS tv_settings
        ON c.session_start < tv_settings.next_create_timestamp
        AND c.session_start >= tv_settings.create_timestamp
        AND c.fk_tvid = tv_settings.fk_tvid
        AND tv_settings.create_timestamp <= '{end_time}'::timestamp
        AND tv_settings.next_create_timestamp >= '{start_time}'::timestamp
    JOIN 
        prod.detection.settings AS settings
        ON tv_settings.fk_settings_id = settings.settings_id
        AND UPPER(settings.country_name) = 'USA'
    LEFT OUTER JOIN
        prod.detection.dma AS dma ON c.fk_dma_id = dma.dma_id
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
        ON c.prev_vizio_epg_station = prev_vizio_station.station_id
    LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
        ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
        AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
        AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS next_vizio_station 
        ON c.next_vizio_epg_station = next_vizio_station.station_id
    LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
        ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
        AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
        AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
    LEFT OUTER JOIN viewing_content_firehose AS prev_content 
        ON c.fk_tvid = prev_content.fk_tvid
        AND prev_content.session_start = c.prev_session_start
"""

# COMMAND ----------

new_prev = f"""
    LEFT OUTER JOIN prod.detection.epg_station AS prev_station  
        ON prev_station.station_id = c.{prev_station}
        AND prev_station.vendor_name = '{tivo_tms.upper()}'
    LEFT OUTER JOIN prod.detection.epg_station AS prev_station_backup
        ON prev_station_backup.station_id = c.{other_prev_station}
        AND c.{prev_station} IS NULL
        AND prev_station_backup.vendor_name = '{tms_tivo}'
    LEFT OUTER JOIN inscape_map_deduped AS prev_map
        ON prev_map.mapped_vendor_station_id = c.{prev_station}
        AND prev_map.mapped_vendor = '{tivo_tms.upper()}'
    LEFT OUTER JOIN inscape_map_deduped AS prev_map_backup
        ON prev_map_backup.mapped_vendor_station_id = c.{other_prev_station}
        AND c.{prev_station} IS NULL
        AND prev_map_backup.mapped_vendor = '{tms_tivo}' 
    LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
        ON c.{prev_station} = prev_station_blacklist.station_id
        AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS prev_station_obfs
        ON c.{prev_station} = prev_station_obfs.vendor_station_id
        AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.epg_show AS prev_program
        ON c.{prev_show} = prev_program.show_id
       AND prev_program.vendor_name = '{tivo_tms.upper()}'
    LEFT OUTER JOIN prod.detection.epg_show AS prev_program_backup
        ON c.{other_prev_show} = prev_program_backup.show_id
       AND prev_program_backup.vendor_name = '{tms_tivo}'
        AND c.{prev_show} IS NULL
"""

# COMMAND ----------

new_next = f"""
    LEFT OUTER JOIN prod.detection.epg_station AS next_station  
        ON next_station.station_id = c.{next_station}
        AND next_station.vendor_name = '{tivo_tms.upper()}'
    LEFT OUTER JOIN prod.detection.epg_station AS next_station_backup
        ON next_station_backup.station_id = c.{other_next_station}
        AND c.{next_station} IS NULL
        AND next_station_backup.vendor_name = '{tms_tivo}'
    LEFT OUTER JOIN inscape_map_deduped AS next_map
        ON next_map.mapped_vendor_station_id = c.{next_station}
        AND next_map.mapped_vendor = '{tivo_tms.upper()}'
    LEFT OUTER JOIN inscape_map_deduped AS next_map_backup
        ON next_map_backup.mapped_vendor_station_id = c.{other_next_station}
        AND c.{next_station} IS NULL
        AND next_map_backup.mapped_vendor = '{tms_tivo}' 
    LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
        ON c.{next_station} = next_station_blacklist.station_id
        AND next_station_blacklist.vendor_name = next_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS next_station_obfs
        ON c.{next_station} = next_station_obfs.vendor_station_id
        AND next_station_obfs.vendor_name = next_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.epg_show AS next_program
        ON c.{next_show} = next_program.show_id
       AND next_program.vendor_name = '{tivo_tms.upper()}'
    LEFT OUTER JOIN prod.detection.epg_show AS next_program_backup
        ON c.{other_next_show} = next_program_backup.show_id
       AND next_program_backup.vendor_name = '{tms_tivo}'
        AND c.{next_show} IS NULL
"""

# COMMAND ----------

tivo_tms = 'tivo'
tms_tivo = 'TMS' if tivo_tms == 'tivo' else 'TIVO'
prev_station = 'prev_station_id' if tivo_tms == 'tivo' else 'prev_tms_station_id'
other_prev_station = 'prev_tms_station_id' if tivo_tms == 'tivo' else 'prev_station_id'
next_station = 'next_station_id' if tivo_tms == 'tivo' else 'next_tms_station_id'
other_next_station = 'next_tms_station_id' if tivo_tms == 'tivo' else 'next_station_id'
prev_show = 'prev_show_id' if tivo_tms == 'tivo' else 'prev_tms_show_id'
other_prev_show = 'prev_tms_show_id' if tivo_tms == 'tivo' else 'prev_show_id'
next_show = 'next_show_id' if tivo_tms == 'tivo' else 'next_tms_show_id'
other_next_show = 'next_tms_show_id' if tivo_tms == 'tivo' else 'next_show_id'

def get_dedup_new_query(start_time, end_time, client_name, tivo_tms, prev_station,
                        other_prev_station, next_station, other_next_station,
                        prev_show, other_prev_show, next_show, other_next_show):
    return f"""
    {new_part_one}
    {part_two}
    {new_select}
    {initial_from}
    {new_prev}
    {misc_joins}
    {new_next}
    {final_bit}"""

# COMMAND ----------

print(
    get_dedup_new_query(
        start_time,
        end_time,
        client_name,
        tivo_tms,
        prev_station,
        other_prev_station,
        next_station,
        other_next_station,
        prev_show,
        other_prev_show,
        next_show,
        other_next_show,
    )
)

# COMMAND ----------

tivo_tms = 'tms'
tms_tivo = 'TMS' if tivo_tms == 'tivo' else 'TIVO'
prev_station = 'prev_station_id' if tivo_tms == 'tivo' else 'prev_tms_station_id'
other_prev_station = 'prev_tms_station_id' if tivo_tms == 'tivo' else 'prev_station_id'
next_station = 'next_station_id' if tivo_tms == 'tivo' else 'next_tms_station_id'
other_next_station = 'next_tms_station_id' if tivo_tms == 'tivo' else 'next_station_id'
prev_show = 'prev_show_id' if tivo_tms == 'tivo' else 'prev_tms_show_id'
other_prev_show = 'prev_tms_show_id' if tivo_tms == 'tivo' else 'prev_show_id'
next_show = 'next_show_id' if tivo_tms == 'tivo' else 'next_tms_show_id'
other_next_show = 'next_tms_show_id' if tivo_tms == 'tivo' else 'next_show_id'

print(
    get_dedup_new_query(
        start_time,
        end_time,
        client_name,
        tivo_tms,
        prev_station,
        other_prev_station,
        next_station,
        other_next_station,
        prev_show,
        other_prev_show,
        next_show,
        other_next_show,
    )
)

# COMMAND ----------

tms_dedup_new_q = f"""
DROP TABLE IF EXISTS dev.mohit_gangwani.{table_name_new}_tms;

CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.{table_name_new}_tms (
    tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip_null string, input_category string, input_device string, app_service string, vizio_epg_channel_id string, vizio_epg_program_id string
    );

INSERT INTO dev.mohit_gangwani.{table_name_new}_tms (
    tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip_null, input_category, input_device, app_service, vizio_epg_channel_id, vizio_epg_program_id
)
WITH clients_table_to_join AS (
    SELECT *
    FROM prod.detection.clients 
    WHERE 
        client_name = 'kinetiq'
)
,nielsen_replacement_national_nyc_alias AS (
    SELECT station_id, fk_show_id, tuner_channel_id, tuner_program_id
    FROM prod.detection.nielsen_replacement_national_nyc
    GROUP BY 1,2,3,4
)
,nielsen_replacement_local_alias AS (
    SELECT station_id, fk_show_id, dma_id, tuner_channel_id, tuner_program_id
    FROM prod.detection.nielsen_replacement_local
    GROUP BY 1,2,3,4,5
)
, activity_obfuscation AS (
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
),
epg_program_aggregate AS (
    SELECT DISTINCT *
    FROM prod.detection.vizio_epg_program_aggregate 
),
viewing_content_firehose AS (
    SELECT DISTINCT fk_tvid, fk_station_id, media_time_start, session_start, session_end, airdate, fk_content_id, is_live
    FROM prod.detection.viewing_content_firehose AS content
    WHERE session_start >= '{start_time}'::timestamp
        AND session_start < '{end_time}'::timestamp
),
content_ids_firehose AS (
    SELECT * FROM detection.content_ids_firehose AS cid
    WHERE content_id IN (
        SELECT DISTINCT c.fk_content_id
        FROM prod.detection.viewing_content_firehose AS c
        WHERE c.session_start >= '{start_time}'::timestamp
            AND c.session_start <= '{end_time}'::timestamp
    )
), station_distribution_blacklist AS (
    WITH agg AS (
        SELECT station_id, vendor_name, CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(client_name), false)) AS cl_list
        FROM prod.detection.station_distribution_obfuscation_overwrite
        GROUP BY 1, 2)
    SELECT station_id, vendor_name
    FROM agg
    WHERE cl_list NOT ILIKE '%{client_name}%'
)
, inscape_map_deduped AS (
    SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
    FROM (
        SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
        FROM detection.inscape_station_map) ism
    WHERE ism.rn = 1
)
SELECT 
/*+ BROADCAST(m), 
  BROADCAST(cl), 
  BROADCAST(tp), 
  BROADCAST(pop),
  BROADCAST(location), 
  BROADCAST(epg_program_aggregate), 
  BROADCAST(prev_cid), 
  BROADCAST(next_cid), 
  BROADCAST(next_map),
  BROADCAST(next_vizio_station),
  BROADCAST(next_vizio_program),
  BROADCAST(prev_vizio_station),
  BROADCAST(prev_vizio_program),
  BROADCAST(next_schedule),
  BROADCAST(next_program),
  BROADCAST(next_station),
  BROADCAST(next_schedule),
  BROADCAST(next_program),
  BROADCAST(next_program_alt),
  BROADCAST(prev_program_alt),
  BROADCAST(prev_map),
  BROADCAST(prev_station),
  BROADCAST(prev_schedule),
  BROADCAST(prev_program),
  RANGE_JOIN(next_schedule, 604800),
  RANGE_JOIN(prev_schedule, 604800) */
DISTINCT 
    COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
    '', 
    NULLIF(location.zipcode, ''), 
    REPLACE(dma.dma_name, ',', ''), 
    m.external_id, 
    c.media_time_start, 
    c.session_start, 
    c.session_end, 
    NULLIF(CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL 
 WHEN COALESCE(c.tms_prev_station_id, c.prev_station_id) = 0 THEN coalesce(prev_filecontent.external_id,SPLIT(prev_cid.content_cid, '_')[0]) 
 WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
 WHEN prev_program.database_key IS NOT NULL THEN prev_program.database_key 
 WHEN 'TMS' = 'TMS' AND prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
 ELSE NULL
 END,''), 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.tms_prev_station_id, c.prev_station_id) = 0 THEN prev_filecontent.title
 WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN NULL
 ELSE REPLACE(COALESCE(prev_program.title, prev_program_backup.title,
 CASE WHEN prev_vizio_program.series_aggregate_title IS NOT NULL AND prev_vizio_program.series_aggregate_title != '' THEN prev_vizio_program.series_aggregate_title
 ELSE prev_vizio_program.title
 END), ',', '')
 END, 
    c.prev_session_start, 
    c.prev_session_end, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.tms_prev_station_id, c.prev_station_id) IS NOT NULL THEN COALESCE(prev_map.inscape_call_sign, prev_map_backup.inscape_call_sign)
 WHEN (prev_station_id = 0) THEN COALESCE(SPLIT(prev_cid.content_cid, '_')[1],'FILE') 
 ELSE NULL
 END, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN c.tms_prev_station_id IS NOT NULL THEN
      CASE WHEN (prev_station.inscape_station_name IS NOT NULL) THEN prev_station.inscape_station_name
           WHEN (LOWER(prev_station.station_affil) LIKE '%affiliate%'
                 OR LOWER(prev_station.station_affil) LIKE '%independent%'
                 OR LOWER(prev_station.station_affil) LIKE '%low power%')
                THEN prev_station.station_affil END
 WHEN c.tms_prev_station_id IS NULL AND c.prev_station_id IS NOT NULL THEN
      CASE WHEN (prev_station_backup.inscape_station_name IS NOT NULL) THEN prev_station_backup.inscape_station_name
           WHEN (LOWER(prev_station_backup.station_affil) LIKE '%affiliate%'
                 OR LOWER(prev_station_backup.station_affil) LIKE '%independent%'
                 OR LOWER(prev_station_backup.station_affil) LIKE '%low power%')
                THEN prev_station_backup.station_affil END
 WHEN prev_chanb.channel_name IS NOT NULL OR c.prev_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
 WHEN c.prev_vizio_epg_station IS NOT NULL THEN prev_vizio_station.name
 ELSE NULL
 END, 
    NULLIF(CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.tms_next_station_id, c.next_station_id) = 0 THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0]) 
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
 WHEN next_program.database_key IS NOT NULL THEN next_program.database_key
 WHEN 'TMS' = 'TMS' AND next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
 ELSE NULL
 END,''), 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.tms_next_station_id, c.next_station_id) = 0 THEN next_filecontent.title
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
 ELSE REPLACE(COALESCE(next_program.title, next_program_backup.title,
 CASE WHEN next_vizio_program.series_aggregate_title IS NOT NULL AND next_vizio_program.series_aggregate_title != '' THEN next_vizio_program.series_aggregate_title
 ELSE next_vizio_program.title
 END), ',', '')
 END, 
    c.next_session_start, 
    c.next_session_end, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.tms_next_station_id, c.next_station_id) IS NOT NULL THEN COALESCE(next_map.inscape_call_sign, next_map_backup.inscape_call_sign)
 WHEN COALESCE(c.tms_next_station_id, c.next_station_id) = 0 THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
 END, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
 WHEN c.tms_next_station_id IS NOT NULL THEN
      CASE WHEN (next_station.inscape_station_name IS NOT NULL) THEN next_station.inscape_station_name
           WHEN (LOWER(next_station.station_affil) LIKE '%affiliate%'
                 OR LOWER(next_station.station_affil) LIKE '%independent%'
                 OR LOWER(next_station.station_affil) LIKE '%low power%')
                THEN next_station.station_affil END
 WHEN c.tms_next_station_id IS NULL AND c.next_station_id IS NOT NULL THEN
      CASE WHEN (next_station_backup.inscape_station_name IS NOT NULL) THEN next_station_backup.inscape_station_name
           WHEN (LOWER(next_station_backup.station_affil) LIKE '%affiliate%'
                 OR LOWER(next_station_backup.station_affil) LIKE '%independent%'
                 OR LOWER(next_station_backup.station_affil) LIKE '%low power%')
                THEN next_station_backup.station_affil END
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN 'OBFUSCATED'
 WHEN c.next_vizio_epg_station IS NOT NULL THEN next_vizio_station.name
 ELSE NULL
 END, 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED'
 AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
 WHEN prev_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_prev.station_id, rep_nyc_nat_prev.station_id) IS NULL OR prev_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN prev_station_blacklist.station_id IS NOT NULL THEN NULL
 ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
 END, 
    REPLACE(m.brand_name, ',', ''), 
    REPLACE(m.title, ',', ''), 
    m.duration, 
    NULL AS ip, 
    tvis.category, 
    tvis.input_device, 
    CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
            CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
                WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
                WHEN LOWER(coalesce(tis.app_name)) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND prev_show_id IS NOT NULL THEN NULL
                WHEN LOWER(coalesce(tis.app_name)) = 'unknown' THEN NULL
                ELSE coalesce(tis.app_name) END
            WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
        END, 
    prev_vizio_station.station_id, 
    prev_vizio_program.program_aggregate_id
    FROM dev.detection.viewing_commercials_firehose_dedup AS c
    JOIN prod.detection.zoo AS z 
        ON c.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    INNER JOIN
        prod.detection.tv AS tv
        ON c.fk_tvid = tv.tvid
        AND tv.oem = 'VIZIO'
    JOIN
        prod.detection.tv_populations AS tp
        ON c.fk_tvid = tp.fk_tvid 
    JOIN
        prod.detection.populations AS pop
        ON tp.fk_population_id = pop.population_id 
        AND LOWER(pop.population_name) = 'opted_in'
    JOIN
        prod.detection.commercial_id_external_firehose AS m
        ON c.fk_commercial_id = m.fk_commercial_id
    JOIN
        clients_table_to_join cl
        ON m.fk_client_id = cl.client_id
    JOIN
        prod.detection.location AS location
        ON c.fk_location_id = location.location_id
        AND UPPER(location.country_code) = 'US'
    JOIN 
        prod.detection.tv_input_stats_firehose  tvis    
        ON c.session_start >= tvis.create_timestamp 
        AND c.session_start < tvis.next_create_timestamp
        AND tvis.create_timestamp <= '{end_time}'::timestamp
        AND tvis.next_create_timestamp >= '{start_time}'::timestamp
        AND c.fk_tvid = tvis.fk_tvid   
        AND c.fk_input_source_id = tvis.fk_input_source_id
    JOIN
        prod.detection.tv_settings AS tv_settings
        ON c.session_start < tv_settings.next_create_timestamp
        AND c.session_start >= tv_settings.create_timestamp
        AND c.fk_tvid = tv_settings.fk_tvid
        AND tv_settings.create_timestamp <= '{end_time}'::timestamp
        AND tv_settings.next_create_timestamp >= '{start_time}'::timestamp
    JOIN 
        prod.detection.settings AS settings
        ON tv_settings.fk_settings_id = settings.settings_id
        AND UPPER(settings.country_name) = 'USA'
    LEFT OUTER JOIN
        prod.detection.dma AS dma ON c.fk_dma_id = dma.dma_id
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
        ON c.prev_vizio_epg_station = prev_vizio_station.station_id
    LEFT OUTER JOIN epg_program_aggregate AS prev_vizio_program 
        ON CAST(c.prev_vizio_epg_program AS BIGINT) = prev_vizio_program.program_aggregate_id
        AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
        AND CAST(c.prev_vizio_epg_program AS BIGINT) IS NOT NULL
    LEFT OUTER JOIN prod.detection.vizio_epg_station AS next_vizio_station 
        ON c.next_vizio_epg_station = next_vizio_station.station_id
    LEFT OUTER JOIN epg_program_aggregate AS next_vizio_program 
        ON CAST(c.next_vizio_epg_program AS BIGINT) = next_vizio_program.program_aggregate_id
        AND CAST(c.prev_vizio_epg_program AS BIGINT) > 0
        AND CAST(c.next_vizio_epg_program AS BIGINT) IS NOT NULL
    LEFT OUTER JOIN viewing_content_firehose AS prev_content 
        ON c.fk_tvid = prev_content.fk_tvid
        AND prev_content.session_start = c.prev_session_start

    LEFT OUTER JOIN prod.detection.epg_station AS prev_station  
        ON prev_station.station_id = c.prev_station_id
        AND prev_station.vendor_name = 'TIVO'
    LEFT OUTER JOIN prod.detection.epg_station AS prev_station_backup
        ON prev_station_backup.station_id = c.tms_prev_station_id
        AND c.prev_station_id IS NULL
        AND prev_station_backup.vendor_name = 'TMS'
    LEFT OUTER JOIN inscape_map_deduped AS prev_map
        ON prev_map.mapped_vendor_station_id = c.prev_station_id
        AND prev_map.mapped_vendor = 'TIVO'
    LEFT OUTER JOIN inscape_map_deduped AS prev_map_backup
        ON prev_map_backup.mapped_vendor_station_id = c.tms_prev_station_id
        AND c.prev_station_id IS NULL
        AND prev_map_backup.mapped_vendor = 'TMS' 
    LEFT OUTER JOIN station_distribution_blacklist AS prev_station_blacklist
        ON c.prev_station_id = prev_station_blacklist.station_id
        AND prev_station_blacklist.vendor_name = prev_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS prev_station_obfs
        ON c.prev_station_id = prev_station_obfs.vendor_station_id
        AND prev_station_obfs.vendor_name = prev_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.epg_show AS prev_program
        ON c.prev_show_id = prev_program.show_id
       AND prev_program.vendor_name = 'TIVO'
    LEFT OUTER JOIN prod.detection.epg_show AS prev_program_backup
        ON c.tms_prev_show_id = prev_program_backup.show_id
       AND prev_program_backup.vendor_name = 'TMS'
        AND c.prev_show_id IS NULL

    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS prev_filecontent 
        ON c.prev_show_id = prev_filecontent.fk_content_id
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS next_filecontent 
        ON c.next_show_id = next_filecontent.fk_content_id
    LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m_filter 
        ON m_filter.fk_content_id = prev_content.fk_content_id
    LEFT OUTER JOIN content_ids_firehose as prev_cid on c.prev_show_id = prev_cid.content_id
    LEFT OUTER JOIN content_ids_firehose as next_cid on c.next_show_id = next_cid.content_id
    LEFT OUTER JOIN prod.detection.clients cl2
        ON m_filter.fk_client_id = cl2.client_id
        AND cl2.client_name <> 'kinetiq'

    LEFT OUTER JOIN prod.detection.epg_station AS next_station  
        ON next_station.station_id = c.next_station_id
        AND next_station.vendor_name = 'TIVO'
    LEFT OUTER JOIN prod.detection.epg_station AS next_station_backup
        ON next_station_backup.station_id = c.tms_next_station_id
        AND c.next_station_id IS NULL
        AND next_station_backup.vendor_name = 'TMS'
    LEFT OUTER JOIN inscape_map_deduped AS next_map
        ON next_map.mapped_vendor_station_id = c.next_station_id
        AND next_map.mapped_vendor = 'TIVO'
    LEFT OUTER JOIN inscape_map_deduped AS next_map_backup
        ON next_map_backup.mapped_vendor_station_id = c.tms_next_station_id
        AND c.next_station_id IS NULL
        AND next_map_backup.mapped_vendor = 'TMS' 
    LEFT OUTER JOIN station_distribution_blacklist AS next_station_blacklist
        ON c.next_station_id = next_station_blacklist.station_id
        AND next_station_blacklist.vendor_name = next_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS next_station_obfs
        ON c.next_station_id = next_station_obfs.vendor_station_id
        AND next_station_obfs.vendor_name = next_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.epg_show AS next_program
        ON c.next_show_id = next_program.show_id
       AND next_program.vendor_name = 'TIVO'
    LEFT OUTER JOIN prod.detection.epg_show AS next_program_backup
        ON c.tms_next_show_id = next_program_backup.show_id
       AND next_program_backup.vendor_name = 'TMS'
        AND c.next_show_id IS NULL

    LEFT OUTER JOIN prod.detection.tv_inputsource tis    
        ON  c.session_start >=  (tis.create_timestamp::double)::timestamp
        AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
        AND tis.create_timestamp <= ('{end_time}'::timestamp::double)::timestamp
        AND tis.next_create_timestamp >= ('{start_time}'::timestamp::double)::timestamp
        AND c.fk_tvid = tis.fk_tvid 
        AND c.fk_input_source_id = tis.fk_input_source_id
    LEFT OUTER JOIN activity_obfuscation appb 
        ON coalesce(tis.app_name) = appb.app_name 
    LEFT OUTER JOIN viewing_obfuscation AS acrb
        ON coalesce(tis.app_name) = acrb.app_name
    LEFT OUTER JOIN 
        prod.detection.free_channels_distribution_blacklist prev_chanb 
        ON prev_vizio_station.name = prev_chanb.channel_name 
    LEFT OUTER JOIN 
        prod.detection.free_channels_distribution_blacklist next_chanb 
        ON next_vizio_station.name = next_chanb.channel_name
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS prev_nielsen_blacklist
        ON prev_nielsen_blacklist.station_id = prev_map.inscape_station_id 
        AND c.session_start >= prev_nielsen_blacklist.blacklist_start 
        AND c.session_start < prev_nielsen_blacklist.blacklist_end 
    LEFT OUTER JOIN prod.detection.nielsen_only_distribution_blacklist AS next_nielsen_blacklist
        ON next_nielsen_blacklist.station_id = next_map.inscape_station_id 
        AND c.session_start >= next_nielsen_blacklist.blacklist_start 
        AND c.session_start < next_nielsen_blacklist.blacklist_end 
    LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_prev
        ON prev_map.inscape_station_id  = rep_local_prev.station_id
        AND c.prev_show_id = rep_local_prev.fk_show_id
        AND c.fk_dma_id = rep_local_prev.dma_id
    LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_prev
        ON prev_map.inscape_station_id  = rep_nyc_nat_prev.station_id
        AND c.prev_show_id = rep_nyc_nat_prev.fk_show_id
    LEFT OUTER JOIN nielsen_replacement_local_alias AS rep_local_next
        ON next_map.inscape_station_id  = rep_local_next.station_id
        AND c.next_show_id = rep_local_next.fk_show_id
        AND c.fk_dma_id = rep_local_next.dma_id
    LEFT OUTER JOIN nielsen_replacement_national_nyc_alias AS rep_nyc_nat_next
        ON next_map.inscape_station_id  = rep_nyc_nat_next.station_id
        AND c.next_show_id = rep_nyc_nat_next.fk_show_id
    WHERE
        c.session_start >= '{start_time}'::timestamp
        AND c.session_start < '{end_time}'::timestamp
        AND c.partition_key >= '{start_time.split('T', 1)[0]}'
        AND c.partition_key <= '{end_time.split('T', 1)[0]}'
        AND ABS(MOD(c.fk_tvid, 10)) = 0
        AND CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END
"""

# COMMAND ----------

print(tivo_dedup_existing_q)

# COMMAND ----------

print(tivo_dedup_new_q)

# COMMAND ----------

print(tms_dedup_existing_q)

# COMMAND ----------

print(tms_dedup_new_q)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vendor_name, COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC JOIN prod.detection.epg_show
# MAGIC   ON fk_show_id = epg_show.show_id
# MAGIC WHERE session_start >= '2024-05-01'
# MAGIC   AND session_start < '2024-07-01'
# MAGIC   AND partition_key >= '2024-05-01'
# MAGIC   AND partition_key < '2024-05-01'
# MAGIC GROUP BY 1
