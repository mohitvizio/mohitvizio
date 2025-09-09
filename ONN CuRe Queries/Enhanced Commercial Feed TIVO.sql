DROP TABLE IF EXISTS prod.customer_reports.enhancedcommercialfeed_production;
CREATE TABLE IF NOT EXISTS prod.customer_reports.enhancedcommercialfeed_production (
    tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service string, vizio_epg_channel_id string, vizio_epg_program_id string
    );
INSERT INTO prod.customer_reports.enhancedcommercialfeed_production (
    tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip, input_category, input_device, app_service, vizio_epg_channel_id, vizio_epg_program_id
)
WITH commercial_id_external_firehose AS (
    SELECT m.fk_commercial_id, m.external_id, m.brand_name, m.title, m.duration
    FROM prod.detection.commercial_id_external_firehose AS m
    JOIN prod.detection.clients cl ON m.fk_client_id = cl.client_id
    WHERE 
        CASE WHEN '{commercial_client}' = 'nielsen' THEN cl.client_name IN ('kinetiq', 'nielsen')
        ELSE cl.client_name = '{commercial_client}' END
    GROUP BY ALL
)
, nielsen_replacement_national_nyc_alias AS ( 
    SELECT rl.station_id, rl.fk_show_id, rl.tuner_channel_id, rl.tuner_program_id
    FROM prod.detection.nielsen_replacement_national_nyc AS rl
    JOIN prod.detection.nielsen_only_distribution_blacklist AS bl ON bl.station_id = rl.station_id
   AND bl.blacklist_end >= '{start_time}'::timestamp
   GROUP BY 1,2,3,4)
,nielsen_replacement_local_alias AS
  ( 
    SELECT rl.station_id, rl.fk_show_id, rl.dma_id, rl.tuner_channel_id, rl.tuner_program_id
    FROM prod.detection.nielsen_replacement_local AS rl
    JOIN prod.detection.nielsen_only_distribution_blacklist AS bl ON bl.station_id = rl.station_id
    AND bl.blacklist_end >= '{start_time}'::timestamp
    GROUP BY 1,2,3,4,5)
, activity_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_activity_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{commercial_client}'
    WHERE override.app_name IS NULL
)
, viewing_obfuscation AS (
    SELECT blocked_apps.app_name
    FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
    LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
        ON blocked_apps.app_name = override.app_name
        AND override.client_name = '{commercial_client}'
    WHERE override.app_name IS NULL
),
epg_program_aggregate AS (
    SELECT DISTINCT *
    FROM prod.detection.vizio_epg_program_aggregate 
),
viewing_content_firehose AS (
    SELECT DISTINCT v.fk_tvid, v.session_start, v.session_end, v.fk_content_id, v.is_live
    FROM prod.detection.viewing_content_firehose AS v
    WHERE 'VIZIO' IN ('VIZIO')           -- If only VIZIO
    -- WHERE 'VIZIO' IN ('ONN')          -- If only ONN
    -- WHERE 'VIZIO' IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
      AND v.session_start >= '{start_time}'::timestamp
      AND v.session_start < '{end_time}'::timestamp
      AND v.partition_key >= '{start_time}'::timestamp::DATE
      AND v.partition_key <= '{end_time}'::timestamp::DATE
    UNION
    SELECT DISTINCT o.fk_tvid, o.session_start, o.session_end, o.fk_content_id, o.is_live
    FROM prod.detection_onn.viewing_content_firehose AS o
    WHERE 'ONN' IN ('VIZIO')           -- If only VIZIO
    -- WHERE 'ONN' IN ('ONN')          -- If only ONN
    -- WHERE 'ONN' IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
      AND o.session_start >= '{start_time}'::timestamp
      AND o.session_start < '{end_time}'::timestamp
      AND o.partition_key >= '{start_time}'::timestamp::DATE
      AND o.partition_key <= '{end_time}'::timestamp::DATE
),
content_ids_firehose AS
    (SELECT * FROM detection.content_ids_firehose AS cid
    WHERE content_id IN ( 
        SELECT DISTINCT fk_content_id FROM viewing_content_firehose )
),
station_distribution_blacklist AS ( 
    WITH agg AS ( 
        SELECT vendor_station_id, vendor_name, ARRAY_JOIN(SORT_ARRAY(COLLECT_LIST(client_name), FALSE), ',' ) AS cl_list
        FROM prod.detection.station_distribution_obfuscation_overwrite
        GROUP BY 1,2 )
    SELECT vendor_station_id AS station_id, vendor_name
    FROM agg
    WHERE cl_list NOT ILIKE '%ispot%'
),
inscape_map_deduped AS (
    SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id
    FROM (
        SELECT inscape_station_id, inscape_call_sign, mapped_vendor, mapped_vendor_station_id, ROW_NUMBER() OVER (PARTITION BY mapped_vendor, mapped_vendor_station_id ORDER BY created_at DESC) AS rn
        FROM prod.detection.inscape_station_map) ism
    WHERE ism.rn = 1
)
SELECT 
/*+ BROADCAST(m),
  BROADCAST(cl2),
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
  BROADCAST(next_program),
  BROADCAST(next_station),
  BROADCAST(next_program),
  BROADCAST(next_program_alt),
  BROADCAST(prev_program_alt),
  BROADCAST(prev_map),
  BROADCAST(prev_station),
  BROADCAST(prev_program),
  BROADCAST(next_map_backup),
  BROADCAST(next_station_blacklist),
  BROADCAST(next_station_obfs),
  BROADCAST(next_program_backup),
  BROADCAST(prev_chanb),
  BROADCAST(next_chanb),
  BROADCAST(prev_nielsen_blacklist),
  BROADCAST(next_nielsen_blacklist) */
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
 WHEN 'TIVO' = 'TIVO' AND prev_vizio_program.program_tms_id IS NOT NULL THEN prev_vizio_program.program_tms_id
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
 WHEN c.prev_station_id IS NOT NULL THEN
 CASE WHEN (prev_station.inscape_station_name IS NOT NULL) THEN prev_station.inscape_station_name
 WHEN (LOWER(prev_station.station_affil) LIKE '%affiliate%'
 OR LOWER(prev_station.station_affil) LIKE '%independent%'
 OR LOWER(prev_station.station_affil) LIKE '%low power%')
 THEN prev_station.station_affil END
 WHEN c.prev_station_id IS NULL AND c.tms_prev_station_id IS NOT NULL THEN
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
 WHEN COALESCE(c.next_station_id, c.tms_next_station_id) = 0 THEN coalesce(next_filecontent.external_id,SPLIT(next_cid.content_cid, '_')[0]) 
 WHEN next_chanb.channel_name IS NOT NULL OR c.next_vizio_epg_station = 98989898989898 THEN NULL
 WHEN next_program.database_key IS NOT NULL THEN next_program.database_key
 WHEN 'TIVO' = 'TIVO' AND next_vizio_program.program_tms_id IS NOT NULL THEN next_vizio_program.program_tms_id
 ELSE NULL
 END,''), 
    CASE
 WHEN (cl2.client_id is not NULL) THEN NULL
 WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
 WHEN next_station_blacklist.station_id IS NOT NULL THEN NULL
 WHEN COALESCE(c.next_station_id, c.tms_next_station_id) = 0 THEN next_filecontent.title
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
 WHEN COALESCE(c.next_station_id, c.tms_next_station_id) IS NOT NULL THEN COALESCE(next_map.inscape_call_sign, next_map_backup.inscape_call_sign)
 WHEN COALESCE(c.next_station_id, c.tms_next_station_id) = 0 THEN COALESCE(SPLIT(next_cid.content_cid, '_')[1],'FILE')
 END, 
    CASE
    WHEN (cl2.client_id is not NULL) THEN NULL
    WHEN next_nielsen_blacklist.station_id IS NOT NULL AND (NVL(rep_local_next.station_id, rep_nyc_nat_next.station_id) IS NULL OR next_nielsen_blacklist.ingest_time IS NOT NULL) THEN NULL
    WHEN next_station_obfs.station_id IS NOT NULL THEN NULL
    WHEN c.next_station_id IS NOT NULL THEN
         CASE WHEN (next_station.inscape_station_name IS NOT NULL) THEN next_station.inscape_station_name
              WHEN (LOWER(next_station.station_affil) LIKE '%affiliate%'
                    OR LOWER(next_station.station_affil) LIKE '%independent%'
                    OR LOWER(next_station.station_affil) LIKE '%low power%')
                   THEN next_station.station_affil END
    WHEN c.next_station_id IS NULL AND c.tms_next_station_id IS NOT NULL THEN
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
    ip.ip_address, 
    tvis.category, 
    tvis.input_device, 
    CASE WHEN UPPER(tvis.category) = 'APPS' THEN 
 CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
 WHEN c.prev_vizio_epg_station IS NULL AND tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
 WHEN appb.app_name IS NOT NULL THEN 'OBFUSCATED'
 WHEN LOWER(coalesce(tis.app_name)) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND COALESCE(c.prev_show_id, c.tms_prev_show_id) IS NOT NULL THEN NULL
 WHEN LOWER(coalesce(tis.app_name)) = 'unknown' THEN NULL
 ELSE coalesce(tis.app_name) END
 WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
 END, 
    prev_vizio_station.station_id, 
    prev_vizio_program.program_aggregate_id
    FROM (
        SELECT * FROM prod.detection.viewing_commercials_firehose_dedup AS v
        WHERE 'VIZIO' IN ('VIZIO')           -- If only VIZIO
        -- WHERE 'VIZIO' IN ('ONN')          -- If only ONN
        -- WHERE 'VIZIO' IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
          AND v.session_start >= '{start_time}'::timestamp
          AND v.session_start < '{end_time}'::timestamp
          AND v.partition_key >= '{start_time}'::timestamp::DATE
          AND v.partition_key <= '{end_time}'::timestamp::DATE
          AND v.fk_commercial_source_id IN (1)
        UNION
        SELECT * FROM prod.detection_onn.viewing_commercials_firehose_dedup AS o
        WHERE 'ONN' IN ('VIZIO')           -- If only VIZIO
        -- WHERE 'ONN' IN ('ONN')          -- If only ONN
        -- WHERE 'ONN' IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
          AND o.session_start >= '{start_time}'::timestamp
          AND o.session_start < '{end_time}'::timestamp
          AND o.partition_key >= '{start_time}'::timestamp::DATE
          AND o.partition_key <= '{end_time}'::timestamp::DATE
          AND o.fk_commercial_source_id IN (1)
    ) AS c
    JOIN commercial_id_external_firehose AS m 
        ON c.fk_commercial_id = m.fk_commercial_id
    JOIN prod.detection.zoo AS z 
        ON c.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    INNER JOIN
        prod.detection.tv AS tv
        ON c.fk_tvid = tv.tvid
        AND tv.oem IN ('VIZIO') -- If only VIZIO
        -- AND tv.oem IN ('ONN') -- If only ONN
        -- AND tv.oem IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
    JOIN
        prod.detection.tv_populations AS tp
        ON c.fk_tvid = tp.fk_tvid 
    JOIN
        prod.detection.populations AS pop
        ON tp.fk_population_id = pop.population_id 
        AND LOWER(pop.population_name) = 'opted_in'
    JOIN
        prod.detection.location AS location
        ON c.fk_location_id = location.location_id
        AND UPPER(location.country_code) = 'US'
    JOIN (
        SELECT * FROM prod.detection.tv_input_stats_firehose v
         WHERE 'VIZIO' IN ('VIZIO')           -- If only VIZIO
         -- WHERE 'VIZIO' IN ('ONN')          -- If only ONN
         -- WHERE 'VIZIO' IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
          AND v.create_timestamp <= '{end_time}'::timestamp
          AND v.next_create_timestamp >= '{start_time}'::timestamp
        UNION
        SELECT * FROM prod.detection_onn.tv_input_stats_firehose o
        WHERE 'ONN' IN ('VIZIO')           -- If only VIZIO
        -- WHERE 'ONN' IN ('ONN')          -- If only ONN
        -- WHERE 'ONN' IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
          AND o.create_timestamp <= '{end_time}'::timestamp
          AND o.next_create_timestamp >= '{start_time}'::timestamp
        ) AS tvis
        ON c.session_start >= tvis.create_timestamp 
        AND c.session_start < tvis.next_create_timestamp
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
        AND cl2.client_name <> '{commercial_client}'
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
        ON c.tms_next_station_id = next_station_blacklist.station_id
        AND next_station_blacklist.vendor_name = next_map.mapped_vendor
    LEFT OUTER JOIN prod.detection.station_metadata_obfuscation AS next_station_obfs
        ON c.tms_next_station_id = next_station_obfs.vendor_station_id
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
    LEFT OUTER JOIN
        prod.detection.tv_ip_address AS ip
        ON c.session_start >= ip.create_timestamp
        AND c.session_start < ip.next_create_timestamp
        AND ip.create_timestamp <= '{end_time}'::timestamp
        AND ip.next_create_timestamp >= '{start_time}'::timestamp
        AND c.fk_tvid = ip.fk_tvid
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
    WHERE CASE WHEN tvis.category = 'APPS' AND prev_vizio_station.name IS NULL AND acrb.app_name IS NOT NULL THEN FALSE ELSE TRUE END;