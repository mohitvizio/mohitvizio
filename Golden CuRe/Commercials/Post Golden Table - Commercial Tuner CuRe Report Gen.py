# Databricks notebook source
def get_column_names(vendor_name):
    columns = "tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, "
    if vendor == 'TMS':
        columns += 'prev_episode_id_tms string, prev_title_tms string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign_tms string, prev_network_affiliate_tms string, next_episode_id_tms string, next_title_tms string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign_tms string, next_network_affiliate_tms string, live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service_tms string, tuner_channel_number string'
    else:
        columns += 'prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service string, tuner_channel_number string'
    return columns

# COMMAND ----------

def column_names_insert(vendor_name):
    columns = 'tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, '
    if vendor_name == 'TMS':
        columns += 'prev_episode_id_tms, prev_title_tms, prev_ts_start, prev_ts_end, prev_channel_callsign_tms, prev_network_affiliate_tms, next_episode_id_tms, next_title_tms, next_ts_start, next_ts_end, next_channel_callsign_tms, next_network_affiliate_tms, live, brand_name, title, duration, ip, input_category, input_device, app_service_tms, tuner_channel_number'
    else:
        columns += 'prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip, input_category, input_device, app_service, tuner_channel_number'
    return columns

# COMMAND ----------

def get_table_name(client_name, vendor, start_time):
    table_name = f'golden_all_comm_{client_name}_{vendor.lower()}_tuner_'
    table_name += start_time.replace('-', '_').replace(':', ' ').split(' ')[0]
    table_name += f"_{start_time.replace('-', '_').replace(':', ' ').split(' ')[1]}"
    return table_name

# COMMAND ----------

start_time = '2025-04-10 00:00:00'
end_time = '2025-04-10 02:00:00'

client_name = 'comscore'
comm_client = 'kinetiq' if client_name in ('nielsen', 'comscore') else client_name

vendor = 'TMS' if client_name == 'nielsen' else 'TIVO'

schema_name = 'dev.mohit_gangwani'
table_name = get_table_name(client_name, vendor, start_time)

golden_table = 'dev.mohit_gangwani.testing_golden_commercials_table'
tuner_golden_table = 'dev.mohit_gangwani.testing_golden_commercials_tuner_table'

vod_toggle = False
null_epid_toggle = False

print(f'{schema_name}.{table_name}')

# COMMAND ----------

# DBTITLE 1,Toggles
spark.sql(f'DROP TABLE IF EXISTS {schema_name}.{table_name};')
spark.sql(f'CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({get_column_names(vendor)});')

# COMMAND ----------

spark.sql(f"""
INSERT INTO {schema_name}.{table_name} ({column_names_insert(vendor)})
SELECT DISTINCT mt.tvid
, ''
, mt.zipcode
, mt.dma
, mt.external_id
, mt.mt_start
, mt.session_start
, mt.session_end
, CASE WHEN {null_epid_toggle} THEN NULL
       WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = False AND mt.prev_station_vod = True THEN NULL
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(tuner.tms_prev_episode_id, mt.tms_prev_episode_id)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(tuner.tivo_prev_episode_id, mt.tivo_prev_episode_id)
  END AS prev_episode_id

, CASE WHEN (mt.excluded_client_list IS NOT NULL
        AND mt.excluded_client_list != '||'
        AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = False AND mt.prev_station_vod = True THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN COALESCE(tuner.tms_prev_show_title, mt.tms_prev_show_title)
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(tuner.tms_prev_show_title, tuner.tivo_prev_show_title, mt.tms_prev_show_title, mt.tivo_prev_show_title)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(tuner.tivo_prev_show_title, tuner.tms_prev_show_title, mt.tivo_prev_show_title, mt.tms_prev_show_title)
  END AS prev_show_title

, mt.prev_ts_start
, mt.prev_ts_end

, CASE WHEN (mt.excluded_client_list IS NOT NULL
        AND mt.excluded_client_list != '||'
        AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = False AND mt.prev_station_vod = True THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN COALESCE(tuner.tms_prev_channel_callsign, mt.tms_prev_channel_callsign)
       WHEN '{vendor}' = 'TMS' THEN COALESCE(tuner.tms_prev_channel_callsign, tuner.tivo_prev_channel_callsign, mt.tms_prev_channel_callsign, mt.tivo_prev_channel_callsign)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(tuner.tivo_prev_channel_callsign, tuner.tms_prev_channel_callsign, mt.tivo_prev_channel_callsign, mt.tms_prev_channel_callsign)
  END AS prev_channel_callsign

, CASE WHEN (mt.excluded_client_list IS NOT NULL
        AND mt.excluded_client_list != '||'
        AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = False AND mt.prev_station_vod = True THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN COALESCE(tuner.tms_prev_network_affiliate, mt.tms_prev_network_affiliate)
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(tuner.tms_prev_network_affiliate, tuner.tivo_prev_network_affiliate, mt.tms_prev_network_affiliate, mt.tivo_prev_network_affiliate)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(tuner.tivo_prev_network_affiliate, tuner.tms_prev_network_affiliate, mt.tivo_prev_network_affiliate, mt.tms_prev_network_affiliate)
  END AS prev_network_affiliate

, CASE WHEN {null_epid_toggle} THEN NULL
       WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN {vod_toggle} = False AND mt.next_station_vod = True THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(tuner.tms_next_episode_id, mt.tms_next_episode_id)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(tuner.tivo_next_episode_id, mt.tivo_next_episode_id)
  END AS next_episode_id

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN {vod_toggle} = False AND next_station_vod = True THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN COALESCE(tuner.tms_next_show_title, mt.tms_next_show_title)
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(tuner.tms_next_show_title, tuner.tivo_next_show_title, mt.tms_next_show_title, mt.tivo_next_show_title)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(tuner.tivo_next_show_title, tuner.tms_next_show_title, mt.tivo_next_show_title, mt.tms_next_show_title)
  END AS next_show_title

, mt.next_ts_start
, mt.next_ts_end

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN {vod_toggle} = False AND mt.next_station_vod = True THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN COALESCE(tuner.tms_next_channel_callsign, mt.tms_next_channel_callsign)
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(tuner.tms_next_channel_callsign, tuner.tivo_next_channel_callsign, mt.tms_next_channel_callsign, mt.tivo_next_channel_callsign)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(tuner.tivo_next_channel_callsign, tuner.tms_next_channel_callsign, mt.tivo_next_channel_callsign, mt.tms_next_channel_callsign)
  END AS next_channel_callsign

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN {vod_toggle} = False AND mt.next_station_vod = True THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN COALESCE(tuner.tms_next_network_affiliate, mt.tms_next_network_affiliate)
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(tuner.tms_next_network_affiliate, tuner.tivo_next_network_affiliate, mt.tms_next_network_affiliate, mt.tivo_next_network_affiliate)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(tuner.tivo_next_network_affiliate, tuner.tms_next_network_affiliate, mt.tivo_next_network_affiliate, mt.tms_next_network_affiliate)
  END AS next_network_affiliate

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = False AND mt.prev_station_vod = True THEN NULL
       ELSE COALESCE(tuner.live, mt.live)
  END AS live

, mt.brand_name
, mt.title
, mt.duration
, mt.ip
, COALESCE(tuner.input_category, mt.input_category) AS input_category
, COALESCE(tuner.input_device, mt.input_device) AS input_device

, CASE WHEN mt.prev_vizio_epg_not_null = False
       THEN CASE WHEN mt.appb_clients <=> '|ALL|' AND {app_toggle} <=> app_service THEN COALESCE(tuner.app_service, mt.app_service)
                 WHEN mt.appb_clients IS NOT NULL AND mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|{client_name}|%' THEN 'OBFUSCATED'
                 WHEN mt.appb_clients <=> '|ALL|' THEN 'OBFUSCATED'
                 ELSE COALESCE(tuner.app_service, mt.app_service) END
       ELSE COALESCE(tuner.app_service, mt.app_service) END
  END AS app_service

, tuner.tuner_channel_number

FROM {golden_table} AS mt
LEFT JOIN {tuner_golden_table} AS tuner
  ON tuner.fk_tvid = mt.fk_tvid
 AND tuner.session_start = mt.session_start
 AND tuner.session_end = mt.session_end
 AND tuner.external_id <=> mt.external_id
 AND tuner.mt_start <=> mt.mt_start
 AND tuner.ip <=> mt.ip
-- JOIN prod.detection.tv_populations AS tp
--   ON mt.fk_tvid = tp.fk_tvid
-- JOIN prod.detection.populations AS pop
--   ON tp.fk_population_id = pop.population_id
--   AND LOWER(pop.population_name) = 'opted_in'
WHERE mt.session_start >= '{start_time}'::timestamp
  AND mt.session_start < '{end_time}'::timestamp
  AND mt.commercial_client = '{comm_client}'
  AND CASE WHEN mt.prev_vizio_epg_not_null = False
           THEN CASE WHEN mt.acrb_clients <=> '|ALL|' AND {app_toggle} <=> app_service THEN TRUE
                     WHEN mt.acrb_clients IS NOT NULL AND mt.acrb_clients != '||' AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN FALSE
                     WHEN mt.acrb_clients <=> '|ALL|' THEN FALSE
                     ELSE TRUE END
           ELSE TRUE END;
""")

# COMMAND ----------


