# Databricks notebook source
def get_column_names(vendor_name):
    columns = "tvid string, hash string, zipcode string, dma string, "
    if vendor == 'TMS':
        columns += 'episode_id_tms string, show_title_tms string, air_date_tms string, channel_callsign_tms string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate_tms string, live_tms string, ip string, input_category string, input_device string, app_service string'
    else:
        columns += 'episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string'
    return columns

# COMMAND ----------

def column_names_insert(vendor_name):
    columns = 'tvid, hash, zipcode, dma, '
    if vendor_name == 'TMS':
        columns += 'episode_id_tms, show_title_tms, air_date_tms, channel_callsign_tms, mt_start, ts_start, ts_end, channel_affiliate_tms, live_tms, ip, input_category, input_device, app_service'
    else:
        columns += 'episode_id, show_title, air_date, channel_callsign, mt_start, ts_start, ts_end, channel_affiliate, live, ip, input_category, input_device, app_service'
    return columns

# COMMAND ----------

def content_only_sql(client_name, vod_toggle, content_report_type):
  if content_report_type == 'content only':
      return f"""
      -- Following conditions are only for Content Only Reports
        AND mt.content_only_condition = FALSE
        AND CASE WHEN '{client_name}' != 'nielsen' AND mt.nielsen_exclusive THEN FALSE ELSE TRUE END
        AND CASE WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN FALSE ELSE TRUE END
        AND CASE WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN FALSE ELSE TRUE END
        AND CASE WHEN mt.acrb_clients <=> '|ALL|' AND NOT('{app_toggle}' <=> NVL(mt.app_service, '98989898')) THEN FALSE ELSE TRUE END
        AND CASE WHEN mt.app_service IN ('{exclude_apps}')  THEN FALSE ELSE TRUE END
        AND CASE WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE THEN FALSE ELSE TRUE END
        AND CASE WHEN '{client_name}' = 'nielsen' AND mt.tms_airdate IS NULL THEN FALSE ELSE TRUE END
        """
  else:
    return ""

# COMMAND ----------

start_time = '2025-07-14 14:00:00'
end_time = '2025-07-14 15:00:00'

client_name = 'nielsen'
vendor = 'TMS'
other_vendor = 'TMS' if vendor == 'TIVO' else 'TIVO'

content_report_type = 'content + null'
# content_report_type = 'content only'

schema_name = 'dev.mohit_gangwani'
table_name = f'golden_{content_report_type.replace(" ", "").replace("+", "_")}_{client_name}_{vendor.lower()}_'
table_name += start_time.replace('-', '_').replace(':', ' ').split(' ')[0]
table_name += f"_{start_time.replace('-', '_').replace(':', ' ').split(' ')[1]}"

golden_table = 'dev.detection.viewing_content_golden'
vod_toggle = False
include_additional_apps = ''
exclude_apps = ''

# COMMAND ----------

print(f'start_time = {start_time}\nend_time = {end_time}\nclient_name = {client_name}\n')
print(f'vendor = {vendor}\nother_vendor = {other_vendor}\nschema_name = {schema_name}\ntable_name = {table_name}')

# COMMAND ----------

drop_sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name};" 
create_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({get_column_names(vendor)});"

# COMMAND ----------

spark.sql(drop_sql.format(schema_name=schema_name, table_name=table_name))

# COMMAND ----------

spark.sql(create_sql.format(schema_name=schema_name, table_name=table_name))

# COMMAND ----------

spark.sql(f"""
INSERT INTO {schema_name}.{table_name} (
    {column_names_insert(vendor)}
)
SELECT DISTINCT mt.tvid
, '' AS hash
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND NOT('{include_additional_apps}' <=> NVL(mt.app_service, '98989898')) THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' != 'nielsen'  AND mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE THEN NULL
       WHEN '{client_name}' = 'nielsen' AND mt.tms_airdate IS NULL THEN NULL
       WHEN '{vendor}' = 'TMS'  THEN mt.tms_episode_id
       WHEN '{vendor}' = 'TIVO' THEN mt.tivo_episode_id
  END AS episode_id

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
                                            ELSE COALESCE(mt.tivo_title, mt.tms_title) END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND NOT('{include_additional_apps}' <=> NVL(mt.app_service, '98989898')) THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' != 'nielsen'  AND mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE THEN NULL
       WHEN '{client_name}' = 'nielsen' AND mt.tms_airdate IS NULL THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_title
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_title, mt.tivo_title)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_title, mt.tms_title)
  END AS show_title

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                                   ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND NOT('{include_additional_apps}' <=> NVL(mt.app_service, '98989898')) THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' != 'nielsen'  AND mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_airdate
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_airdate, mt.tms_airdate)
  END AS air_date

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                                            ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND NOT('{include_additional_apps}' <=> NVL(mt.app_service, '98989898')) THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE THEN NULL
       WHEN '{client_name}' = 'nielsen' AND mt.tms_airdate IS NULL THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_channel_callsign
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign)
  END AS channel_callsign

, CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND NOT('{include_additional_apps}' <=> NVL(mt.app_service, '98989898')) THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' != 'nielsen'  AND mt.nielsen_exclusive   THEN NULL
       WHEN {vod_toggle} = FALSE          AND mt.vod_station = TRUE  THEN NULL
       WHEN '{client_name}' = 'nielsen' AND mt.tms_airdate IS NULL THEN NULL
       ELSE mt.mt_start
  END AS mt_start

, session_start AS ts_start
, session_end   AS ts_end

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                                            ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND NOT('{include_additional_apps}' <=> NVL(mt.app_service, '98989898')) THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE THEN NULL
       WHEN '{client_name}' = 'nielsen' AND mt.tms_airdate IS NULL THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_channel_affiliate
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate)
  END AS channel_affiliate

, CASE WHEN mt.vizio_epg_not_null THEN 't'
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND NOT('{include_additional_apps}' <=> NVL(mt.app_service, '98989898')) THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE THEN NULL
       WHEN '{client_name}' = 'nielsen'  AND mt.tms_airdate IS NULL THEN NULL
       WHEN '{client_name}' != 'nielsen' AND COALESCE(mt.tivo_airdate, mt.tms_airdate) IS NULL THEN NULL
       ELSE mt.is_live
  END AS live

, mt.ip_address AS ip
, mt.input_category
, mt.input_device

, CASE WHEN mt.vizio_epg_not_null = False
       THEN CASE WHEN mt.appb_clients <=> '|ALL|' AND '{include_additional_apps}' <=> NVL(mt.app_service, '98989898') THEN mt.app_service
                 WHEN mt.appb_clients IS NOT NULL AND mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|{client_name}|%' THEN 'OBFUSCATED'
                 WHEN mt.appb_clients <=> '|ALL|' THEN 'OBFUSCATED'
                 WHEN mt.app_service IN ('{exclude_apps}') THEN NULL
                 ELSE mt.app_service END
       ELSE mt.app_service
  END AS app_service

FROM {golden_table} AS mt
-- JOIN prod.detection.tv_populations AS u
--   ON mt.fk_tvid = u.fk_tvid
-- JOIN prod.detection.populations AS pop
--   ON u.fk_population_id = pop.population_id 
--  AND pop.population_name = 'opted_in'
WHERE mt.session_start >= '{start_time}'::TIMESTAMP
  AND mt.session_start < '{end_time}'::TIMESTAMP
{content_only_sql(client_name, vod_toggle, content_report_type)}
"""
)

# COMMAND ----------

 # if contet_report_type == 'content + null':
#     print(initial_sql)
# elif contet_report_type == 'content only':
#     print(initial_sql + content_only_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN '|ALL|' <=> '|ALL|' AND NOT('' <=> NVL(NULL, '98989898')) THEN FALSE ELSE TRUE END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN NULL IN ('')  THEN FALSE ELSE TRUE END
