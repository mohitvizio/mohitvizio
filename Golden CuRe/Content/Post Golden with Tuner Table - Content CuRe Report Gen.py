# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('HOUR', session_start), COUNT(*)
# MAGIC FROM dev.detection.viewing_content_golden
# MAGIC WHERE session_start >= CURRENT_DATE
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 DESC

# COMMAND ----------

def get_column_names(vendor_name):
    columns = "tvid string, hash string, zipcode string, dma string, "
    if vendor == 'TMS':
        columns += 'episode_id_tms string, show_title_tms string, air_date_tms string, channel_callsign_tms string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate_tms string, live_tms string, ip string, input_category string, input_device string, app_service string, tuner_channel_number string'
    else:
        columns += 'episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string, tuner_channel_number string'
    return columns

# COMMAND ----------

def column_names_insert(vendor_name):
    columns = 'tvid, hash, zipcode, dma, '
    if vendor_name == 'TMS':
        columns += 'episode_id_tms, show_title_tms, air_date_tms, channel_callsign_tms, mt_start, ts_start, ts_end, channel_affiliate_tms, live_tms, ip, input_category, input_device, app_service, tuner_channel_number'
    else:
        columns += 'episode_id, show_title, air_date, channel_callsign, mt_start, ts_start, ts_end, channel_affiliate, live, ip, input_category, input_device, app_service, tuner_channel_number'
    return columns

# COMMAND ----------

def content_only_sql(client_name, vod_toggle, content_report_type):
    if content_report_type == 'content only':
        return f"""
        -- Following conditions are only for Content Only Reports
        AND (mt.tuner_content_only_condition = FALSE OR mt.content_only_condition = FALSE)
        AND CASE WHEN '{client_name}' != 'nielsen' AND mt.nielsen_exclusive THEN FALSE ELSE TRUE END
        AND CASE WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN FALSE ELSE TRUE END
        AND CASE WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN FALSE ELSE TRUE END
        AND CASE WHEN mt.acrb_clients <=> '|ALL|' AND '{app_toggle}' != mt.app_service THEN FALSE ELSE TRUE END
        AND CASE WHEN WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE
             AND mt.tuner_tms_episode_id <=> mt.tms_episode_id AND mt.tuner_tivo_episode_id <=> mt.tivo_episode_id THEN FALSE ELSE TRUE END
        """
    else:
        return ""

# COMMAND ----------

start_time = '2025-07-16 15:00:00'
end_time = '2025-07-16 16:00:00'

client_name = 'comscore'
vendor = 'TIVO'
other_vendor = 'TMS' if vendor == 'TIVO' else 'TIVO'

content_report_type = 'content + null'
# contet_report_type = 'content only'

schema_name = 'dev.mohit_gangwani'
table_name = f'golden_tuner_{content_report_type.replace(" ", "").replace("+", "_")}_{client_name}_{vendor.lower()}_'
table_name += start_time.replace('-', '_').replace(':', ' ').split(' ')[0]
table_name += f"_{start_time.replace('-', '_').replace(':', ' ').split(' ')[1]}"

golden_table = 'dev.detection.viewing_content_golden'
vod_toggle = False
app_toggle = ''

# COMMAND ----------

print(f'start_time = {start_time}\nend_time = {end_time}\nclient_name = {client_name}\n')
print(f'vendor = {vendor}\nother_vendor = {other_vendor}\nschema_name = {schema_name}\ntable_name = {table_name}')

# COMMAND ----------

drop_sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name};" 
create_sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({get_column_names(vendor)});"

# COMMAND ----------

spark.sql(drop_sql)
spark.sql(create_sql)

# COMMAND ----------

spark.sql(f"""
INSERT INTO {schema_name}.{table_name} (
    {column_names_insert(vendor)}
)
SELECT mt.tvid
, '' AS hash
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND '{app_toggle}' != COALESCE(mt.app_service, '98989898989898') THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') OR mt.tuner_app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN
            CASE WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND mt.tuner_tms_episode_id IS NULL THEN NULL
                 WHEN COALESCE(mt.tuner_tms_airdate, mt.tms_airdate) IS NULL AND COALESCE(mt.tuner_tms_episode_id, mt.tms_episode_id) IS NULL THEN NULL
                 ELSE COALESCE(mt.tuner_tms_episode_id, mt.tms_episode_id) END
       WHEN mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND COALESCE(mt.tuner_tms_episode_id, mt.tuner_tivo_episode_id) IS NULL THEN NULL
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tuner_tms_episode_id, mt.tms_episode_id)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tuner_tivo_episode_id, mt.tivo_episode_id)
  END AS episode_id

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
                                            ELSE COALESCE(mt.tivo_title, mt.tms_title) END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND '{app_toggle}' != COALESCE(mt.app_service, '98989898989898') THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') OR mt.tuner_app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN
            CASE WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND mt.tuner_tms_episode_id IS NULL THEN NULL
                 WHEN COALESCE(mt.tuner_tms_airdate, mt.tms_airdate) IS NULL AND COALESCE(mt.tuner_tms_episode_id, mt.tms_episode_id) IS NULL THEN NULL
                 ELSE COALESCE(mt.tuner_tms_title, mt.tms_title) END
       WHEN mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND COALESCE(mt.tuner_tms_episode_id, mt.tuner_tivo_episode_id) IS NULL THEN NULL
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tuner_tms_title, mt.tuner_tivo_title, mt.tms_title, mt.tivo_title)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tuner_tivo_title, mt.tuner_tms_title, mt.tivo_title, mt.tms_title)
  END AS show_title

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                                            ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND '{app_toggle}' != COALESCE(mt.app_service, '98989898989898') THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') OR mt.tuner_app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN
            CASE WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND mt.tuner_tms_episode_id IS NULL THEN NULL
                 WHEN COALESCE(mt.tuner_tms_airdate, mt.tms_airdate) IS NULL AND COALESCE(mt.tuner_tms_episode_id, mt.tms_episode_id) IS NULL THEN NULL
                 ELSE COALESCE(mt.tuner_tms_airdate, mt.tms_airdate) END
       WHEN mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND COALESCE(mt.tuner_tms_episode_id, mt.tuner_tivo_episode_id) IS NULL THEN NULL 
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tuner_tms_airdate, mt.tuner_tivo_airdate, mt.tms_airdate, mt.tivo_airdate)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tuner_tivo_airdate, mt.tuner_tms_airdate, mt.tivo_airdate, mt.tms_airdate)
  END AS air_date

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                                            ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND '{app_toggle}' != COALESCE(mt.app_service, '98989898989898') THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') OR mt.tuner_app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN
            CASE WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND mt.tuner_tms_episode_id IS NULL THEN NULL
                 WHEN COALESCE(mt.tuner_tms_airdate, mt.tms_airdate) IS NULL AND COALESCE(mt.tuner_tms_episode_id, mt.tms_episode_id) IS NULL THEN NULL
                 ELSE COALESCE(mt.tuner_tms_channel_callsign, mt.tms_channel_callsign) END
       WHEN mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND COALESCE(mt.tuner_tms_episode_id, mt.tuner_tivo_episode_id) IS NULL THEN NULL
       WHEN '{vendor}' = 'TMS'
          THEN COALESCE(mt.tuner_tms_channel_callsign, mt.tuner_tivo_channel_callsign, mt.tms_channel_callsign, mt.tivo_channel_callsign)
       WHEN '{vendor}' = 'TIVO'
          THEN COALESCE(mt.tuner_tivo_channel_callsign, mt.tuner_tms_channel_callsign, mt.tivo_channel_callsign, mt.tms_channel_callsign)
  END AS channel_callsign

, CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND '{app_toggle}' != COALESCE(mt.app_service, '98989898989898') THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') OR mt.tuner_app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN
            CASE WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND mt.tuner_tms_episode_id IS NULL THEN NULL
                 WHEN COALESCE(mt.tuner_tms_airdate, mt.tms_airdate) IS NULL AND COALESCE(mt.tuner_tms_episode_id, mt.tms_episode_id) IS NULL THEN NULL
                 ELSE COALESCE(mt.tuner_mt_start, mt.mt_start) END
       WHEN mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND COALESCE(mt.tuner_tms_episode_id, mt.tuner_tivo_episode_id) IS NULL THEN NULL
       WHEN COALESCE(mt.tuner_tivo_airdate, mt.tuner_tms_airdate, mt.tivo_airdate, mt.tms_airdate) IS NULL THEN NULL
       ELSE COALESCE(mt.tuner_mt_start, mt.mt_start)
  END AS mt_start

, mt.session_start AS ts_start
, mt.session_end   AS ts_end

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor}' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                                   ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND '{app_toggle}' != COALESCE(mt.app_service, '98989898989898') THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') OR mt.tuner_app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN
            CASE WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND mt.tuner_tms_episode_id IS NULL THEN NULL
                 WHEN COALESCE(mt.tuner_tms_airdate, mt.tms_airdate) IS NULL AND COALESCE(mt.tuner_tms_episode_id, mt.tms_episode_id) IS NULL THEN NULL
                 ELSE COALESCE(mt.tuner_tms_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND COALESCE(mt.tuner_tms_episode_id, mt.tuner_tivo_episode_id) IS NULL THEN NULL
       WHEN '{vendor}' = 'TMS'
          THEN COALESCE(mt.tuner_tms_channel_affiliate, mt.tuner_tivo_channel_affiliate, mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
       WHEN '{vendor}' = 'TIVO'
          THEN COALESCE(mt.tuner_tivo_channel_affiliate, mt.tuner_tms_channel_affiliate, mt.tivo_channel_affiliate, mt.tms_channel_affiliate)
  END AS channel_affiliate

, CASE WHEN mt.vizio_epg_not_null THEN 't'
       WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.acrb_clients <=> '|ALL|' AND '{app_toggle}' != COALESCE(mt.app_service, '98989898989898') THEN NULL
       WHEN mt.app_service IN ('{exclude_apps}') OR mt.tuner_app_service IN ('{exclude_apps}') THEN NULL
       WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN
            CASE WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND mt.tuner_tms_episode_id IS NULL THEN NULL
                 WHEN COALESCE(mt.tuner_tms_airdate, mt.tms_airdate) IS NULL AND COALESCE(mt.tuner_tms_episode_id, mt.tms_episode_id) IS NULL THEN NULL
                 ELSE COALESCE(mt.tuner_is_live, mt.is_live) END
       WHEN mt.nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE AND COALESCE(mt.tuner_tms_episode_id, mt.tuner_tivo_episode_id) IS NULL THEN NULL
       WHEN COALESCE(mt.tuner_tivo_airdate, mt.tuner_tms_airdate, mt.tivo_airdate, mt.tms_airdate) IS NULL THEN NULL
       ELSE COALESCE(mt.tuner_is_live, mt.is_live)
  END AS live

, mt.ip_address AS ip
, COALESCE(mt.tuner_input_category, mt.input_category) AS input_category
, COALESCE(mt.tuner_input_device, mt.input_device) AS input_device

, CASE WHEN mt.vizio_epg_not_null = False
       THEN CASE WHEN mt.appb_clients <=> '|ALL|' AND '{app_toggle}' <=> mt.app_service THEN COALESCE(mt.tuner_app_service, mt.app_service)
                 WHEN mt.appb_clients IS NOT NULL AND mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|{client_name}|%' THEN 'OBFUSCATED'
                 WHEN mt.appb_clients <=> '|ALL|' THEN 'OBFUSCATED'
                 WHEN mt.app_service IN ('{exclude_apps}') OR mt.tuner_app_service IN ('{exclude_apps}') THEN NULL
                 ELSE COALESCE(mt.tuner_app_service, mt.app_service) END
       ELSE COALESCE(mt.tuner_app_service, mt.app_service)
  END AS app_service

, mt.tuner_channel_number AS tuner_channel_number

FROM {golden_table} AS mt
JOIN prod.detection.tv_populations AS u
  ON mt.fk_tvid = u.fk_tvid
JOIN prod.detection.populations AS pop
  ON u.fk_population_id = pop.population_id 
 AND pop.population_name = 'opted_in'
WHERE mt.session_start >= '{start_time}'::TIMESTAMP
  AND mt.session_start < '{end_time}'::TIMESTAMP
  AND mt.session_start_hour >= DATE_TRUNC('HOUR', '{start_time}'::TIMESTAMP)
  AND mt.session_start_hour <= DATE_TRUNC('HOUR', '{end_time}'::TIMESTAMP)
{content_only_sql(client_name, vod_toggle, content_report_type)}
"""
)

# COMMAND ----------

content_only_sql = f"""
-- Following conditions are only for Content Only Reports
WHERE mt.tuner_content_only_condition = FALSE
  AND CASE WHEN '{client_name}' != 'nielsen' AND mt.nielsen_exclusive THEN FALSE ELSE TRUE END
  AND CASE WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN FALSE ELSE TRUE END
  AND CASE WHEN (mt.acrb_clients != '||' AND mt.acrb_clients NOT LIKE '%|{client_name}|%')  OR mt.acrb_clients = '|ALL|' THEN FALSE ELSE TRUE END
  AND CASE WHEN mt.app_service IN ('{exclude_apps}') OR mt.tuner_app_service IN ('{exclude_apps}') THEN FALSE ELSE TRUE END
  AND CASE WHEN {vod_toggle} = FALSE AND mt.vod_station = TRUE THEN FALSE ELSE TRUE END
  """

# COMMAND ----------



# COMMAND ----------

if contet_report_type == 'content + null':
    print(initial_sql)
elif contet_report_type == 'content only':
    print(initial_sql + content_only_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH stations AS (
# MAGIC   SELECT x.inscape_call_sign
# MAGIC   , x.mapped_vendor_station_id AS station_id
# MAGIC   , x.station_name
# MAGIC   , x.station_time_zone AS station_tz
# MAGIC   FROM (
# MAGIC     SELECT ism.inscape_call_sign
# MAGIC     , ism.mapped_vendor_station_id
# MAGIC     , st.inscape_station_name AS station_name
# MAGIC     , st.station_time_zone
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY ism.mapped_vendor_station_id ORDER BY ism.created_at DESC) AS rn
# MAGIC     FROM detection.inscape_station_map AS ism
# MAGIC     JOIN detection.epg_station st
# MAGIC       ON st.station_id = ism.mapped_vendor_station_id
# MAGIC      AND st.vendor_name = 'TIVO'
# MAGIC     WHERE mapped_vendor = 'TIVO'
# MAGIC       AND (st.ingested = 'TRUE' OR st.attributed = 'TRUE')
# MAGIC       AND st.local_or_national = 'National'
# MAGIC   ) x
# MAGIC   WHERE x.rn = 1
# MAGIC )
# MAGIC , simulcast_schedule AS (
# MAGIC   WITH simulcast_shows AS (
# MAGIC     SELECT sch.airdate
# MAGIC     , sh.database_key AS epid
# MAGIC     , COUNT(DISTINCT ism.station_name) AS num_stations
# MAGIC     FROM detection.epg_schedule sch
# MAGIC     JOIN detection.epg_show sh
# MAGIC       ON sh.show_id = sch.fk_show_id
# MAGIC     JOIN stations AS ism
# MAGIC       ON ism.station_id = sch.fk_station_id
# MAGIC     WHERE sch.vendor_name = 'TIVO'
# MAGIC       AND sh.vendor_name = 'TIVO'
# MAGIC       AND sch.airdate >= CURRENT_DATE - 90
# MAGIC       AND sch.airdate <= CURRENT_DATE + 1
# MAGIC     GROUP BY 1, 2
# MAGIC   )
# MAGIC   SELECT smsh.airdate
# MAGIC   , smsh.epid
# MAGIC   , sch.fk_show_id
# MAGIC   , sch.fk_station_id
# MAGIC   , ism.inscape_call_sign
# MAGIC   , ism.station_name
# MAGIC   , ism.station_tz
# MAGIC   FROM simulcast_shows AS smsh
# MAGIC   JOIN detection.epg_schedule sch
# MAGIC     ON sch.airdate = smsh.airdate
# MAGIC   JOIN detection.epg_show sh
# MAGIC     ON sh.show_id = sch.fk_show_id
# MAGIC    AND sh.database_key = smsh.epid
# MAGIC   JOIN stations AS ism
# MAGIC     ON ism.station_id = sch.fk_station_id
# MAGIC   WHERE sch.vendor_name = 'TIVO'
# MAGIC     AND sh.vendor_name = 'TIVO'
# MAGIC     AND sch.airdate >= CURRENT_DATE - 90
# MAGIC     AND sch.airdate <= CURRENT_DATE + 1
# MAGIC     AND smsh.num_stations > 1
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC (
# MAGIC   SELECT DATE_TRUNC('DAY', session_start) AS session_day
# MAGIC   , sch.station_name
# MAGIC   , 'DP4' AS pipeline
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , SUM(session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_tvid) AS tv_count
# MAGIC   , AVG(session_duration) AS avg_session_duration
# MAGIC   FROM (
# MAGIC     SELECT *
# MAGIC     , LEAD(vc.session_start) OVER (PARTITION BY vc.fk_tvid ORDER BY vc.session_start, vc.session_end) AS next_start
# MAGIC     , LAG(vc.session_end) OVER (PARTITION BY vc.fk_tvid ORDER BY vc.session_start, vc.session_end) AS prev_end
# MAGIC     , LEAD(vc.session_duration) OVER (PARTITION BY vc.fk_tvid ORDER BY vc.session_start, vc.session_end) AS next_duration
# MAGIC     , LAG(vc.session_duration) OVER (PARTITION BY vc.fk_tvid ORDER BY vc.session_start, vc.session_end) AS prev_duration
# MAGIC     , CASE WHEN next_start < vc.session_end AND DATE_TRUNC('HOUR', next_start) = DATE_TRUNC('HOUR', vc.session_end) THEN 1
# MAGIC            WHEN prev_end > vc.session_start AND DATE_TRUNC('HOUR', prev_end) = DATE_TRUNC('HOUR', vc.session_start) THEN 1
# MAGIC       END AS overlapping_session
# MAGIC     , CASE WHEN overlapping_session IS NULL THEN 1
# MAGIC            ELSE CASE WHEN next_start IS NOT NULL
# MAGIC                       AND next_start < vc.session_end
# MAGIC                       AND next_duration < vc.session_duration
# MAGIC                       AND DATE_TRUNC('HOUR', next_start) = DATE_TRUNC('HOUR', vc.session_start) THEN 1
# MAGIC                      WHEN prev_end IS NOT NULL
# MAGIC                       AND prev_end > vc.session_start
# MAGIC                       AND prev_duration < vc.session_duration
# MAGIC                       AND DATE_TRUNC('HOUR', prev_end) = DATE_TRUNC('HOUR', vc.session_end) THEN 1
# MAGIC                 END
# MAGIC       END AS keep
# MAGIC     FROM (
# MAGIC       SELECT fk_tvid
# MAGIC       , session_start
# MAGIC       , session_end
# MAGIC       , session_duration
# MAGIC       , fk_station_id
# MAGIC       , fk_show_id
# MAGIC       , airdate
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY fk_tvid, session_start ORDER BY session_start, session_end DESC, NVL(fk_show_id, 0) DESC) AS rn
# MAGIC       FROM prod.detection.viewing_content_firehose
# MAGIC       WHERE session_start >= CURRENT_DATE - INTERVAL 8 DAYS
# MAGIC       AND session_start < CURRENT_DATE - INTERVAL 1 DAY
# MAGIC     ) vc
# MAGIC     WHERE vc.rn = 1
# MAGIC   ) vc
# MAGIC  JOIN simulcast_schedule AS sch
# MAGIC   ON sch.fk_show_id = vc.fk_show_id
# MAGIC  AND sch.fk_station_id = vc.fk_station_id
# MAGIC  AND sch.airdate = vc.airdate
# MAGIC  WHERE vc.keep = 1
# MAGIC  GROUP BY 1, 2
# MAGIC )
# MAGIC UNION
# MAGIC (
# MAGIC SELECT DATE_TRUNC('DAY', session_start) AS session_day
# MAGIC   , sch.station_name
# MAGIC   , 'DP5' AS pipeline
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , SUM(session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_tvid) AS tv_count
# MAGIC   , AVG(session_duration) AS avg_session_duration
# MAGIC   FROM (
# MAGIC     SELECT *
# MAGIC     , LEAD(vc.session_start) OVER (PARTITION BY vc.fk_tvid ORDER BY vc.session_start, vc.session_end) AS next_start
# MAGIC     , LAG(vc.session_end) OVER (PARTITION BY vc.fk_tvid ORDER BY vc.session_start, vc.session_end) AS prev_end
# MAGIC     , LEAD(vc.session_duration) OVER (PARTITION BY vc.fk_tvid ORDER BY vc.session_start, vc.session_end) AS next_duration
# MAGIC     , LAG(vc.session_duration) OVER (PARTITION BY vc.fk_tvid ORDER BY vc.session_start, vc.session_end) AS prev_duration
# MAGIC     , CASE WHEN next_start < vc.session_end AND DATE_TRUNC('HOUR', next_start) = DATE_TRUNC('HOUR', vc.session_end) THEN 1
# MAGIC            WHEN prev_end > vc.session_start AND DATE_TRUNC('HOUR', prev_end) = DATE_TRUNC('HOUR', vc.session_start) THEN 1
# MAGIC       END AS overlapping_session
# MAGIC     , CASE WHEN overlapping_session IS NULL THEN 1
# MAGIC            ELSE CASE WHEN next_start IS NOT NULL
# MAGIC                       AND next_start < vc.session_end
# MAGIC                       AND next_duration < vc.session_duration
# MAGIC                       AND DATE_TRUNC('HOUR', next_start) = DATE_TRUNC('HOUR', vc.session_start) THEN 1
# MAGIC                      WHEN prev_end IS NOT NULL
# MAGIC                       AND prev_end > vc.session_start
# MAGIC                       AND prev_duration < vc.session_duration
# MAGIC                       AND DATE_TRUNC('HOUR', prev_end) = DATE_TRUNC('HOUR', vc.session_end) THEN 1
# MAGIC                 END
# MAGIC       END AS keep
# MAGIC     FROM (
# MAGIC       SELECT fk_tvid
# MAGIC       , session_start
# MAGIC       , session_end
# MAGIC       , session_duration
# MAGIC       , fk_station_id
# MAGIC       , fk_show_id
# MAGIC       , airdate
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY fk_tvid, session_start ORDER BY session_start, session_end DESC, NVL(fk_show_id, 0) DESC) AS rn
# MAGIC       FROM qa.detection.viewing_content_firehose
# MAGIC       WHERE session_start >= CURRENT_DATE - INTERVAL 8 DAYS
# MAGIC       AND session_start < CURRENT_DATE - INTERVAL 1 DAY
# MAGIC     ) vc
# MAGIC     WHERE vc.rn = 1
# MAGIC   ) vc
# MAGIC  JOIN simulcast_schedule AS sch
# MAGIC   ON sch.fk_show_id = vc.fk_show_id
# MAGIC  AND sch.fk_station_id = vc.fk_station_id
# MAGIC  AND sch.airdate = vc.airdate
# MAGIC  WHERE vc.keep = 1
# MAGIC  GROUP BY 1, 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.detection.viewing_content_golden
# MAGIC LIMIT 100
