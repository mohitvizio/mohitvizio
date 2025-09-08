# Databricks notebook source
def get_column_names(vendor_name):
    columns = "tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, "
    if vendor == 'TMS':
        columns += 'prev_episode_id_tms string, prev_title_tms string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign_tms string, prev_network_affiliate_tms string, next_episode_id_tms string, next_title_tms string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign_tms string, next_network_affiliate_tms string, live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service_tms string'
    else:
        columns += 'prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service string'
    return columns

# COMMAND ----------

def column_names_insert(vendor_name):
    columns = 'tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, '
    if vendor_name == 'TMS':
        columns += 'prev_episode_id_tms, prev_title_tms, prev_ts_start, prev_ts_end, prev_channel_callsign_tms, prev_network_affiliate_tms, next_episode_id_tms, next_title_tms, next_ts_start, next_ts_end, next_channel_callsign_tms, next_network_affiliate_tms, live, brand_name, title, duration, ip, input_category, input_device, app_service_tms'
    else:
        columns += 'prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip, input_category, input_device, app_service'
    return columns

# COMMAND ----------

start_time = '2025-06-17 15:00:00'
end_time = '2025-06-17 16:00:00'

client_name = 'ispot'
comm_client = 'ispot'
vendor = 'TIVO'

schema_name = 'dev.mohit_gangwani'
table_name = f'golden_all_comm_{client_name}_{vendor.lower()}_'
table_name += start_time.replace('-', '_').replace(':', ' ').split(' ')[0]
table_name += f"_{start_time.replace('-', '_').replace(':', ' ').split(' ')[1]}"

golden_table = 'prod.detection.viewing_commercials_golden'

# COMMAND ----------

vod_toggle = False
null_epid_toggle = False
app_toggle = ''

# COMMAND ----------

print(f'{schema_name}.{table_name}')

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {schema_name}.{table_name};')

# COMMAND ----------

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
       WHEN {vod_toggle} = False AND prev_station_vod = True THEN NULL
       WHEN '{vendor}' = 'TMS'  THEN mt.tms_prev_episode_id
       WHEN '{vendor}' = 'TIVO' THEN mt.tivo_prev_episode_id
  END AS prev_episode_id

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = False AND prev_station_vod = True THEN NULL
       WHEN '{client_name}' = 'nielsen'   THEN mt.tms_prev_show_title
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_prev_show_title, mt.tivo_prev_show_title)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_prev_show_title, mt.tms_prev_show_title)
  END AS prev_show_title

, mt.prev_ts_start
, mt.prev_ts_end

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN prev_station_vod = True THEN NULL
       WHEN '{client_name}' = 'nielsen'   THEN mt.tms_prev_channel_callsign
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_prev_channel_callsign, mt.tivo_prev_channel_callsign)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_prev_channel_callsign, mt.tms_prev_channel_callsign)
  END AS prev_channel_callsign

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN prev_station_vod = True THEN NULL
       WHEN '{client_name}' = 'nielsen'   THEN mt.tms_prev_network_affiliate
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_prev_network_affiliate, mt.tivo_prev_network_affiliate)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_prev_network_affiliate, mt.tms_prev_network_affiliate)
  END AS prev_network_affiliate

, CASE WHEN {null_epid_toggle} THEN NULL
       WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = False AND next_station_vod = True THEN NULL
       WHEN '{vendor}' = 'TMS'  THEN mt.tms_next_episode_id
       WHEN '{vendor}' = 'TIVO' THEN mt.tivo_next_episode_id
  END AS next_episode_id

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN {vod_toggle} = False AND next_station_vod = True THEN NULL
       WHEN '{client_name}' = 'nielsen'   THEN mt.tms_next_show_title
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_next_show_title, mt.tivo_next_show_title)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_next_show_title, mt.tms_next_show_title)
  END AS next_show_title

, mt.next_ts_start
, mt.next_ts_end

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN next_station_vod = True THEN NULL
       WHEN '{client_name}' = 'nielsen'   THEN mt.tms_next_channel_callsign
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_next_channel_callsign, mt.tivo_next_channel_callsign)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_next_channel_callsign, mt.tms_next_channel_callsign)
  END AS next_channel_callsign

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN next_station_vod = True THEN NULL
       WHEN '{client_name}' = 'nielsen'   THEN mt.tms_next_network_affiliate
       WHEN '{vendor}' = 'TMS'  THEN COALESCE(mt.tms_next_network_affiliate, mt.tivo_next_network_affiliate)
       WHEN '{vendor}' = 'TIVO' THEN COALESCE(mt.tivo_next_network_affiliate, mt.tms_next_network_affiliate)
  END AS next_network_affiliate

, CASE WHEN (mt.excluded_client_list IS NOT NULL
             AND mt.excluded_client_list != '||'
             AND mt.excluded_client_list NOT LIKE '%|{client_name}|%') OR mt.excluded_client_list <=> '|ALL|' THEN NULL
       WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN {vod_toggle} <=> False AND prev_station_vod = True THEN NULL
       ELSE mt.live
  END AS live

, mt.brand_name
, mt.title
, mt.duration
, mt.ip
, mt.input_category
, mt.input_device

, CASE WHEN mt.prev_vizio_epg_not_null = False
       THEN CASE WHEN mt.appb_clients <=> '|ALL|' AND '{app_toggle}' <=> mt.app_service THEN mt.app_service
                 WHEN mt.appb_clients IS NOT NULL AND mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|{client_name}|%' THEN 'OBFUSCATED'
                 WHEN mt.appb_clients = '|ALL|' THEN 'OBFUSCATED'
                 ELSE mt.app_service END
       ELSE mt.app_service
  END AS app_service

FROM {golden_table} AS mt
JOIN prod.detection.tv_populations AS tp
  ON mt.fk_tvid = tp.fk_tvid
JOIN prod.detection.populations AS pop
  ON tp.fk_population_id = pop.population_id
  AND LOWER(pop.population_name) = 'opted_in'
WHERE mt.session_start >= '{start_time}'::timestamp
  AND mt.session_start < '{end_time}'::timestamp
  AND CASE WHEN 'nielsen' = '{client_name}' THEN mt.commercial_client IN ('{comm_client}', 'nielsen') ELSE mt.commercial_client = '{comm_client}' END
  AND mt.session_start_hour = '{start_time}'::timestamp
  AND CASE WHEN mt.prev_vizio_epg_not_null = False
           THEN CASE WHEN mt.acrb_clients <=> '|ALL|' AND '{app_toggle}' <=> app_service THEN TRUE
                     WHEN mt.acrb_clients IS NOT NULL AND mt.acrb_clients != '||' AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN FALSE
                     WHEN mt.acrb_clients <=> '|ALL|' THEN FALSE
                     ELSE TRUE END
           ELSE TRUE END;
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT app_service, acrb_clients, prev_vizio_epg_not_null, COUNT(*)
# MAGIC FROM dev.mohit_gangwani.testing_golden_commercials_table
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT blocked_apps.app_name, override.client_name
# MAGIC   FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC   LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC     ON blocked_apps.app_name = override.app_name
# MAGIC   GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('HOUR', ts_start), COUNT(*)
# MAGIC FROM prod.cooker.vizio_content_firehose
# MAGIC WHERE ts_start >= CURRENT_DATE
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('HOUR', ts_start), COUNT(*)
# MAGIC FROM prod.staging.vizio_content_firehose
# MAGIC WHERE ts_start >= CURRENT_DATE
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH dp4 AS (
# MAGIC   SELECT DATE_TRUNC('HOUR', ts_start) AS session_hour
# MAGIC   , COUNT(Distinct tvid::BIGINT) AS total_tvs
# MAGIC   , SUM(TIMESTAMPDIFF(SECOND, ts_start, ts_end))/3600.0 AS total_duration
# MAGIC   , 1.0*SUM(CASE WHEN cid != 'unknown' THEN TIMESTAMPDIFF(SECOND, ts_start, ts_end) END) / (total_duration*3600.0) AS dect_rate
# MAGIC   FROM (
# MAGIC     SELECT *
# MAGIC     , LEAD(ts_start) OVER (PARTITION BY tvid ORDER BY ts_start, ts_end) AS next_start
# MAGIC     , LAG(ts_end) OVER (PARTITION BY tvid ORDER BY ts_start, ts_end) AS prev_end
# MAGIC     , LEAD(ts_duration) OVER (PARTITION BY tvid ORDER BY ts_start, ts_end) AS next_duration
# MAGIC     , LAG(ts_duration) OVER (PARTITION BY tvid ORDER BY ts_start, ts_end) AS prev_duration
# MAGIC     , CASE WHEN next_start < ts_end AND DATE_TRUNC('HOUR', next_start) = DATE_TRUNC('HOUR', ts_end) THEN 1
# MAGIC            WHEN prev_end > ts_start AND DATE_TRUNC('HOUR', prev_end) = DATE_TRUNC('HOUR', ts_start) THEN 1
# MAGIC       END AS overlapping_session
# MAGIC     , CASE WHEN overlapping_session IS NULL THEN 1
# MAGIC            ELSE CASE WHEN next_start IS NOT NULL AND next_start < ts_end AND next_duration < ts_duration AND DATE_TRUNC('HOUR', next_start) = DATE_TRUNC('HOUR', ts_start) THEN 1
# MAGIC                      WHEN prev_end IS NOT NULL AND prev_end > ts_start AND prev_duration < ts_duration AND DATE_TRUNC('HOUR', prev_end) = DATE_TRUNC('HOUR', ts_end) THEN 1
# MAGIC         END END AS keep
# MAGIC     FROM (
# MAGIC       SELECT tvid
# MAGIC       , ts_start
# MAGIC       , ts_end
# MAGIC       , TIMESTAMPDIFF(SECOND, ts_start, ts_end) AS ts_duration
# MAGIC       , cid
# MAGIC       , chan_callsign
# MAGIC       , epid
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY tvid, ts_start ORDER BY ts_start, ts_end DESC, epid) AS rn
# MAGIC       FROM prod.staging.vizio_content_firehose
# MAGIC       WHERE ts_start >= DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 3 HOURS
# MAGIC         AND ts_start < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 1 HOURS
# MAGIC
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC   ) vc
# MAGIC   WHERE keep = 1
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC , dp5 AS (
# MAGIC   SELECT DATE_TRUNC('Hour', ts_start) AS session_hour
# MAGIC   , COUNT(Distinct tvid::BIGINT) AS total_tvs
# MAGIC   , SUM(TIMESTAMPDIFF(SECOND, ts_start, ts_end))/3600.0 AS total_duration
# MAGIC   , 1.0*SUM(CASE WHEN cid != 'unknown' THEN TIMESTAMPDIFF(SECOND, ts_start, ts_end) END) / (total_duration*3600.0) AS dect_rate
# MAGIC   FROM (
# MAGIC     SELECT *
# MAGIC     , LEAD(ts_start) OVER (PARTITION BY tvid ORDER BY ts_start, ts_end) AS next_start
# MAGIC     , LAG(ts_end) OVER (PARTITION BY tvid ORDER BY ts_start, ts_end) AS prev_end
# MAGIC     , LEAD(ts_duration) OVER (PARTITION BY tvid ORDER BY ts_start, ts_end) AS next_duration
# MAGIC     , LAG(ts_duration) OVER (PARTITION BY tvid ORDER BY ts_start, ts_end) AS prev_duration
# MAGIC     , CASE WHEN next_start < ts_end AND DATE_TRUNC('HOUR', next_start) = DATE_TRUNC('HOUR', ts_end) THEN 1
# MAGIC            WHEN prev_end > ts_start AND DATE_TRUNC('HOUR', prev_end) = DATE_TRUNC('HOUR', ts_start) THEN 1
# MAGIC       END AS overlapping_session
# MAGIC     , CASE WHEN overlapping_session IS NULL THEN 1
# MAGIC            ELSE CASE WHEN next_start IS NOT NULL AND next_start < ts_end AND next_duration < ts_duration AND DATE_TRUNC('HOUR', next_start) = DATE_TRUNC('HOUR', ts_start) THEN 1
# MAGIC                      WHEN prev_end IS NOT NULL AND prev_end > ts_start AND prev_duration < ts_duration AND DATE_TRUNC('HOUR', prev_end) = DATE_TRUNC('HOUR', ts_end) THEN 1
# MAGIC         END END AS keep
# MAGIC     FROM (
# MAGIC       SELECT tvid
# MAGIC       , ts_start
# MAGIC       , ts_end
# MAGIC       , TIMESTAMPDIFF(SECOND, ts_start, ts_end) AS ts_duration
# MAGIC       , cid
# MAGIC       , chan_callsign
# MAGIC       , epid
# MAGIC       , ROW_NUMBER() OVER (PARTITION BY tvid, ts_start ORDER BY ts_start, ts_end DESC, epid) AS rn
# MAGIC       FROM prod.cooker.vizio_content_firehose
# MAGIC       WHERE ts_start >= DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 3 HOURS
# MAGIC         AND ts_start < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 1 HOURS
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC   ) vc
# MAGIC   WHERE keep = 1
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC SELECT dp4.session_hour
# MAGIC , dp4.total_tvs, dp4.total_duration, dp4.dect_rate
# MAGIC , dp5.total_tvs - dp4.total_tvs AS total_tvs_diff
# MAGIC , (dp5.total_tvs - dp4.total_tvs)/dp4.total_tvs AS total_tvs_diff_perc
# MAGIC , dp5.total_duration - dp4.total_duration AS total_duration_diff
# MAGIC , (dp5.total_duration - dp4.total_duration)/dp4.total_duration AS total_duration_diff_perc
# MAGIC , dp5.dect_rate - dp4.dect_rate AS dect_rate_diff
# MAGIC , (dp5.dect_rate - dp4.dect_rate)/dp4.dect_rate AS dect_rate_diff_perc
# MAGIC FROM dp4
# MAGIC JOIN dp5 ON dp4.session_hour = dp5.session_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM prod.detection.viewing_content_firehose
# MAGIC WHERE fk_content_id IS NULL
# MAGIC AND session_start >= CURRENT_DATE

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH dp4 AS (
# MAGIC   SELECT DATE_TRUNC('HOUR', vc.ts_start) AS session_hour
# MAGIC   , COUNT(Distinct vc.tvid) AS total_tvs
# MAGIC   , SUM(TIMESTAMPDIFF(SECOND, vc.ts_start, vc.ts_end))/3600.0 AS total_duration
# MAGIC   , COUNT(*) AS session_count
# MAGIC   FROM prod.staging.vizio_attrcomm_firehose vc
# MAGIC   WHERE ts_start >= DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 3 HOURS
# MAGIC     AND ts_start < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 1 HOURS
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC , dp5 AS (
# MAGIC   SELECT DATE_TRUNC('HOUR', vc.ts_start) AS session_hour
# MAGIC   , COUNT(Distinct vc.tvid) AS total_tvs
# MAGIC   , SUM(TIMESTAMPDIFF(SECOND, vc.ts_start, vc.ts_end))/3600.0 AS total_duration
# MAGIC   , COUNT(*) AS session_count
# MAGIC   FROM prod.cooker.vizio_attrcomm_firehose vc
# MAGIC   WHERE ts_start >= DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 3 HOURS
# MAGIC     AND ts_start < DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) - INTERVAL 1 HOURS
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC SELECT dp4.session_hour
# MAGIC , dp5.total_tvs - dp4.total_tvs AS total_tvs_diff
# MAGIC , (dp5.total_tvs - dp4.total_tvs)/dp4.total_tvs AS total_tvs_diff_perc
# MAGIC , dp5.total_duration - dp4.total_duration AS total_duration_diff
# MAGIC , (dp5.total_duration - dp4.total_duration)/dp4.total_duration AS total_duration_diff_perc
# MAGIC , dp5.session_count - dp4.session_count AS session_count_diff
# MAGIC , (dp5.session_count - dp4.session_count)/dp4.session_count AS session_count_diff_perc
# MAGIC FROM dp4
# MAGIC JOIN dp5 ON dp4.session_hour = dp5.session_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tv_latest AS (
# MAGIC   SELECT tvid, oem, ROW_NUMBER() OVER (PARTITION BY token ORDER BY joined_date DESC) AS rn
# MAGIC   FROM detection.tv
# MAGIC )
# MAGIC SELECT tv.oem, COUNT(DISTINCT tv.tvid)
# MAGIC FROM detection.viewing_content_firehose vc
# MAGIC JOIN tv_latest tv
# MAGIC   ON tv.tvid = vc.fk_tvid
# MAGIC WHERE vc.session_start >= CURRENT_DATE
# MAGIC   AND tv.rn = 1
# MAGIC   AND vc.fk_zoo_id = 17
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT oem
# MAGIC , COUNT(DISTINCT tvid)
# MAGIC FROM (
# MAGIC   SELECT tvid
# MAGIC   , oem
# MAGIC   , ROW_NUMBER() OVER (PARTITION BY token ORDER BY joined_date DESC) AS rn
# MAGIC   FROM detection.tv
# MAGIC )
# MAGIC WHERE rn = 1
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH stage_ism AS (
# MAGIC   SELECT inscape_station_id, inscape_call_sign, mapped_vendor_station_id, inscape_station_name
# MAGIC     FROM (
# MAGIC         SELECT ism.inscape_station_id
# MAGIC         , ism.inscape_call_sign
# MAGIC         , ism.mapped_vendor_station_id
# MAGIC         , st.inscape_station_name
# MAGIC         , ROW_NUMBER() OVER (PARTITION BY ism.mapped_vendor, ism.mapped_vendor_station_id ORDER BY ism.created_at DESC) AS rn
# MAGIC         FROM stage.detection.inscape_station_map ism
# MAGIC         JOIN stage.detection.epg_station st
# MAGIC           ON st.station_id = ism.mapped_vendor_station_id
# MAGIC         WHERE st.vendor_name = 'TIVO'
# MAGIC           AND ism.mapped_vendor = 'TIVO'
# MAGIC           -- AND (st.attributed = 'TRUE' OR st.ingested = 'TRUE')
# MAGIC           AND st.fk_dma_id = 178
# MAGIC         ) ism
# MAGIC     WHERE ism.rn = 1
# MAGIC )
# MAGIC , prod_ism AS (
# MAGIC   SELECT inscape_station_id, inscape_call_sign, mapped_vendor_station_id, inscape_station_name
# MAGIC     FROM (
# MAGIC         SELECT ism.inscape_station_id
# MAGIC         , ism.inscape_call_sign
# MAGIC         , ism.mapped_vendor_station_id
# MAGIC         , st.inscape_station_name
# MAGIC         , ROW_NUMBER() OVER (PARTITION BY ism.mapped_vendor, ism.mapped_vendor_station_id ORDER BY ism.created_at DESC) AS rn
# MAGIC         FROM prod.detection.inscape_station_map ism
# MAGIC         JOIN prod.detection.epg_station st
# MAGIC           ON st.station_id = ism.mapped_vendor_station_id
# MAGIC         WHERE st.vendor_name = 'TIVO'
# MAGIC           AND ism.mapped_vendor = 'TIVO'
# MAGIC           -- AND (st.attributed = 'TRUE' OR st.ingested = 'TRUE')
# MAGIC           AND st.fk_dma_id = 178
# MAGIC         ) ism
# MAGIC     WHERE ism.rn = 1
# MAGIC )
# MAGIC , stage_data AS (
# MAGIC   SELECT DATE_TRUNC('HOUR', vc.session_start) AS session_hour
# MAGIC   , inscape_call_sign
# MAGIC   , inscape_station_name
# MAGIC   , COUNT(DISTINCT vc.fk_tvid) AS tv_count
# MAGIC   , SUM(vc.session_duration)/3600.0 AS ttl_duration
# MAGIC   , COUNT(*) AS session_count
# MAGIC   FROM stage.detection.viewing_content_firehose vc
# MAGIC   JOIN stage_ism AS ism
# MAGIC     ON ism.mapped_vendor_station_id = vc.fk_station_id
# MAGIC   WHERE vc.session_start >= CURRENT_DATE - 15
# MAGIC   GROUP BY 1, 2, 3
# MAGIC )
# MAGIC , prod_data AS (
# MAGIC   SELECT DATE_TRUNC('HOUR', vc.session_start) AS session_hour
# MAGIC   , inscape_call_sign
# MAGIC   , inscape_station_name
# MAGIC   , COUNT(DISTINCT vc.fk_tvid) AS tv_count
# MAGIC   , SUM(vc.session_duration)/3600.0 AS ttl_duration
# MAGIC   , COUNT(*) AS session_count
# MAGIC   FROM prod.detection.viewing_content_firehose vc
# MAGIC   JOIN prod_ism AS ism
# MAGIC     ON ism.mapped_vendor_station_id = vc.fk_station_id
# MAGIC   WHERE vc.session_start >= CURRENT_DATE - 15
# MAGIC   GROUP BY 1, 2, 3
# MAGIC )
# MAGIC SELECT NVL(p.session_hour, s.session_hour) AS session_hour
# MAGIC , COALESCE(p.inscape_call_sign, s.inscape_call_sign) AS inscape_call_sign
# MAGIC , COALESCE(p.inscape_station_name, s.inscape_station_name) AS inscape_station_name
# MAGIC , NVL(s.tv_count, 0)/GREATEST(p.tv_count*1.0, 1.0) AS tv_count_ratio
# MAGIC , NVL(s.ttl_duration, 0)/GREATEST(p.ttl_duration*1.0, 1.0) AS ttl_duration_ratio
# MAGIC , NVL(s.session_count, 0)/GREATEST(p.session_count*1.0, 1.0) AS session_count_ratio
# MAGIC FROM prod_data AS p
# MAGIC FULL JOIN stage_data AS s
# MAGIC   ON s.session_hour = p.session_hour
# MAGIC  AND p.inscape_call_sign = s.inscape_call_sign
# MAGIC  AND p.inscape_station_name = s.inscape_station_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- WITH st AS (
# MAGIC SELECT * FROM stage.detection.epg_station
# MAGIC WHERE station_num IN (69043291, 10572984927, 69025694, 1768416758, 69024922, 69022667, 69026180, 69037835, 4237665670, 69024440, 69031602, 69025162, 69035069, 6851227720, 1714281768, 7064446889, 10598634196, 69027174, 3967961598, 69031041, 4044136925, 6665558657, 69024217, 69028731, 69043799, 6803633098, 1372289689, 8275761950, 4007799889, 8284846422, 69027199, 4472177519, 4472199536, 8459149555, 7324776649, 5369775529, 5505190447, 12664111047, 12686774993, 4479652705, 7442662356, 12501228419, 8087376850, 6380181757, 10877951705, 6668529655, 69024348, 69029910, 69033128, 69040847, 1526829583, 1527249211, 1527249213, 9990348390, 1760661782, 1760678841, 1760684319, 9341064310, 9362313093, 1734844379, 1734844380, 1734844381, 1714280624, 1714281958, 69033997, 69023326, 69021888, 3944545021, 466402185, 466402786, 12024701474, 1701032442, 4721246126, 11501263749, 7219677714, 3977271632, 3977275317, 4099718777, 6646465164, 9307185560, 13002119846, 69038993, 7504579972, 4225124148, 11712153192, 11712169690, 8031421270, 1422655662, 7702681282, 9833835750, 3977412095, 69028493, 1291247277, 1717081649, 69025632, 426083337, 5987306798, 1752913088, 1777966509, 14794392220, 5136360696, 3675712349, 4255353103, 4255354270, 1416384750, 7626800678, 13465119157, 303971618, 69022684, 598254830, 5960466127, 69022204, 69026400, 69028627, 69041299, 11354332183, 8022500311, 6967104014, 7702674109, 69033425, 6447634362, 1807175698, 1341299427, 7950223215, 7950223216, 12861052017, 12861052262, 14810392594, 8202216994, 69028405, 3224083030, 8722434228, 1714280453, 1750924616, 561995389, 7702718327, 69022691, 1766045127, 14696651930, 1473782267, 7703238919, 9693046289, 13607627723, 5960881069, 1772950370, 3160636242, 4950797453, 7172766787, 7172767603, 9363926726, 10091362757, 1750491013, 69032706, 1742817385, 9530120335, 5159701833, 14295603945, 14295605066, 69023070, 69042698, 1371619074, 69039110, 69045671, 1802962265, 69035044, 69034978, 69024227, 69024506, 1719645794, 11289497308, 69024636, 3163083950, 6052478671, 69045180, 69034521, 1377140016, 69024340, 6107961624, 69023407, 69022698, 69045660, 8925656551, 5521856724, 8287759673, 69026167, 69029495, 10694949527, 12151308172, 69031017, 5960491197, 69037860, 6541691764, 10128255599, 14727654222, 5140006530, 13937191779, 1527249215, 4587979174, 69034263, 69034267, 69034189, 11694541455, 1805308690, 1730236084, 4312031167, 5855421942, 1714279280, 6216212611, 7817110547, 7703232264, 9628016749, 8352230325, 13589595580, 1742817401, 69032456, 69040987, 7823788679, 1612163953, 69034852, 69034849, 69034848, 11192561657, 5824058572, 69034526, 3977449140, 9909116004, 12122745004, 13483809460, 69025696, 4038313917, 958671847, 69033125, 69040852, 1102135849, 1692961952, 13173783405, 4322371729, 69024966, 8263734045, 4150198859, 9656227068, 69033959, 4360652936, 11480239163, 11481079845, 10877937578, 1833200812, 12139458399, 1121533546, 4895458551, 10003195842, 8807659375, 69039135, 69041364, 5136576445, 69045166, 69034824, 15322958362, 69025695, 4061941189, 69035107, 69037518, 10841361888, 10070033815, 69034906, 333024942, 10887631613, 69044616, 4824402811, 69030340, 9611186271, 9611178065, 69045159, 4538326598, 4538326599, 5525636587, 1724783480, 9296022117, 1377141068, 5140014561, 14243290889, 3676106135, 10280469603, 7187572086, 69039181, 69029992, 3203711715, 69039424, 1734856469, 1714279390, 1714281087, 13210915662, 69030971, 69034370, 9450115747, 69038317, 385835916, 10070969581, 6249324847, 1760793828, 4348104534, 69032951, 5921954164, 7172647417, 69027105, 8722435791, 1757929581, 69025640, 69027910, 1492578854, 11855536522, 69025644, 9295852266, 6023749529, 1775407595, 69024419, 1700538940, 1414753311, 1700580772, 12120030436, 12086264950, 7894583394, 69046718, 69029091, 14696607147, 3986221125, 69033971, 69037508, 69037509, 1700550511, 69024946, 69028991, 830926963, 10536532013, 1745309503, 69028651, 69026623, 69021562, 69039615, 1410169373, 4237269392, 6665577720, 69034490, 288479646, 2084748858, 1519203822, 3977452658, 10821548446, 10951864215, 69024986, 69024990, 69024324, 69024845, 8421334622, 69025180, 69025993, 69024987, 69033742, 10544590923, 7703148770, 69022738, 69033945, 8173616242, 1734749462, 1734751821, 1887419932, 10730140747, 10730144189, 1703045301, 8097782739, 69031170, 300184131, 1748267755, 15827291894, 1457538022, 3676163107, 3676165439, 3313700379, 1806935466, 9688861419, 4114184290, 4199895787, 69039519, 4896052190, 69031870, 69025830, 3676170772, 6151172863, 1890105381, 5919005790, 7727227423, 1714280604, 69033131, 1055382996, 69032746, 7247405643, 1387256919, 7709404458, 3238354888, 9990503045, 69039960, 69034442, 7702707317, 69029866, 4940711786, 69033973, 69046722, 5620854684, 8459048307, 69032808, 1956880408, 1956903345, 7703302216, 69029721, 1016815995, 9288606843, 10804684797, 69037766, 8282275856, 14696657797, 6523737549, 6708525201, 1934320984, 1934322204, 7442813539, 11819385584, 69034764, 3206608639, 69022409, 8069930609, 3160621897, 69033436, 1496711904, 5654143000, 5654146873, 6478332657, 15767269128, 8987764890, 6610569204, 69050087, 296204844, 69046507, 3175968463, 69032664, 69040202, 3201955788, 5450156119, 69042746, 69031799, 69033248, 69033249, 8352140041, 4038331684, 13037644421, 69031394, 69025741, 69033797, 69031302, 69023830, 3965198321, 11260451442, 69028531, 8022492616, 69027974, 10514428902, 3676257610, 1069006873, 5525622175, 69029266, 1727483454, 69027984, 1346560359, 69039532, 1239644782, 69027083, 69031577, 10624507412, 3979595185, 69024230, 69022754, 69024806, 69028952, 368523193, 830903359, 9909394199, 69026103, 7702699558, 9763461433, 13937235026, 69024387, 69024383, 69024830, 10021329076, 69022205, 69026330, 69023357, 4552281509, 69037465, 69027877, 69027983, 5836850166, 5836866014, 5836885431, 5836885432, 6318715867, 1440588275, 7702695035, 9638879913, 1730567370, 3586868354, 502387411, 9724092893, 69028470, 69037909, 7444879560, 69023666, 69028963, 10273429639, 69025496, 12426302881, 3676271616, 10724696848, 69029552, 69038234, 1547402244, 69030896, 69030897, 1992041053, 12188027224, 12188027475, 12188028035, 69028912, 69028914, 3978932451, 10545041407, 69033107, 69033110, 10540649488, 11805049319, 69022212, 830873570, 69028920, 679849626, 69028965, 69028967, 830913210, 69041212, 69042674, 69034013, 10541381244, 69025183, 679853976, 830900763, 69028930, 69028931, 367361471, 679852078, 69024999, 69028909, 830883481, 368523731, 10572241820, 7224653027, 12412570888, 1625538810, 3175968464, 69030214, 69030217, 5960370924, 7703109652, 69033833, 7541533581, 11444017841, 11444017843, 11444017844, 11444032676, 5834372168, 12890873040, 3979150754, 4065394438, 868200513, 4591780273, 1654743143, 7702723228, 69039258, 10841361887, 1737301185, 7317664924, 69043824, 6714532474, 69037555, 69037526, 1701354689, 69033292, 10172532399, 15510892272, 3518377355, 5718007512, 5718008414, 8542197191, 4350810656, 69050090, 3678212326, 69025021, 69026664, 6322379089, 69030138, 69022763, 6387013858, 9822652348, 1717081667, 1717081669, 69026802, 1393528453, 1734072157, 4677955620, 5525648747, 69025705, 3302790278, 6991866663, 7294673369, 6597498445, 1727148589, 3430419742, 7508958148, 11636454693, 1516142158, 7269767739, 1386777886, 1746088344, 8427429056, 11225948468, 14696662925, 1457538021, 9834276417, 69029562, 5525482996, 5992310452, 7168412282, 7168413232, 8804822403, 69033715, 69032959, 69022774, 7703116275, 8202181154, 560522393, 6610569203, 4654224407, 4654225669, 4129976141, 6216191638, 8394136692, 3532368636, 69028984, 69028985, 10541123089, 1762749003, 69037927, 888432544, 1291104873, 69039098, 1893126468, 5016103489, 7703341498, 4538327194, 9209054317, 7084193336, 69039175, 69038597, 5654530138, 8267397871, 69031197, 9088385891, 9088385892, 69024683, 5525628370, 4224477154, 6308444615, 562017274, 3487288501, 7816528737, 9322049078, 8097585104, 8097621935, 10623902284, 69025376, 13034012223, 69026185, 1416384752, 69033218, 1034645011, 1700359799, 1700273217, 1700359790, 7695851096, 10549360458, 6774509359, 5696166445, 69028534, 1352035725, 11923857230, 559456031, 7168512666, 7301594689, 69034045, 4483206899, 69039127, 69025374, 1734084580, 14696574015, 3678325926, 10273487709, 69028276, 69025723, 69027440, 1772950456, 69024816, 69028386, 368523547, 830930082, 69024107, 69026119, 69034450, 4150220487, 1714280813, 69024382, 12257829211, 69028371, 7172737390, 7172737391, 3521906928, 10887629048, 7810618972, 11008628275, 69025016, 69033209, 4638340786, 8209173319, 5768442393, 9909114924, 4234000443, 11377275668, 69039123, 6322335221, 1420858228, 7703300096, 9921545708, 1644655148, 5710898406, 69024264, 69045562, 1714279294, 561999237, 69028664, 69025621, 69024532, 69038588, 3486941584, 69021820, 3678351045, 1097736641, 323006750, 5471101775, 69034022, 10877947492, 69028989, 10541347382, 69033470, 890718372, 1395111761, 5960780330, 1702978043, 3678405405, 7751350888, 1956814286, 1711273262, 69034866, 4708020319, 1731286615, 6241996032, 1775403064, 1775404367, 8899423545, 8899424978, 69029502, 11386532277, 69034799, 693638559, 7611369483, 69024760, 69042680, 1698369350, 5732604920, 6496908271, 14696575353, 14422720939, 69026674, 1725767417, 7417535431, 1818598583, 69025716, 69034952, 8281870867, 3236247447, 5750148747, 5750166364, 5836864287, 5024188317, 7703151436, 7728722322, 69030108, 10624464596, 3227864133, 1807072531, 12003194758, 922764259, 8705880598, 1226046150, 7304916288, 12928547978, 69033927, 8304932776, 69024125, 69028996, 1692455911, 718923048, 7095651715, 69034402, 6901883873, 4784058080, 8284845721, 69027020, 14769861953, 69033946, 69033926, 69034981, 69034982, 69029524, 69027770, 6668511525, 1734726035, 6541646858, 13081389534, 9767373738, 1716558644, 69040141, 7195269611, 14874341474, 7898695091, 7898695351, 69034048, 69031689, 795499965, 3221704058, 11879610040, 11486852087, 69032818, 1430812095, 5631878685, 7996830519, 69025707, 5768416065, 11797716193, 9916400029, 69034965, 1796087627, 69031383, 1716558650, 12551682875, 12551684579, 69029666, 4677964592, 1742817400, 69025008, 69039206, 69024235, 5755389791, 8242466977, 1460948432, 8287885200, 69033239, 11772416780, 69024468, 297009582, 7703249986, 9614958761, 3679137172, 10235545918, 1725091256, 1241369629, 1852729393, 13617444318, 14696657537, 1949759867, 9258028467, 7860008170, 69032875, 1938745695, 10150504278, 10544557027, 3586868344, 69023158, 4812460012, 1042866942, 8651477537, 6397735651, 561982800, 7798006049, 6355075665, 69045273, 69029265, 9707762806, 1680748114, 4548529654, 13648505715, 1791339756, 1791340972, 1791341394, 1791343207, 1763292034, 1848206125, 69027272, 10790889154, 12138989997, 69038806, 69033542, 69028196, 10743710066, 1734072159, 1102282107, 1102252527, 5960891360, 69043377, 69039690, 4616417207, 10962419235, 10962422373, 1055382997, 7003276378, 69031224, 14696681175, 7674019842, 9643029201, 69038678, 7703169260, 9250293248, 9250293909, 9250300675, 14789865819, 672004788, 1700075202, 1700081030, 6737855337, 69024586, 69024361, 69029627, 1415084613, 9920377233, 11086432831, 69022469, 332721217, 792070168, 369880490, 990708694, 5333919475, 69040984, 7316951363, 8062989055, 8062989081, 8355506566, 69026632, 69026633, 69026634, 6816930988, 4190725436, 69034727, 69034728, 69034730, 1701235524, 69024245, 4042599681, 15127377913, 7629966215, 69028812, 3187091552, 69034401, 5439627078, 5189086510, 14123807553, 14123930704, 12068757201, 1454747062, 7702910603, 9526097961, 69031038, 7464585041, 8026251877, 69023960, 3672353045, 6023184701, 69022087, 69022471, 6734093375, 6734093376, 6734093379, 7224347670, 69029631, 7693393013, 8079456879, 14696587540, 7674015340, 9529666670, 69041372, 1730851074, 1441800159, 1760799552, 14696572765, 4218123663, 5823252449, 8563834360, 3976893182, 5569533046, 1697612714, 1697616570, 3557132817, 9911752080, 69022839, 69029501, 69027973, 8076108102, 69023201, 10536490614, 8594510202, 69045536, 69045537, 69021191, 69026700, 8072586067, 8638745790, 69030922, 5463797162, 7703333014, 69039721, 312864161, 7064870142, 9680677644, 5364523422, 69037923, 1718810522, 3204408405, 4438888888, 9925428394, 4721235350, 1177699881, 8439504574, 9404436816, 563343396, 69026120, 69038117, 10319189484, 10790798373, 7585050945, 4333945023, 6814280297, 69031016, 69024366, 69027996, 69026631, 4647367460, 7703109210, 8138513151, 8138513156, 3679946411, 14713024504, 69033212, 6496918337, 69039395, 4612352472, 10568530272, 69024448, 1791346881, 3283949015, 69033349, 756581766, 1851019882, 1848486235, 69033117, 69028667, 12064552098, 8426924673, 69023956, 69034211, 7375907924, 69026319, 6285752754, 11684661352, 69041760, 69033710, 5710895697, 69034791, 69034786, 1823857941, 69050066, 69026052, 4240714360, 69044642, 1505064507, 7702994847, 8439818160, 11188655538, 69034382, 966282834, 1302837794, 8165119456, 5936972416, 69025794, 9006794659, 9006794660, 7827401372, 5369832114, 562011807, 69040279, 8040339597, 7703179430, 69026645, 1487983817, 7702734722, 9738703315, 13275168328, 11844093627, 3672362400, 4604919619, 11101919421, 8640956051, 69030153, 69024422, 69023958, 562133982, 3246281400, 7440824790, 5587014462, 69023822, 1752727557, 69033483, 69041046, 8101216116, 8101216563, 14696660619, 7740779056, 8205777127, 69029028, 1488014911, 14258855033, 4359738183, 14990452623, 7468465733, 69024702, 1176750811, 1362892491, 5152209444, 69050067, 8804832822, 8804832823, 69025076, 7270318051, 4246164869, 5624289184, 6208755288, 8394614593, 1750471991, 5874966508, 7438979206, 69039119, 6015423513, 3227986437, 69028412, 10456010592, 69039330, 7703342491, 1727483462, 1778065376, 69025465, 69028903, 10568523000, 12871722201, 69029292, 1390953612, 69026107, 69031196, 1758403645, 7074273853, 7537555140, 69024503, 69025708, 9916755391, 69032891, 69038047, 1867682934, 8164938801, 8164953198, 9260185549, 9260198937, 9260211440, 9260225104, 7737330869, 7737343053, 9295413204, 10644458728, 69043354, 5432379030, 1538493496, 14859375175, 69024375, 7513759022, 12294353006, 69024452, 1793257700, 3158294534, 7753829861, 69023166, 69023167, 69037921, 1702688443, 1517702976, 69031231, 69039182, 324102277, 1511654933, 69025270, 69028906, 830881494, 69025930, 69030406, 69030026, 69027010, 5533065807, 69038850, 69050068, 69032264, 7143967212, 1791393104, 5256524562, 69039269, 4888892002, 4623729324, 6262445657, 7003245857, 69027175, 833515022, 8040343249, 8326679115, 69037648, 1078511710, 69031330, 69039550, 10710061463, 69028916, 69028917, 830891416, 3587395189, 7953301228, 4472585980, 69035111, 4043514805, 7172714758, 7172728323, 7172728588, 7600293098, 69024893, 1149392581, 12861682261, 1705604082, 69026688, 69034154, 69032867, 367362149, 7109527252, 4308063166, 361086234, 4812385733, 69033991, 9508960629, 3569641253, 69040135, 1555475165, 69030001, 1996158668, 5791815181, 69035089, 7831019137, 7612236663, 11386015881, 69023446, 4087019067, 5823689415, 5732838146, 7775754296, 7768558602, 14696600837, 69033460, 1789445566, 9043022539, 4166425745, 4076427951, 69033119, 69025666, 69025667, 7695695599, 14696653194, 69032872, 7969270093, 69031533, 5569708852, 69033120, 69037489, 69037487, 10016980369, 1519203823, 7702919148, 9676929298, 13464928401, 13464930535, 6201212789, 6201217136, 69025664, 7637524545, 7637532736, 7637535579, 69027793, 4472753329, 7702929086, 69033556, 3190516421, 69024434, 69022932, 514526007, 6906547527, 7737750688, 69025736, 1495195618, 69024548, 7349668671, 69025145, 69022943, 7920006039, 7920008218, 3224625262, 69030870, 5413920245, 1718853437, 69043235, 69045143, 69043237, 7737271588, 6366298110, 1555572812, 69027772, 9908199405, 7913354639, 8142545877, 4035521384, 1344293314, 8319974203, 69039347, 9916305139, 12580839596, 69024454, 7329784581, 1780114113, 6589910251, 1495118207, 69038921, 5520678898, 15831459291, 69038618, 5775561768, 657653873, 1748434246, 69025020, 5882581898, 5882582601, 5882585248, 69024621, 8763474341, 13967564544, 14776359155, 69033409, 6714549616, 10804958749, 6186545201, 6186545427, 69028317, 1734813734, 8003647823, 8003647986, 8003648119, 1393528447, 5274059877, 320278555, 14173456569, 8333191404, 69024983, 368523583, 10568640102, 69025697, 1762651104, 3673703239, 69034726, 9920387381, 4712558955, 4608698183, 3227111057, 12407699402, 1778065383, 69024485, 69024487, 69024488, 7615126436, 10541346782, 69024491, 69028680, 14943940937, 69029323, 4166055151, 4175339325, 6390690049, 13415983113, 7615165247, 921165997, 1018606667, 5624287724, 69025521, 69024522, 14660280541, 69038843, 69025516, 783508594, 4479779134, 1007848576, 69034016, 4201826499, 10306993236, 69029256, 1725401851, 6322876581, 9800303526, 9800303527, 1793633120, 3673706497, 4043514804, 603926581, 603927677, 1292565292, 8804847138, 8804847139, 69024562, 69050074, 8793853734, 69030744, 5943030040, 10710041638, 69032878, 69050082, 69042728, 6496916665, 6496917490, 69038858, 8397758120, 770465974, 12953323800, 69023453, 69029878, 4837537546, 8304824878, 69024676, 69026479, 10815161522, 10815600735, 1712020574, 69023879, 69024472, 5699852331, 6816930991, 69033379, 69029698, 69024977, 69025091, 69028727, 368518323, 10541347381, 69024627, 6665626934, 1116798070, 6514190718, 69040121, 2049803284, 4086897693, 3546415806, 69032553, 69030322, 69037940, 7301972263, 4562607390, 12370995670, 8059807455, 8059808294, 69031845, 4844981441, 7869991098, 69032506, 5688495761, 1763615386, 69025427, 69039811, 7683141632, 14696657213, 69022982, 830908537, 69025040, 7783526372, 69028638, 69027189, 6596831724, 7003457353, 69030071, 3679478235, 12923744735, 69024118, 6386526118, 69037713, 9300225046, 8304375046, 13205477559, 7703197633, 69029253, 69039177, 5715084961, 8517601882, 8530903188, 8530920878, 69031388, 7585040850, 15592429292, 69038959, 8304375050, 5208841426, 69038996, 69024502, 69038837, 69028957, 69028960, 830912047, 69031447, 69034811, 69027395, 6493380223, 69027658, 69028631, 69023975, 1495120148, 1204648371, 69025406, 69022993, 69028860, 10578036265, 69025431, 69028879, 830895461, 69028892, 830876719, 10545121154, 69025235, 69028938, 10541442524, 69028873, 830863868, 69025236, 69028862, 69028863, 368523099, 830855530, 1478463293, 69034001, 69034002, 368523260, 69022995, 69028924, 69028925, 830897272, 69033112, 69032917, 10541085988, 69025343, 830937467, 679857005, 69022996, 69028977, 69028982, 11946735828, 379538796, 10572314108, 69022997, 69028949, 830903062, 69022999, 367362610, 10540457566, 69023000, 69032928, 830899570, 69034909, 5837566786, 13279420702, 9500742889, 9500742983, 7513780565, 10410411853, 69023439, 4456991955, 4457001931, 922774890, 69023002, 8885602729, 7212188474, 69025498, 69028994, 11728666967, 69023109, 69031148, 3157799929, 5455484069, 69024809, 1496711894, 1496711900, 562128572, 69027863, 6245747521, 1702913741, 1095189145, 7524856852, 10331012165, 69027549, 8728540639, 69039124, 69025791, 7172752407, 7172752408, 1970638139, 3289000522, 5578499057, 11529129617, 7703342490, 69050070, 8242483128, 8242483650, 69023011, 69028974, 1712020528, 679857745, 3224606254, 6354253655, 69043428, 7488587801, 69028644, 5650485409, 69028669, 7290641286, 7703254300, 8421682958, 1502357981, 69024249, 69034712, 6205046888, 7922738936, 7703228150, 4929947793, 6848685494, 1879228234, 69038989, 69033672, 7438966036, 8988213441, 5759844509, 7513566478, 69031244, 69042695, 7287366744, 69029276, 69024651, 1728656656, 3625146317, 15184614044, 15184615915, 13738619283, 69031354, 69040844, 6887956504, 6887957744, 69029185, 5285958797, 1519203824, 8270572912, 11661631018, 2015510726, 69033113, 69033114, 11202914868, 69023016, 7172481570, 69038849, 8856801232, 69040150, 3280801408, 8822174093, 69039210, 8378532193, 69031730, 69038983, 8093958265, 69029895, 4201900706, 69039600, 4033069194, 12806127282, 4318687293, 14603524284, 16019223552, 3985448687, 3576277589, 7702738914, 9909604956, 9909605456, 69025710, 69031362, 10558077040, 10558077131, 3221236289, 69034023, 5282473851, 69037562, 69033499, 7939207689, 8304375052, 1770470290, 69039134, 5654205407, 9911824201, 69023848, 69027987, 7702761973, 13085081974, 10558242505, 69028467, 1393528449, 9900079251, 3676330087, 12232839838, 8478671806, 1686577924, 6285985075, 7702732546, 10628396466, 69029964, 69022450, 6668519735, 69026914, 11942689997, 11942689998, 8821994785, 8821995027, 8821995276, 8822000430, 69029012, 368524554, 1390740457, 5768437896, 69038450, 69038987, 4633508728, 3676370658, 69039067, 5116522437, 15912030402, 15912228165, 69023550, 69026043, 7794161985, 8157888343, 69023924, 4041260399, 69023035, 69040989, 3909565195, 3518039492, 10101736684, 69039711, 11080148152, 11080148428, 69021982, 562016403, 7808075431, 9731437736, 9731437737, 9731437738, 5525488518, 69027835, 69022429, 12949588666, 4152610703, 4152680284, 6140858992, 562015056, 7312281735, 1780571504, 3983706305, 333593235, 11501466440, 8242543605, 4425717128, 69027180, 69045257, 69022208, 6482408293, 6482408294, 69024618, 69023043, 69024619, 1689092725, 6714221071, 7745974337, 11513214602, 7353781003, 10572209749, 69025344, 368523553, 830898248, 69044449, 9912309281, 922757314, 312269990, 5834845580, 1359814619, 1147571632, 1736649644, 69027333, 968924997, 10820646279, 69025721, 829200243, 1702823017, 7053081265, 69050073, 69026564, 69034831, 5525485828, 69039710, 7079174353, 8763887100, 8763890550, 69024568, 69034719, 679818599, 10679935395, 1775467919, 10331551475, 69024527, 69035032, 7287731580, 7287730353, 1810093020, 1810097513, 8651346093, 10576009647, 11014868242, 69026091, 8352184092, 10414146934, 69026595, 69026596, 69026597, 69025328, 69038925, 3551545260, 10620722344, 10620730285, 69038834, 3140543894, 3227983670, 10887549910, 10887549911, 69029008, 4318272589, 8409664364, 8609712129, 8609713654, 8609717611, 305892560, 69025650, 69033807, 5768442392, 69028372, 12562779033, 7913360874, 8675988454, 5208154427, 69039041, 9822014902, 9822014901, 3590009128, 3590009130, 69034940, 7195278853, 69025197, 69028999, 368518007, 10540609778, 69033427, 1390932010, 10809901134, 10809901545, 1791407781, 6514144883, 10276568863, 11676232824, 69033693, 69028828, 7312947996, 3676382848, 10030634629, 1807007314, 1362466900, 5855299340, 8089696006, 11043620439, 1809866973, 4565829202, 6816927519, 4044136932, 5739936960, 69028854, 69028857, 368523479, 10544536651, 69028694, 1448158534, 69039509, 69033658, 69033661, 10363183350, 69035213, 6376505762, 1697636527, 8563765805, 4472701826, 11251777579, 11251777580, 11251777581, 69046098, 10743710063, 11849647513, 69040157, 69033917, 7723882905, 7693072491, 10541445878, 69033950, 1719026844, 1719043294, 10719797363, 1457912276, 3215712468, 7830978109, 1370205406, 12196514951)
# MAGIC -- )
# MAGIC -- SELECT DATE_TRUNC('HOUR', vc.session_start) AS session_hour
# MAGIC -- SELECT st.station_id
# MAGIC -- SELECT st.fk_dma_id
# MAGIC --   -- , COUNT(DISTINCT vc.fk_tvid) AS tv_count
# MAGIC --   , SUM(vc.session_duration)/3600.0 AS ttl_duration
# MAGIC --   -- , COUNT(*) AS session_count
# MAGIC --   FROM stage.detection.viewing_content_firehose vc
# MAGIC --   JOIN st
# MAGIC --     ON st.station_id = vc.fk_station_id
# MAGIC --   WHERE vc.session_start >= CURRENT_DATE - 15
# MAGIC --   GROUP BY 1
# MAGIC --   ORDER BY 2 DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stage.detection.dma
# MAGIC WHERE dma_id IN (28,
# MAGIC 264,
# MAGIC 305,
# MAGIC 51,
# MAGIC 203,
# MAGIC 199)
