# Databricks notebook source
def report_type_gen(comm_source_type: str):
    if comm_source_type.lower() == 'acr only':
        return 1
    elif comm_source_type.lower() == 'kinarl only':
        return 2
    elif comm_source_type.lower() == 'sslogs only':
        return 3
    elif comm_source_type.lower() == 'acr + kinarl':
        return (1, 2)
    elif comm_source_type.lower() == 'acr + sslogs':
        return (1, 3)
    elif comm_source_type.lower() == 'kinarl + sslogs':
        return (2, 3)
    elif comm_source_type.lower() == 'all':
        return (1, 2, 3)

# COMMAND ----------

client_name = 'simulmedia'
vendor_name = 'TIVO'
golden_table_name = 'dev.detection.viewing_commercials_cfe_golden'
start_time = '2025-02-24 07:00:00'
end_time = '2025-02-25 07:00:00'
comm_source_type = 'all'
new_table_name = f'golden_cure_cfe_merge_modular_{client_name}_{comm_source_type.replace(" ", "_").replace("+", "_")}'

# COMMAND ----------

initial_sql = f"""
DROP TABLE IF EXISTS dev.mohit_gangwani.{new_table_name};
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.{new_table_name} (
    tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.{new_table_name} (
    tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip, input_category, input_device, app_service
)
SELECT DISTINCT mt.tvid
, ''
, mt.zipcode
, mt.dma
, mt.external_id
, mt.mt_start
, mt.session_start
, mt.session_end
, CASE WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN (mt.prev_station_blacklist_clients IS NOT NULL
             AND mt.prev_station_blacklist_clients != '||'
             AND mt.prev_station_blacklist_clients NOT LIKE '%|{client_name}|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
       ELSE CASE WHEN '{vendor_name}' = 'TMS'  THEN mt.tms_prev_episode_id
                 WHEN '{vendor_name}' = 'TIVO' THEN mt.tivo_prev_episode_id END
  END AS prev_episode_id
, CASE WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN (mt.prev_station_blacklist_clients IS NOT NULL
             AND mt.prev_station_blacklist_clients != '||'
             AND mt.prev_station_blacklist_clients NOT LIKE '%|{client_name}|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_prev_show_title
       ELSE CASE WHEN '{vendor_name}' = 'TMS'  THEN COALESCE(mt.tms_prev_show_title, mt.tivo_prev_show_title)
                 WHEN '{vendor_name}' = 'TIVO' THEN  COALESCE(mt.tivo_prev_show_title, mt.tms_prev_show_title) END
  END AS prev_show_title
, mt.prev_ts_start
, mt.prev_ts_end
, CASE WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN (mt.prev_station_blacklist_clients IS NOT NULL
             AND mt.prev_station_blacklist_clients != '||'
             AND mt.prev_station_blacklist_clients NOT LIKE '%|{client_name}|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_prev_callsign
       ELSE CASE WHEN '{vendor_name}' = 'TMS'  THEN COALESCE(mt.tms_prev_callsign, mt.tivo_prev_callsign)
                 WHEN '{vendor_name}' = 'TIVO' THEN COALESCE(mt.tivo_prev_callsign, mt.tms_prev_callsign) END
  END AS prev_channel_callsign
, CASE WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL       
       WHEN (mt.prev_station_blacklist_clients IS NOT NULL
             AND mt.prev_station_blacklist_clients != '||'
             AND mt.prev_station_blacklist_clients NOT LIKE '%|{client_name}|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_prev_network_affiliate
       ELSE CASE WHEN '{vendor_name}' = 'TMS'  THEN COALESCE(mt.tms_prev_network_affiliate, mt.tivo_prev_network_affiliate)
                 WHEN '{vendor_name}' = 'TIVO' THEN COALESCE(mt.tivo_prev_network_affiliate, mt.tms_prev_network_affiliate) END
  END AS prev_network_affiliate
, CASE WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN (mt.next_station_blacklist_clients IS NOT NULL
             AND mt.next_station_blacklist_clients != '||'
             AND mt.next_station_blacklist_clients NOT LIKE '%|{client_name}|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
       ELSE CASE WHEN '{vendor_name}' = 'TMS'  THEN mt.tms_next_episode_id
                 WHEN '{vendor_name}' = 'TIVO' THEN mt.tivo_next_episode_id END
  END AS next_episode_id
, CASE WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN (mt.next_station_blacklist_clients IS NOT NULL
            AND mt.next_station_blacklist_clients != '||'
            AND mt.next_station_blacklist_clients NOT LIKE '%|{client_name}|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_next_show_title
       ELSE CASE WHEN '{vendor_name}' = 'TMS'  THEN COALESCE(mt.tms_next_show_title, mt.tivo_next_show_title)
                 WHEN '{vendor_name}' = 'TIVO' THEN COALESCE(mt.tivo_next_show_title, mt.tms_next_show_title) END
  END AS next_show_title
, mt.next_ts_start
, mt.next_ts_end
, CASE WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN (mt.next_station_blacklist_clients IS NOT NULL
             AND mt.next_station_blacklist_clients != '||'
             AND mt.next_station_blacklist_clients NOT LIKE '%|{client_name}|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_next_callsign
       ELSE CASE WHEN '{vendor_name}' = 'TMS'  THEN COALESCE(mt.tms_next_callsign, mt.tivo_next_callsign)
                 WHEN '{vendor_name}' = 'TIVO' THEN COALESCE(mt.tivo_next_callsign, mt.tms_next_callsign) END
  END AS next_channel_callsign
, CASE WHEN '{client_name}' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
       WHEN (mt.next_station_blacklist_clients IS NOT NULL
             AND mt.next_station_blacklist_clients != '||'
             AND mt.next_station_blacklist_clients NOT LIKE '%|{client_name}|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_next_network_affiliate
       ELSE CASE WHEN '{vendor_name}' = 'TMS'  THEN COALESCE(mt.tms_next_network_affiliate, mt.tivo_next_network_affiliate)
                 WHEN '{vendor_name}' = 'TIVO' THEN COALESCE(mt.tivo_next_network_affiliate, mt.tms_next_network_affiliate) END
  END AS next_network_affiliate
, CASE WHEN '{client_name}' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
       WHEN (mt.prev_station_blacklist_clients IS NOT NULL
             AND mt.prev_station_blacklist_clients != '||'
             AND mt.prev_station_blacklist_clients NOT LIKE '%|{client_name}|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
       ELSE mt.live
  END AS live
, mt.brand_name
, mt.title
, mt.duration
, mt.ip
, mt.input_category
, mt.input_device
, CASE WHEN (mt.appb_clients IS NOT NULL
             AND mt.appb_clients != '||'
             AND mt.appb_clients NOT LIKE '%|{client_name}|%') OR mt.appb_clients <=> '|ALL|' THEN 'OBFUSCATED'
       ELSE mt.app_service
  END AS app_service
FROM {golden_table_name} AS mt
JOIN prod.detection.tv_populations AS tp
  ON mt.fk_tvid = tp.fk_tvid
JOIN prod.detection.populations AS pop
  ON tp.fk_population_id = pop.population_id
  AND LOWER(pop.population_name) = 'opted_in'
WHERE mt.session_start >= '{start_time}'::timestamp
  AND mt.session_start < '{end_time}'::timestamp
  AND CASE WHEN (mt.acrb_clients IS NOT NULL
                 AND mt.acrb_clients != '||'
                 AND mt.acrb_clients NOT LIKE '%|{client_name}|%') OR mt.acrb_clients <=> '|ALL|' THEN FALSE ELSE TRUE END
  AND fk_commercial_source_id IN ({report_type_gen(comm_source_type)})
"""

# COMMAND ----------

print(
    initial_sql.format(
        new_table_name=new_table_name,
        golden_table_name=golden_table_name,
        start_time=start_time,
        end_time=end_time,
        client_name=client_name,
        vendor_name=vendor_name,
        comm_source_type=comm_source_type,
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.golden_cure_cfe_merge_modular_simulmedia_all;
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.golden_cure_cfe_merge_modular_simulmedia_all (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC INSERT INTO dev.mohit_gangwani.golden_cure_cfe_merge_modular_simulmedia_all (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip, input_category, input_device, app_service
# MAGIC )
# MAGIC SELECT DISTINCT mt.tvid
# MAGIC , ''
# MAGIC , mt.zipcode
# MAGIC , mt.dma
# MAGIC , mt.external_id
# MAGIC , mt.mt_start
# MAGIC , mt.session_start
# MAGIC , mt.session_end
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN mt.tms_prev_episode_id
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN mt.tivo_prev_episode_id END
# MAGIC   END AS prev_episode_id
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_prev_show_title
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_prev_show_title, mt.tivo_prev_show_title)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN  COALESCE(mt.tivo_prev_show_title, mt.tms_prev_show_title) END
# MAGIC   END AS prev_show_title
# MAGIC , mt.prev_ts_start
# MAGIC , mt.prev_ts_end
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_prev_callsign
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_prev_callsign, mt.tivo_prev_callsign)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_prev_callsign, mt.tms_prev_callsign) END
# MAGIC   END AS prev_channel_callsign
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL       
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_prev_network_affiliate
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_prev_network_affiliate, mt.tivo_prev_network_affiliate)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_prev_network_affiliate, mt.tms_prev_network_affiliate) END
# MAGIC   END AS prev_network_affiliate
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.next_station_blacklist_clients != '||'
# MAGIC              AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN mt.tms_next_episode_id
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN mt.tivo_next_episode_id END
# MAGIC   END AS next_episode_id
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC             AND mt.next_station_blacklist_clients != '||'
# MAGIC             AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_next_show_title
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_next_show_title, mt.tivo_next_show_title)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_next_show_title, mt.tms_next_show_title) END
# MAGIC   END AS next_show_title
# MAGIC , mt.next_ts_start
# MAGIC , mt.next_ts_end
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.next_station_blacklist_clients != '||'
# MAGIC              AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_next_callsign
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_next_callsign, mt.tivo_next_callsign)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_next_callsign, mt.tms_next_callsign) END
# MAGIC   END AS next_channel_callsign
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.next_station_blacklist_clients != '||'
# MAGIC              AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_next_network_affiliate
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_next_network_affiliate, mt.tivo_next_network_affiliate)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_next_network_affiliate, mt.tms_next_network_affiliate) END
# MAGIC   END AS next_network_affiliate
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        ELSE mt.live
# MAGIC   END AS live
# MAGIC , mt.brand_name
# MAGIC , mt.title
# MAGIC , mt.duration
# MAGIC , mt.ip
# MAGIC , mt.input_category
# MAGIC , mt.input_device
# MAGIC , CASE WHEN (mt.appb_clients IS NOT NULL
# MAGIC              AND mt.appb_clients != '||'
# MAGIC              AND mt.appb_clients NOT LIKE '%|simulmedia|%') OR mt.appb_clients <=> '|ALL|' THEN 'OBFUSCATED'
# MAGIC        ELSE mt.app_service
# MAGIC   END AS app_service
# MAGIC FROM dev.detection.viewing_commercials_cfe_golden AS mt
# MAGIC JOIN prod.detection.tv_populations AS tp
# MAGIC   ON mt.fk_tvid = tp.fk_tvid
# MAGIC JOIN prod.detection.populations AS pop
# MAGIC   ON tp.fk_population_id = pop.population_id
# MAGIC   AND LOWER(pop.population_name) = 'opted_in'
# MAGIC WHERE mt.session_start >= '2025-02-24 07:00:00'::timestamp
# MAGIC   AND mt.session_start < '2025-02-25 07:00:00'::timestamp
# MAGIC   AND CASE WHEN (mt.acrb_clients IS NOT NULL
# MAGIC                  AND mt.acrb_clients != '||'
# MAGIC                  AND mt.acrb_clients NOT LIKE '%|simulmedia|%') OR mt.acrb_clients <=> '|ALL|' THEN FALSE ELSE TRUE END
# MAGIC   AND fk_commercial_source_id IN (1, 2, 3)
# MAGIC   AND MOD(mt.fk_tvid, 100) = 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.golden_cure_cfe_merge_modular_simulmedia_kinarl_only;
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.golden_cure_cfe_merge_modular_simulmedia_kinarl_only (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC INSERT INTO dev.mohit_gangwani.golden_cure_cfe_merge_modular_simulmedia_kinarl_only (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip, input_category, input_device, app_service
# MAGIC )
# MAGIC SELECT DISTINCT mt.tvid
# MAGIC , ''
# MAGIC , mt.zipcode
# MAGIC , mt.dma
# MAGIC , mt.external_id
# MAGIC , mt.mt_start
# MAGIC , mt.session_start
# MAGIC , mt.session_end
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN mt.tms_prev_episode_id
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN mt.tivo_prev_episode_id END
# MAGIC   END AS prev_episode_id
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_prev_show_title
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_prev_show_title, mt.tivo_prev_show_title)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN  COALESCE(mt.tivo_prev_show_title, mt.tms_prev_show_title) END
# MAGIC   END AS prev_show_title
# MAGIC , mt.prev_ts_start
# MAGIC , mt.prev_ts_end
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_prev_callsign
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_prev_callsign, mt.tivo_prev_callsign)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_prev_callsign, mt.tms_prev_callsign) END
# MAGIC   END AS prev_channel_callsign
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL       
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_prev_network_affiliate
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_prev_network_affiliate, mt.tivo_prev_network_affiliate)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_prev_network_affiliate, mt.tms_prev_network_affiliate) END
# MAGIC   END AS prev_network_affiliate
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.next_station_blacklist_clients != '||'
# MAGIC              AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN mt.tms_next_episode_id
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN mt.tivo_next_episode_id END
# MAGIC   END AS next_episode_id
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC             AND mt.next_station_blacklist_clients != '||'
# MAGIC             AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_next_show_title
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_next_show_title, mt.tivo_next_show_title)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_next_show_title, mt.tms_next_show_title) END
# MAGIC   END AS next_show_title
# MAGIC , mt.next_ts_start
# MAGIC , mt.next_ts_end
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.next_station_blacklist_clients != '||'
# MAGIC              AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_next_callsign
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_next_callsign, mt.tivo_next_callsign)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_next_callsign, mt.tms_next_callsign) END
# MAGIC   END AS next_channel_callsign
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.next_station_blacklist_clients != '||'
# MAGIC              AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_next_network_affiliate
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_next_network_affiliate, mt.tivo_next_network_affiliate)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_next_network_affiliate, mt.tms_next_network_affiliate) END
# MAGIC   END AS next_network_affiliate
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        ELSE mt.live
# MAGIC   END AS live
# MAGIC , mt.brand_name
# MAGIC , mt.title
# MAGIC , mt.duration
# MAGIC , mt.ip
# MAGIC , mt.input_category
# MAGIC , mt.input_device
# MAGIC , CASE WHEN (mt.appb_clients IS NOT NULL
# MAGIC              AND mt.appb_clients != '||'
# MAGIC              AND mt.appb_clients NOT LIKE '%|simulmedia|%') OR mt.appb_clients <=> '|ALL|' THEN 'OBFUSCATED'
# MAGIC        ELSE mt.app_service
# MAGIC   END AS app_service
# MAGIC FROM dev.mohit_gangwani.golden_table_test_cfe_merge_modular_test AS mt
# MAGIC JOIN prod.detection.tv_populations AS tp
# MAGIC   ON mt.fk_tvid = tp.fk_tvid
# MAGIC JOIN prod.detection.populations AS pop
# MAGIC   ON tp.fk_population_id = pop.population_id
# MAGIC   AND LOWER(pop.population_name) = 'opted_in'
# MAGIC WHERE mt.session_start >= '2025-01-24 07:00:00'::timestamp
# MAGIC   AND mt.session_start < '2025-01-25 07:00:00'::timestamp
# MAGIC   AND CASE WHEN (mt.acrb_clients IS NOT NULL
# MAGIC                  AND mt.acrb_clients != '||'
# MAGIC                  AND mt.acrb_clients NOT LIKE '%|simulmedia|%') OR mt.acrb_clients <=> '|ALL|' THEN FALSE ELSE TRUE END
# MAGIC   AND fk_commercial_source_id IN (2)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.golden_cure_cfe_merge_modular_simulmedia_sslogs_only;
# MAGIC CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.golden_cure_cfe_merge_modular_simulmedia_sslogs_only (
# MAGIC     tvid string, hash string, zipcode string, dma string, value string, mt_start integer, ts_start timestamp, ts_end timestamp, prev_episode_id string, prev_title string, prev_ts_start timestamp, prev_ts_end timestamp, prev_channel_callsign string, prev_network_affiliate string, next_episode_id string, next_title string, next_ts_start timestamp, next_ts_end timestamp, next_channel_callsign string, next_network_affiliate string, live string, brand_name string, title string, duration integer, ip string, input_category string, input_device string, app_service string
# MAGIC     );
# MAGIC INSERT INTO dev.mohit_gangwani.golden_cure_cfe_merge_modular_simulmedia_sslogs_only (
# MAGIC     tvid, hash, zipcode, dma, value, mt_start, ts_start, ts_end, prev_episode_id, prev_title, prev_ts_start, prev_ts_end, prev_channel_callsign, prev_network_affiliate, next_episode_id, next_title, next_ts_start, next_ts_end, next_channel_callsign, next_network_affiliate, live, brand_name, title, duration, ip, input_category, input_device, app_service
# MAGIC )
# MAGIC SELECT DISTINCT mt.tvid
# MAGIC , ''
# MAGIC , mt.zipcode
# MAGIC , mt.dma
# MAGIC , mt.external_id
# MAGIC , mt.mt_start
# MAGIC , mt.session_start
# MAGIC , mt.session_end
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN mt.tms_prev_episode_id
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN mt.tivo_prev_episode_id END
# MAGIC   END AS prev_episode_id
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_prev_show_title
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_prev_show_title, mt.tivo_prev_show_title)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN  COALESCE(mt.tivo_prev_show_title, mt.tms_prev_show_title) END
# MAGIC   END AS prev_show_title
# MAGIC , mt.prev_ts_start
# MAGIC , mt.prev_ts_end
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_prev_callsign
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_prev_callsign, mt.tivo_prev_callsign)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_prev_callsign, mt.tms_prev_callsign) END
# MAGIC   END AS prev_channel_callsign
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL       
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_prev_network_affiliate
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_prev_network_affiliate, mt.tivo_prev_network_affiliate)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_prev_network_affiliate, mt.tms_prev_network_affiliate) END
# MAGIC   END AS prev_network_affiliate
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.next_station_blacklist_clients != '||'
# MAGIC              AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN mt.tms_next_episode_id
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN mt.tivo_next_episode_id END
# MAGIC   END AS next_episode_id
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC             AND mt.next_station_blacklist_clients != '||'
# MAGIC             AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_next_show_title
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_next_show_title, mt.tivo_next_show_title)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_next_show_title, mt.tms_next_show_title) END
# MAGIC   END AS next_show_title
# MAGIC , mt.next_ts_start
# MAGIC , mt.next_ts_end
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.next_station_blacklist_clients != '||'
# MAGIC              AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_next_callsign
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_next_callsign, mt.tivo_next_callsign)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_next_callsign, mt.tms_next_callsign) END
# MAGIC   END AS next_channel_callsign
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.next_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.next_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.next_station_blacklist_clients != '||'
# MAGIC              AND mt.next_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.next_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        WHEN 'simulmedia' = 'nielsen' THEN mt.tms_next_network_affiliate
# MAGIC        ELSE CASE WHEN 'TIVO' = 'TMS'  THEN COALESCE(mt.tms_next_network_affiliate, mt.tivo_next_network_affiliate)
# MAGIC                  WHEN 'TIVO' = 'TIVO' THEN COALESCE(mt.tivo_next_network_affiliate, mt.tms_next_network_affiliate) END
# MAGIC   END AS next_network_affiliate
# MAGIC , CASE WHEN 'simulmedia' != 'nielsen' AND mt.prev_nielsen_exclusive THEN NULL
# MAGIC        WHEN (mt.prev_station_blacklist_clients IS NOT NULL
# MAGIC              AND mt.prev_station_blacklist_clients != '||'
# MAGIC              AND mt.prev_station_blacklist_clients NOT LIKE '%|simulmedia|%') OR mt.prev_station_blacklist_clients <=> '|ALL|' THEN NULL
# MAGIC        ELSE mt.live
# MAGIC   END AS live
# MAGIC , mt.brand_name
# MAGIC , mt.title
# MAGIC , mt.duration
# MAGIC , mt.ip
# MAGIC , mt.input_category
# MAGIC , mt.input_device
# MAGIC , CASE WHEN (mt.appb_clients IS NOT NULL
# MAGIC              AND mt.appb_clients != '||'
# MAGIC              AND mt.appb_clients NOT LIKE '%|simulmedia|%') OR mt.appb_clients <=> '|ALL|' THEN 'OBFUSCATED'
# MAGIC        ELSE mt.app_service
# MAGIC   END AS app_service
# MAGIC FROM dev.mohit_gangwani.golden_table_test_cfe_merge_modular_test AS mt
# MAGIC JOIN prod.detection.tv_populations AS tp
# MAGIC   ON mt.fk_tvid = tp.fk_tvid
# MAGIC JOIN prod.detection.populations AS pop
# MAGIC   ON tp.fk_population_id = pop.population_id
# MAGIC   AND LOWER(pop.population_name) = 'opted_in'
# MAGIC WHERE mt.session_start >= '2025-01-24 07:00:00'::timestamp
# MAGIC   AND mt.session_start < '2025-01-25 07:00:00'::timestamp
# MAGIC   AND CASE WHEN (mt.acrb_clients IS NOT NULL
# MAGIC                  AND mt.acrb_clients != '||'
# MAGIC                  AND mt.acrb_clients NOT LIKE '%|simulmedia|%') OR mt.acrb_clients <=> '|ALL|' THEN FALSE ELSE TRUE END
# MAGIC   AND fk_commercial_source_id IN (3)
# MAGIC
