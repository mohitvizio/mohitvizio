# Databricks notebook source
schema = 'dev.ashwin'
new_report = 'golden_table_modular_report_all_enabled_qa_deduplicated'
existing_report = 'golden_table_modular_report_all_enabled_prod_deduplicated'
# new_report = 'golden_cure_cfe_merge_modular_simulmedia_kinarl_only'
# existing_report = 'cfe_simulmedia_kinarl_only'
# new_report = 'golden_cure_cfe_merge_modular_simulmedia_sslogs_only'
# existing_report = 'cfe_simulmedia_sslogs_only'

# COMMAND ----------

# spark.sql(f"""
# DELETE FROM {schema}.{new_report} AS ex
# WHERE EXISTS (
#     SELECT *
#      FROM (SELECT tvid, ts_start, ts_end, ip, mt_start, value
#      , ROW_NUMBER() OVER (PARTITION BY tvid, ts_start, ts_end ORDER BY value) AS rn
#      FROM {schema}.{new_report}) AS exs_report
# WHERE exs_report.tvid = ex.tvid
#  AND exs_report.ts_start = ex.ts_start
#  AND exs_report.ts_end = ex.ts_end
#  AND exs_report.ip <=> ex.ip
#  AND exs_report.mt_start <=> ex.mt_start
#  AND exs_report.value <=> ex.value
#  AND exs_report.rn > 1)""")

# COMMAND ----------

# spark.sql(f"""
# DELETE FROM {schema}.{existing_report} AS ex
# WHERE EXISTS (
#     SELECT *
#      FROM (SELECT tvid, ts_start, ts_end, ip, mt_start, value
#      , ROW_NUMBER() OVER (PARTITION BY tvid, ts_start, ts_end ORDER BY value) AS rn
#      FROM {schema}.{existing_report}) AS exs_report
# WHERE exs_report.tvid = ex.tvid
#  AND exs_report.ts_start = ex.ts_start
#  AND exs_report.ts_end = ex.ts_end
#  AND exs_report.ip <=> ex.ip
#  AND exs_report.mt_start <=> ex.mt_start
#  AND exs_report.value <=> ex.value
#  AND exs_report.rn > 1)""")

# COMMAND ----------

count_test = """
(SELECT 'Matching Rows' AS table_name
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end) > 0
)
UNION
(SELECT 'Existing Report' AS table_name
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 WHERE TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end) > 0
)
UNION
(SELECT 'Golden Report' AS table_name
 , COUNT(*) AS session_count
 , COUNT(DISTINCT new_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{new_report} AS new_report
 WHERE TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end) > 0
)
ORDER BY 2, 1;"""

# COMMAND ----------

count_diff = """
WITH exs AS (
    SELECT COUNT(*) AS session_count
    , COUNT(DISTINCT exs_report.tvid) AS total_tvs
    , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
    FROM {schema}.{existing_report} AS exs_report
    WHERE TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end) > 0
)
, new AS (
    SELECT COUNT(*) AS session_count
    , COUNT(DISTINCT new_report.tvid) AS total_tvs
    , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
    FROM {schema}.{new_report} AS new_report
    WHERE TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end) > 0
)
SELECT new.session_count - exs.session_count AS session_count_diff
, new.total_tvs - exs.total_tvs AS total_tvs_diff
, session_count_diff*100.0/exs.session_count AS session_count_diff_pct
, total_tvs_diff*100.0/exs.total_tvs AS total_tvs_diff_pct
FROM exs, new;"""

# COMMAND ----------

null_not_null = """
(SELECT 'Existing Table' AS table_name
 , CASE WHEN exs_report.live IS NULL THEN 'Null Session' ELSE 'Detected Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 GROUP BY 2
)
UNION
(SELECT 'Golden Report' AS table_name
 , CASE WHEN new_report.live IS NULL THEN 'Null Session' ELSE 'Detected Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT new_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{new_report} AS new_report
 GROUP BY 2
)
"""

# COMMAND ----------

null_not_null_diff = """
WITH exs AS (
    SELECT CASE WHEN exs_report.live IS NULL THEN 'Null Session' ELSE 'Detected Session' END AS session_type
    , COUNT(*) AS session_count
    , COUNT(DISTINCT exs_report.tvid) AS total_tvs
    , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
    FROM {schema}.{existing_report} AS exs_report
    GROUP BY 1
)
, new AS (
    SELECT CASE WHEN new_report.live IS NULL THEN 'Null Session' ELSE 'Detected Session' END AS session_type
    , COUNT(*) AS session_count
    , COUNT(DISTINCT new_report.tvid) AS total_tvs
    , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
    FROM {schema}.{new_report} AS new_report
    GROUP BY 1
)
SELECT exs.session_type
, new.session_count - exs.session_count AS session_count_diff
, new.total_tvs - exs.total_tvs AS total_tvs_diff
, session_count_diff*100.0/exs.session_count AS session_count_diff_pct
, total_tvs_diff*100.0/exs.total_tvs AS total_tvs_diff_pct
FROM exs
JOIN new ON exs.session_type = new.session_type
"""

# COMMAND ----------

app_v_linear = """
(SELECT 'Existing Table' AS table_name
 , CASE WHEN exs_report.app_service IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 GROUP BY 2
)
UNION
(SELECT 'Golden Report' AS table_name
 , CASE WHEN new_report.app_service IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT new_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{new_report} AS new_report
 GROUP BY 2
)
"""

# COMMAND ----------

app_v_linear_diff = """
WITH exs AS (SELECT 'Existing Table' AS table_name
 , CASE WHEN exs_report.app_service IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 GROUP BY 2
)
, new AS (SELECT 'Golden Report' AS table_name
 , CASE WHEN new_report.app_service IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT new_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{new_report} AS new_report
 GROUP BY 2
)
SELECT exs.session_type
, new.session_count - exs.session_count AS session_count_diff
, new.total_tvs - exs.total_tvs AS total_tvs_diff
, session_count_diff*100.0/exs.session_count AS session_count_diff_pct
, total_tvs_diff*100.0/exs.total_tvs AS total_tvs_diff_pct
FROM exs
JOIN new ON exs.session_type = new.session_type
"""

# COMMAND ----------

live_v_timeshifted = """
(SELECT 'Existing Table' AS table_name
 , CASE WHEN exs_report.live = 't' THEN 'Live Session'
        WHEN exs_report.live = 'f' THEN 'Timeshifted Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 WHERE exs_report.live IS NOT NULL
 GROUP BY 2
)
UNION
(SELECT 'Golden Report' AS table_name
  , CASE WHEN new_report.live = 't' THEN 'Live Session'
        WHEN new_report.live = 'f' THEN 'Timeshifted Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT new_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{new_report} AS new_report
 WHERE new_report.live IS NOT NULL
 GROUP BY 2
)
"""

# COMMAND ----------

live_v_timeshifted_diff = """
WITH exs AS (SELECT 'Existing Table' AS table_name
 , CASE WHEN exs_report.live = 't' THEN 'Live Session'
        WHEN exs_report.live = 'f' THEN 'Timeshifted Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 WHERE exs_report.live IS NOT NULL
 GROUP BY 2
)
, new AS (SELECT 'Golden Report' AS table_name
  , CASE WHEN new_report.live = 't' THEN 'Live Session'
        WHEN new_report.live = 'f' THEN 'Timeshifted Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT new_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{new_report} AS new_report
 WHERE new_report.live IS NOT NULL
 GROUP BY 2
)
SELECT exs.session_type
, new.session_count - exs.session_count AS session_count_diff
, new.total_tvs - exs.total_tvs AS total_tvs_diff
, session_count_diff*100.0/exs.session_count AS session_count_diff_pct
, total_tvs_diff*100.0/exs.total_tvs AS total_tvs_diff_pct
FROM exs
JOIN new ON exs.session_type = new.session_type
"""

# COMMAND ----------

input_test = """
SELECT CASE WHEN exs_report.input_category <=> new_report.input_category THEN 'Match'
       ELSE 'No Match'
  END AS input_category_match
, CASE WHEN exs_report.input_device <=> new_report.input_device THEN 'Match'
       ELSE 'No Match'
  END AS input_device_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
GROUP BY 1, 2
ORDER BY 1, 2"""

# COMMAND ----------

location_test = """
SELECT CASE WHEN exs_report.zipcode <=> new_report.zipcode THEN 'Match'
       ELSE 'No Match'
  END AS zipcode_match
, CASE WHEN exs_report.dma <=> new_report.dma THEN 'Match'
       ELSE 'No Match'
  END AS dma_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
GROUP BY 1, 2
ORDER BY 1, 2;"""

# COMMAND ----------

prev_next_epid_test = """
SELECT CASE WHEN exs_report.prev_episode_id IS NOT NULL THEN
           CASE WHEN exs_report.prev_episode_id = new_report.prev_episode_id THEN '1 - Match'
                       WHEN exs_report.prev_episode_id != new_report.prev_episode_id THEN '3 - No Match'
                       WHEN NULLIF(new_report.prev_episode_id, '') IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.prev_episode_id IS NULL AND new_report.prev_episode_id IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS prev_episode_id_match
, CASE WHEN exs_report.next_episode_id IS NOT NULL THEN
           CASE WHEN exs_report.next_episode_id = new_report.next_episode_id THEN '1 - Match'
                       WHEN exs_report.next_episode_id != new_report.next_episode_id THEN '3 - No Match'
                       WHEN NULLIF(new_report.next_episode_id, '') IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.next_episode_id IS NULL AND NULLIF(new_report.next_episode_id, '') IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS next_episode_id_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
GROUP BY 1, 2
ORDER BY 1, 2
"""

# COMMAND ----------

next_prev_show_title_match = """
SELECT CASE WHEN exs_report.prev_title  IS NOT NULL THEN
           CASE WHEN LOWER(regexp_replace(exs_report.prev_title, '(\\W+)', '')) = LOWER(regexp_replace(new_report.prev_title, '(\\W+)', '')) THEN '1 - Match'
                       WHEN LOWER(regexp_replace(exs_report.prev_title, '(\\W+)', '')) != LOWER(regexp_replace(new_report.prev_title, '(\\W+)', '')) THEN '3 - No Match'
                       WHEN new_report.prev_title IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.prev_title IS NULL AND new_report.prev_title IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS prev_title_match
, CASE WHEN exs_report.next_title  IS NOT NULL THEN
           CASE WHEN LOWER(regexp_replace(exs_report.next_title, '(\\W+)', '')) = LOWER(regexp_replace(new_report.next_title, '(\\W+)', '')) THEN '1 - Match'
                       WHEN LOWER(regexp_replace(exs_report.next_title, '(\\W+)', '')) != LOWER(regexp_replace(new_report.next_title, '(\\W+)', '')) THEN '3 - No Match'
                       WHEN new_report.next_title IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.next_title IS NULL AND new_report.next_title IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS next_title_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
GROUP BY 1, 2
ORDER BY 1, 2
"""

# COMMAND ----------

next_prev_channel_callsign_match = """
SELECT CASE WHEN exs_report.prev_channel_callsign  IS NOT NULL THEN
           CASE WHEN exs_report.prev_channel_callsign = new_report.prev_channel_callsign THEN '1 - Match'
                       WHEN exs_report.prev_channel_callsign != new_report.prev_channel_callsign THEN '3 - No Match'
                       WHEN new_report.prev_channel_callsign IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.prev_channel_callsign IS NULL AND new_report.prev_channel_callsign IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS prev_channel_callsign_match
, CASE WHEN exs_report.next_channel_callsign  IS NOT NULL THEN
           CASE WHEN exs_report.next_channel_callsign = new_report.next_channel_callsign THEN '1 - Match'
                       WHEN exs_report.next_channel_callsign != new_report.next_channel_callsign THEN '3 - No Match'
                       WHEN new_report.next_channel_callsign IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.next_channel_callsign IS NULL AND new_report.next_channel_callsign IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS next_channel_callsign_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs  
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
GROUP BY 1, 2
ORDER BY 1, 2
"""

# COMMAND ----------

next_prev_network_affiliate_match = """
SELECT CASE WHEN exs_report.prev_network_affiliate  IS NOT NULL THEN
           CASE WHEN exs_report.prev_network_affiliate = new_report.prev_network_affiliate THEN '1 - Match'
                       WHEN exs_report.prev_network_affiliate != new_report.prev_network_affiliate THEN '3 - No Match'
                       WHEN new_report.prev_network_affiliate IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.prev_network_affiliate IS NULL AND new_report.prev_network_affiliate IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS prev_network_affiliate_match
, CASE WHEN exs_report.next_network_affiliate  IS NOT NULL THEN
           CASE WHEN exs_report.next_network_affiliate = new_report.next_network_affiliate THEN '1 - Match'
                       WHEN exs_report.next_network_affiliate != new_report.next_network_affiliate THEN '3 - No Match'
                       WHEN new_report.next_network_affiliate IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.next_network_affiliate IS NULL AND new_report.next_network_affiliate IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS next_network_affiliate_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
GROUP BY 1, 2
ORDER BY 1, 2
"""

# COMMAND ----------

comm_metadata_match = """
SELECT CASE WHEN LOWER(regexp_replace(exs_report.brand_name, '(\\W+)', '')) <=> LOWER(regexp_replace(new_report.brand_name, '(\\W+)', '')) THEN 'Match'
       ELSE 'No Match'
  END AS brand_name_match
, CASE WHEN LOWER(regexp_replace(exs_report.title, '(\\W+)', '')) <=> LOWER(regexp_replace(new_report.title, '(\\W+)', '')) THEN 'Match'
       ELSE 'No Match'
  END AS ad_title_match
, CASE WHEN exs_report.duration <=> new_report.duration THEN 'Match'
       ELSE 'No Match'
  END AS ad_duration_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
"""

# COMMAND ----------

live_app_service_matach = """
SELECT CASE WHEN exs_report.app_service  IS NOT NULL THEN
           CASE WHEN exs_report.app_service = new_report.app_service THEN '1 - Match'
                       WHEN exs_report.app_service != new_report.app_service THEN '3 - No Match'
                       WHEN new_report.app_service IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.app_service IS NULL AND new_report.app_service IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS app_service_match
, CASE WHEN exs_report.live  IS NOT NULL THEN
           CASE WHEN exs_report.live = new_report.live THEN '1 - Match'
                       WHEN exs_report.live != new_report.live THEN '3 - No Match'
                       WHEN new_report.live IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.live IS NULL AND new_report.live IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS live_match
, COUNT(*) AS session_count
, COUNT(DISTINCT exs_report.tvid) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
GROUP BY 1, 2
ORDER BY 1, 2;
"""

# COMMAND ----------

spark.sql(count_test.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(count_diff.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(null_not_null.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(null_not_null_diff.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(app_v_linear.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(app_v_linear_diff.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(live_v_timeshifted.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(live_v_timeshifted_diff.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(input_test.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(location_test.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(prev_next_epid_test.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(next_prev_channel_callsign_match.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(next_prev_network_affiliate_match.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(next_prev_show_title_match.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(comm_metadata_match.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(live_app_service_matach.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

print(count_test.format(schema=schema, existing_report=existing_report, new_report=new_report))

# COMMAND ----------

spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND NVL(exs_report.input_device, '') != NVL(new_report.input_device, '')
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
, exs_report.input_device
, 'Existing Report' AS report_name
 FROM {schema}.{existing_report} AS exs_report
 JOIN mm
   ON exs_report.tvid = mm.tvid
 AND exs_report.ts_start = mm.ts_start
 AND exs_report.ts_end = mm.ts_end
 AND exs_report.ip <=> mm.ip
 AND exs_report.mt_start <=> mm.mt_start)
UNION
 (SELECT new_report.tvid
, new_report.ts_start
, new_report.ts_end
, new_report.ip
, new_report.mt_start
, new_report.input_device
, 'New Report' AS report_name
 FROM {schema}.{new_report} AS new_report
 JOIN mm
  ON new_report.tvid = mm.tvid
 AND new_report.ts_start = mm.ts_start
 AND new_report.ts_end = mm.ts_end
 AND new_report.ip <=> mm.ip
 AND new_report.mt_start <=> mm.mt_start)
ORDER BY tvid, ts_start, report_name
LIMIT 1000""").display()

# COMMAND ----------

spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND NVL(exs_report.prev_network_affiliate, '') != NVL(new_report.prev_network_affiliate, '')
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
, exs_report.prev_network_affiliate
, 'Existing Report' AS report_name
 FROM {schema}.{existing_report} AS exs_report
 JOIN mm
   ON exs_report.tvid = mm.tvid
 AND exs_report.ts_start = mm.ts_start
 AND exs_report.ts_end = mm.ts_end
 AND exs_report.ip <=> mm.ip
 AND exs_report.mt_start <=> mm.mt_start)
UNION
 (SELECT new_report.tvid
, new_report.ts_start
, new_report.ts_end
, new_report.ip
, new_report.mt_start
, new_report.prev_network_affiliate
, 'New Report' AS report_name
 FROM {schema}.{new_report} AS new_report
 JOIN mm
  ON new_report.tvid = mm.tvid
 AND new_report.ts_start = mm.ts_start
 AND new_report.ts_end = mm.ts_end
 AND new_report.ip <=> mm.ip
 AND new_report.mt_start <=> mm.mt_start)
ORDER BY tvid, ts_start, report_name
LIMIT 1000""").display()

# COMMAND ----------

spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND NVL(LOWER(regexp_replace(exs_report.prev_title, '(\\W+)', '')),'') != NVL(LOWER(regexp_replace(new_report.prev_title, '(\\W+)', '')),'')
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.prev_episode_id
, exs_report.prev_title
, 'Existing Report' AS report_name
 FROM {schema}.{existing_report} AS exs_report
 JOIN mm
   ON exs_report.tvid = mm.tvid
 AND exs_report.ts_start = mm.ts_start
 AND exs_report.ts_end = mm.ts_end
 AND exs_report.ip <=> mm.ip
 AND exs_report.mt_start <=> mm.mt_start)
UNION
 (SELECT new_report.tvid
, new_report.ts_start
, new_report.ts_end
, new_report.prev_episode_id
, new_report.prev_title
, 'New Report' AS report_name
 FROM {schema}.{new_report} AS new_report
 JOIN mm
  ON new_report.tvid = mm.tvid
 AND new_report.ts_start = mm.ts_start
 AND new_report.ts_end = mm.ts_end
 AND new_report.ip <=> mm.ip
 AND new_report.mt_start <=> mm.mt_start)
ORDER BY tvid, ts_start, report_name
LIMIT 10000""").display()

# COMMAND ----------

spark.sql(f"""
SELECT exs_report.prev_title AS prod_title
, new_report.prev_title      AS golden_report_title
, exs_report.prev_episode_id <=> new_report.prev_episode_id AS prev_epid_match
, exs_report.app_service
, COUNT(*)
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND TRIM(NVL(LOWER(regexp_replace(exs_report.prev_title, '(\\W+)', '')),'')) != TRIM(NVL(LOWER(regexp_replace(new_report.prev_title, '(\\W+)', '')),''))
 GROUP BY 1, 2, 3,4
 ORDER BY 5 DESC
LIMIT 1000""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT LOWER(regexp_replace('The Key Bridge Disaster: Reflect Recover Rebuilt', '(\\W+)', ''))
# MAGIC UNION 
# MAGIC SELECT LOWER(regexp_replace('The Key Bridge Disaster: Reflect Recover Rebuild', '(\\W+)', '')) 

# COMMAND ----------

spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND NVL(exs_report.value, '') != NVL(new_report.value, '')
 AND NVL(exs_report.title, '') != NVL(new_report.title, '')
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.value AS external_id
-- , exs_report.ip
-- , exs_report.mt_start
, exs_report.title
, 'Existing Report' AS report_name
 FROM {schema}.{existing_report} AS exs_report
 JOIN mm
   ON exs_report.tvid = mm.tvid
 AND exs_report.ts_start = mm.ts_start
 AND exs_report.ts_end = mm.ts_end
 AND exs_report.ip <=> mm.ip
 AND exs_report.mt_start <=> mm.mt_start)
UNION
 (SELECT new_report.tvid
, new_report.ts_start
, new_report.ts_end
, new_report.value AS external_id
-- , new_report.ip
-- , new_report.mt_start
, new_report.title
, 'New Report' AS report_name
 FROM {schema}.{new_report} AS new_report
 JOIN mm
  ON new_report.tvid = mm.tvid
 AND new_report.ts_start = mm.ts_start
 AND new_report.ts_end = mm.ts_end
 AND new_report.ip <=> mm.ip
 AND new_report.mt_start <=> mm.mt_start)
ORDER BY tvid, ts_start, report_name
LIMIT 1000""").display()

# COMMAND ----------

spark.sql(f"""
SELECT new_report.value AS dev_external_id
, exs_report.value AS prod_external_id
, new_report.value = exs_report.value AS external_id_match
, new_report.title AS dev_title 
, exs_report.title AS prod_title
, COUNT(*)
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND NVL(exs_report.title, '') != NVL(new_report.title, '')
 GROUP BY 1, 2, 3, 4, 5
 ORDER BY 6 DESC
""").display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.debug_comm_metadata_new_comm_ext;
# MAGIC -- CREATE TABLE dev.mohit_gangwani.debug_comm_metadata_new_comm_ext AS
# MAGIC -- WITH xyz AS (
# MAGIC -- WITH clients_table_to_join AS (
# MAGIC --   SELECT *
# MAGIC --   FROM dev.detection.clients
# MAGIC --   WHERE client_name IN ('kinetiq', 'SpringServe-Prod')
# MAGIC -- )
# MAGIC -- , spring_serve_ads AS (
# MAGIC --     WITH fileingest AS (
# MAGIC --       SELECT ssm.reportingid AS ssl_id
# MAGIC --         , ssm.brand AS brand_name
# MAGIC --         , ssm.advertiser AS advertiser
# MAGIC --         , CASE WHEN ssm.contenttitle IN ('SpringServe commercial', '[TBD]') THEN NULL ELSE ssm.contenttitle END AS title
# MAGIC --         , ssm.content_duration AS duration
# MAGIC --         , 1 AS rn
# MAGIC --         FROM dev.public.fileingest_cidmap ssm
# MAGIC --         WHERE client_id = 'SpringServe-Prod'
# MAGIC --           AND brand NOT IN ('SpringServe', '-', '_', '[TBD]')
# MAGIC --         GROUP BY 1,2,3,4,5
# MAGIC --     )
# MAGIC --     , mturk AS (
# MAGIC --       SELECT ssm.vast_hash AS ssl_id
# MAGIC --         , ssm.advertiser AS brand_name
# MAGIC --         , ssm.advertiser AS advertiser
# MAGIC --         , ssm.brand AS title
# MAGIC --         , ssm.duration
# MAGIC --         , 2 AS rn
# MAGIC --         FROM dev.public.springserve_metadata ssm
# MAGIC --         WHERE ssm.advertiser NOT IN ('', 'SpringServe', '-', '_', '[TBD]')
# MAGIC --         GROUP BY 1,2,3,4,5
# MAGIC --     )
# MAGIC --     SELECT ssl_id, brand_name, advertiser, title, duration
# MAGIC --     FROM (
# MAGIC --       SELECT *
# MAGIC --       , ROW_NUMBER() OVER (PARTITION BY ssl_id ORDER BY rn) AS nrn
# MAGIC --       FROM (
# MAGIC --         SELECT * FROM fileingest
# MAGIC --         UNION
# MAGIC --         SELECT * FROM mturk
# MAGIC --       ) a
# MAGIC --     ) a
# MAGIC --     WHERE nrn = 1
# MAGIC --   )
# MAGIC --   SELECT external_id, brand_name, title, duration
# MAGIC --   FROM (
# MAGIC --     SELECT external_id
# MAGIC --     , brand_name
# MAGIC --     , title
# MAGIC --     , duration
# MAGIC --     , ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY rn) AS new_rn
# MAGIC --     FROM (
# MAGIC --       SELECT external_id, brand_name, title, duration, 0 as rn
# MAGIC --       FROM (
# MAGIC --         SELECT *
# MAGIC --         , ROW_NUMBER() OVER (PARTITION BY m.external_id ORDER BY if(m.source = 'Ingested',0,1),m.fk_commercial_id DESC) AS rk
# MAGIC --         FROM dev.detection.commercial_id_external_firehose AS m
# MAGIC --         JOIN clients_table_to_join cl
# MAGIC --           ON m.fk_client_id = cl.client_id
# MAGIC --         WHERE m.brand_name != 'SpringServe'
# MAGIC --           AND m.brand_name IS NOT NULL
# MAGIC --       )
# MAGIC --       WHERE rk = 1
# MAGIC --       UNION ALL
# MAGIC --       SELECT ssl_id, brand_name, title, duration, 1 AS rn
# MAGIC --       FROM spring_serve_ads
# MAGIC --     ) a
# MAGIC --   ) 
# MAGIC --   WHERE new_rn = 1)
# MAGIC --   SELECT * FROM xyz
# MAGIC --   WHERE external_id = '615b6cb9080863da0d13e7a4a109ec74';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.debug_comm_metadata_existing_comm_ext;
# MAGIC CREATE TABLE dev.mohit_gangwani.debug_comm_metadata_existing_comm_ext AS
# MAGIC WITH clients_table_to_join AS (
# MAGIC   SELECT *
# MAGIC   FROM prod.detection.clients
# MAGIC   WHERE client_name IN ('kinetiq', 'SpringServe-Prod')
# MAGIC )
# MAGIC , spring_serve_ads AS (
# MAGIC   WITH fileingest AS (
# MAGIC     SELECT ssm.reportingid AS ssl_id
# MAGIC       , ssm.brand AS brand_name
# MAGIC       , ssm.advertiser AS advertiser
# MAGIC       , CASE WHEN ssm.contenttitle IN ('SpringServe commercial', '[TBD]') THEN NULL ELSE ssm.contenttitle END AS title
# MAGIC       , ssm.content_duration AS duration
# MAGIC       , 1 AS rn
# MAGIC       FROM prod.public.fileingest_cidmap ssm
# MAGIC       WHERE client_id = 'SpringServe-Prod'
# MAGIC         AND brand NOT IN ('SpringServe', '-', '_', '[TBD]')
# MAGIC       GROUP BY 1,2,3,4,5
# MAGIC   )
# MAGIC   , mturk AS (
# MAGIC     SELECT ssm.vast_hash AS ssl_id
# MAGIC       , ssm.advertiser AS brand_name
# MAGIC       , ssm.advertiser AS advertiser
# MAGIC       , ssm.brand AS title
# MAGIC       , ssm.duration
# MAGIC       , 2 AS rn
# MAGIC       FROM prod.public.springserve_metadata ssm
# MAGIC       WHERE ssm.advertiser NOT IN ('', 'SpringServe', '-', '_', '[TBD]')
# MAGIC       GROUP BY 1,2,3,4,5
# MAGIC   )
# MAGIC   SELECT ssl_id, brand_name, advertiser, title, duration
# MAGIC   FROM (
# MAGIC     SELECT *
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY ssl_id ORDER BY rn) AS nrn
# MAGIC     FROM (
# MAGIC       SELECT * FROM fileingest
# MAGIC       UNION
# MAGIC       SELECT * FROM mturk
# MAGIC     ) a
# MAGIC   ) a
# MAGIC   WHERE nrn = 1
# MAGIC )
# MAGIC SELECT external_id, brand_name, title, duration
# MAGIC FROM (
# MAGIC SELECT external_id, brand_name, title, duration
# MAGIC , ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY rn) AS new_rn
# MAGIC FROM (
# MAGIC     SELECT external_id,brand_name,title,duration,0 as rn
# MAGIC     FROM (
# MAGIC         SELECT *, ROW_NUMBER() OVER (PARTITION BY m.external_id ORDER BY if(m.source = 'Ingested',0,1),m.fk_commercial_id DESC) AS rk   
# MAGIC         FROM prod.detection.commercial_id_external_firehose AS m
# MAGIC         JOIN clients_table_to_join cl
# MAGIC             ON m.fk_client_id = cl.client_id
# MAGIC         WHERE m.brand_name != 'SpringServe'
# MAGIC             AND m.brand_name IS NOT NULL
# MAGIC         ) 
# MAGIC     WHERE rk = 1
# MAGIC     UNION ALL
# MAGIC     SELECT ssl_id, brand_name, title, duration, 1 AS rn 
# MAGIC     FROM spring_serve_ads
# MAGIC     ) a
# MAGIC ) m
# MAGIC WHERE m.new_rn = 1;

# COMMAND ----------

spark.sql(f"""
SELECT new_report.value AS dev_external_id
, exs_report.value AS prod_external_id
, new_report.value = exs_report.value AS external_id_match
, new_report.title AS dev_title
, exs_report.title AS prod_title
, nr.title AS title_if_updated
, COUNT(*)
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND NVL(exs_report.title, '') != NVL(new_report.title, '')
 JOIN dev.mohit_gangwani.debug_comm_metadata_existing_comm_ext nr
   ON exs_report.value = nr.external_id
 GROUP BY 1, 2, 3, 4, 5,6
 ORDER BY 7 DESC
""").display()

# COMMAND ----------

spark.sql(f"""
SELECT new_report.value AS dev_external_id
, exs_report.value AS prod_external_id
, new_report.value = exs_report.value AS external_id_match
, new_report.brand_name AS dev_brand
, exs_report.brand_name AS prod_brand
, nr.brand_name AS brand_if_updated
, COUNT(*)
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND NVL(exs_report.brand_name, '') != NVL(new_report.brand_name, '')
 JOIN dev.mohit_gangwani.debug_comm_metadata_existing_comm_ext nr
   ON exs_report.value = nr.external_id
 GROUP BY 1, 2, 3, 4, 5,6
 ORDER BY 7 DESC
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT prod.title AS prod_title, dev.title AS dev_title, COUNT(*)
# MAGIC FROM dev.mohit_gangwani.debug_comm_metadata_new_comm_ext AS dev
# MAGIC JOIN dev.mohit_gangwani.debug_comm_metadata_existing_comm_ext AS prod
# MAGIC   ON dev.external_id = prod.external_id
# MAGIC  AND NVL(dev.title, '') != NVL(prod.title, '')
# MAGIC -- WHERE dev.external_id = '87c12bf0d78c0647291f69bf6997e21c'
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 3 DESC
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT prod.brand_name AS prod_brand_name, dev.brand_name AS dev_brand_name, COUNT(*)
# MAGIC FROM dev.mohit_gangwani.debug_comm_metadata_new_comm_ext AS dev
# MAGIC JOIN dev.mohit_gangwani.debug_comm_metadata_existing_comm_ext AS prod
# MAGIC   ON dev.external_id = prod.external_id
# MAGIC  AND NVL(dev.brand_name, '') != NVL(prod.brand_name, '')
# MAGIC -- WHERE dev.external_id = '87c12bf0d78c0647291f69bf6997e21c'
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 3 DESC
# MAGIC LIMIT 1000

# COMMAND ----------

spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND NVL(exs_report.prev_network_affiliate, '') != NVL(new_report.prev_network_affiliate, '')
 )
--  SELECT gt.*
SELECT gt.tivo_prev_network_affiliate, COUNT(*)
 FROM dev.detection.viewing_commercials_cfe_golden AS gt
 JOIN mm
   ON gt.tvid = mm.tvid
 AND gt.session_start = mm.ts_start
 AND gt.session_end = mm.ts_end
 AND gt.ip <=> mm.ip
 AND gt.mt_start <=> mm.mt_start
WHERE gt.session_start >= '2025-03-18T07:00:00'
  AND gt.session_start < '2025-03-18T08:00:00'
  AND gt.session_start_hour = '2025-03-18 07:00:00'
GROUP BY 1
-- ORDER BY tvid, session_start
-- LIMIT 1000
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.detection.vizio_epg_station
# MAGIC WHERE station_id IN (3257112237,3257112240,4290550957,4172895320,9898989898)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.detection.vizio_epg_station
# MAGIC WHERE station_id IN (3257112237,3257112240,4290550957,4172895320,9898989898)

# COMMAND ----------


spark.sql(f"""
          SELECT DATE_TRUNC('HOUR', gt.session_start), gt.live, COUNT(*)
-- SELECT gt.fk_tvid, gt.session_start, gt.session_end, gt.external_id, gt.app_service, gt.live, gt.prev_ts_start, gt.prev_ts_end
FROM dev.detection.viewing_commercials_cfe_golden gt
-- JOIN {schema}.{new_report} AS new_report
--   ON new_report.tvid = SPLIT_PART(gt.fk_tvid, '_', 1)
--  AND new_report.ts_start = gt.session_start
--  AND new_report.ts_end = gt.session_end
--  AND new_report.value = gt.external_id
WHERE gt.session_start >= '2025-03-01 00:00:00'
  AND gt.session_end < '2025-03-04 00:00:00'
--   AND new_report.live IS NULL
  AND gt.fk_commercial_source_id = 2
  GROUP BY 1, 2
-- ORDER BY 1, 2, 3
LIMIT 1000
""").display()

# COMMAND ----------


spark.sql(f"""
          SELECT DATE_TRUNC('HOUR', gt.session_start), gt.live, COUNT(*)
-- SELECT gt.fk_tvid, gt.session_start, gt.session_end, gt.external_id, gt.app_service, gt.live, gt.prev_ts_start, gt.prev_ts_end
FROM dev.detection.viewing_commercials_cfe_golden gt
-- JOIN {schema}.{new_report} AS new_report
--   ON new_report.tvid = SPLIT_PART(gt.fk_tvid, '_', 1)
--  AND new_report.ts_start = gt.session_start
--  AND new_report.ts_end = gt.session_end
--  AND new_report.value = gt.external_id
WHERE gt.session_start >= '2025-03-01 00:00:00'
  AND gt.session_end < '2025-03-04 00:00:00'
--   AND new_report.live IS NULL
  AND gt.fk_commercial_source_id = 2
  GROUP BY 1, 2
-- ORDER BY 1, 2, 3
LIMIT 1000
""").display()

# COMMAND ----------


spark.sql(f"""
-- SELECT c.fk_tvid, c.session_start, c.session_end, c.external_id
SELECT DATE_TRUNC('HOUR', c.session_start)
, CASE WHEN (cl2.client_id is not NULL) THEN NULL
       WHEN tvis.category = 'APPS' and tis.app_name = 'OBFUSCATED' AND c.prev_vizio_epg_station IS NOT NULL THEN 't'
        ELSE CASE WHEN prev_content.is_live = TRUE THEN 't' WHEN prev_content.is_live = FALSE THEN 'f' ELSE NULL END
    END AS is_live
, COUNT(*)
FROM prod.detection.viewing_commercials_firehose_dedup_cfe_merge c
JOIN prod.detection.tv_input_stats_firehose tvis
  ON c.session_start >= tvis.create_timestamp
 AND c.session_start < tvis.next_create_timestamp
 AND tvis.create_timestamp <= '2025-04-02T00:00:00'::timestamp
 AND tvis.next_create_timestamp >= '2025-04-01T00:00:00'::timestamp
 AND c.fk_tvid = tvis.fk_tvid
 AND c.fk_input_source_id = tvis.fk_input_source_id
LEFT OUTER JOIN prod.detection.tv_inputsource tis    
  ON c.session_start >=  (tis.create_timestamp::double)::timestamp
 AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
 AND tis.create_timestamp <= ('2025-04-02T00:00:00'::timestamp::double)::timestamp
 AND tis.next_create_timestamp >= ('2025-04-01T00:00:00'::timestamp::double)::timestamp
 AND c.fk_tvid = tis.fk_tvid 
 AND c.fk_input_source_id = tis.fk_input_source_id
LEFT OUTER JOIN prod.detection.viewing_content_firehose AS prev_content
  ON  prev_content.session_start >= '2025-04-01T00:00:00'::timestamp
 AND prev_content.session_start < '2025-04-02T00:00:00'::timestamp
 AND c.fk_tvid = prev_content.fk_tvid
 AND prev_content.session_start = c.prev_session_start
LEFT OUTER JOIN prod.detection.content_id_external_firehose AS m_filter 
  ON m_filter.fk_content_id = prev_content.fk_content_id
LEFT OUTER JOIN prod.detection.clients cl2
  ON m_filter.fk_client_id = cl2.client_id
 AND cl2.client_name NOT IN ('kinetiq', 'SpringServe-Prod')
-- JOIN {schema}.{new_report} AS new_report
--   ON new_report.tvid = SPLIT_PART(c.fk_tvid, '_', 1)
--  AND new_report.ts_start = c.session_start
--  AND new_report.ts_end = c.session_end
--  AND new_report.value = c.external_id
WHERE c.session_start >= '2025-04-01 00:00:00'
  AND c.session_end < '2025-04-02 00:00:00'
--   AND new_report.live IS NULL
  AND c.fk_commercial_source_id = 2
GROUP BY 1, 2
-- ORDER BY 1, 2, 3
-- LIMIT 100
""").display()

# COMMAND ----------


