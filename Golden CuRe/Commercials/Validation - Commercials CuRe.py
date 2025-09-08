# Databricks notebook source
schema = 'dev.mohit_gangwani'
new_report = 'golden_all_comm_ispot_tivo_2025_06_17_15'
existing_report = 'existing_all_comm_ispot_tivo_2025_06_17_15'
vendor_name = 'TIVO'
suff = '_tms' if vendor_name == 'TMS' else ''

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
 , CASE WHEN exs_report.app_service{suff} IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 GROUP BY 2
)
UNION
(SELECT 'Golden Report' AS table_name
 , CASE WHEN new_report.app_service{suff} IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
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
 , CASE WHEN exs_report.app_service{suff} IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 GROUP BY 2
)
, new AS (SELECT 'Golden Report' AS table_name
 , CASE WHEN new_report.app_service{suff} IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
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
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
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
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
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
SELECT CASE WHEN exs_report.prev_episode_id{suff} IS NOT NULL THEN
           CASE WHEN exs_report.prev_episode_id{suff} = new_report.prev_episode_id{suff} THEN '1 - Match'
                       WHEN exs_report.prev_episode_id{suff} != new_report.prev_episode_id{suff} THEN '3 - No Match'
                       WHEN NULLIF(new_report.prev_episode_id{suff}, '') IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.prev_episode_id{suff} IS NULL AND new_report.prev_episode_id{suff} IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS prev_episode_id_match
, CASE WHEN exs_report.next_episode_id{suff} IS NOT NULL THEN
           CASE WHEN exs_report.next_episode_id{suff} = new_report.next_episode_id{suff} THEN '1 - Match'
                       WHEN exs_report.next_episode_id{suff} != new_report.next_episode_id{suff} THEN '3 - No Match'
                       WHEN NULLIF(new_report.next_episode_id{suff}, '') IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.next_episode_id{suff} IS NULL AND NULLIF(new_report.next_episode_id{suff}, '') IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS next_episode_id_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
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
SELECT CASE WHEN exs_report.prev_title{suff} IS NOT NULL THEN
           CASE WHEN LOWER(regexp_replace(exs_report.prev_title{suff}, '[^a-zA-Z0-9]', '')) = LOWER(regexp_replace(new_report.prev_title{suff}, '[^a-zA-Z0-9]', '')) THEN '1 - Match'
                       WHEN LOWER(regexp_replace(exs_report.prev_title{suff}, '[^a-zA-Z0-9]', '')) != LOWER(regexp_replace(new_report.prev_title{suff}, '[^a-zA-Z0-9]', '')) THEN '3 - No Match'
                       WHEN new_report.prev_title{suff} IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.prev_title{suff} IS NULL AND new_report.prev_title{suff} IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS prev_title_match
, CASE WHEN exs_report.next_title{suff}  IS NOT NULL THEN
           CASE WHEN LOWER(regexp_replace(exs_report.next_title{suff}, '[^a-zA-Z0-9]', '')) = LOWER(regexp_replace(new_report.next_title{suff}, '[^a-zA-Z0-9]', '')) THEN '1 - Match'
                       WHEN LOWER(regexp_replace(exs_report.next_title{suff}, '[^a-zA-Z0-9]', '')) != LOWER(regexp_replace(new_report.next_title{suff}, '[^a-zA-Z0-9]', '')) THEN '3 - No Match'
                       WHEN new_report.next_title{suff} IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.next_title{suff} IS NULL AND new_report.next_title{suff} IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS next_title_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
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
SELECT CASE WHEN exs_report.prev_channel_callsign{suff} IS NOT NULL THEN
           CASE WHEN exs_report.prev_channel_callsign{suff} = new_report.prev_channel_callsign{suff} THEN '1 - Match'
                       WHEN exs_report.prev_channel_callsign{suff} != new_report.prev_channel_callsign{suff} THEN '3 - No Match'
                       WHEN new_report.prev_channel_callsign{suff} IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.prev_channel_callsign{suff} IS NULL AND new_report.prev_channel_callsign{suff} IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS prev_channel_callsign_match
, CASE WHEN exs_report.next_channel_callsign{suff} IS NOT NULL THEN
           CASE WHEN exs_report.next_channel_callsign{suff} = new_report.next_channel_callsign{suff} THEN '1 - Match'
                       WHEN exs_report.next_channel_callsign{suff} != new_report.next_channel_callsign{suff} THEN '3 - No Match'
                       WHEN new_report.next_channel_callsign{suff} IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.next_channel_callsign{suff} IS NULL AND new_report.next_channel_callsign{suff} IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS next_channel_callsign_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs  
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
SELECT CASE WHEN exs_report.prev_network_affiliate{suff} IS NOT NULL THEN
           CASE WHEN exs_report.prev_network_affiliate{suff} = new_report.prev_network_affiliate{suff} THEN '1 - Match'
                       WHEN exs_report.prev_network_affiliate{suff} != new_report.prev_network_affiliate{suff} THEN '3 - No Match'
                       WHEN new_report.prev_network_affiliate{suff} IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.prev_network_affiliate{suff} IS NULL AND new_report.prev_network_affiliate{suff} IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS prev_network_affiliate_match
, CASE WHEN exs_report.next_network_affiliate{suff}  IS NOT NULL THEN
           CASE WHEN exs_report.next_network_affiliate{suff} = new_report.next_network_affiliate{suff} THEN '1 - Match'
                       WHEN exs_report.next_network_affiliate{suff} != new_report.next_network_affiliate{suff} THEN '3 - No Match'
                       WHEN new_report.next_network_affiliate{suff} IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.next_network_affiliate{suff} IS NULL AND new_report.next_network_affiliate{suff} IS NOT NULL THEN '5 - Existing Null, New Not Null'
        ELSE '2 - All Null'
  END AS next_network_affiliate_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
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
SELECT CASE WHEN LOWER(regexp_replace(exs_report.brand_name, '[^a-zA-Z0-9]', '')) <=> LOWER(regexp_replace(new_report.brand_name, '[^a-zA-Z0-9]', '')) THEN 'Match'
       ELSE 'No Match'
  END AS brand_name_match
, CASE WHEN LOWER(regexp_replace(exs_report.title, '[^a-zA-Z0-9]', '')) <=> LOWER(regexp_replace(new_report.title, '[^a-zA-Z0-9]', '')) THEN 'Match'
       ELSE 'No Match'
  END AS ad_title_match
, CASE WHEN exs_report.duration <=> new_report.duration THEN 'Match'
       ELSE 'No Match'
  END AS ad_duration_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
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
SELECT CASE WHEN exs_report.app_service{suff}  IS NOT NULL THEN
           CASE WHEN exs_report.app_service{suff} = new_report.app_service{suff} THEN '1 - Match'
                       WHEN exs_report.app_service{suff} != new_report.app_service{suff} THEN '3 - No Match'
                       WHEN new_report.app_service{suff} IS NULL THEN '4 - Existing Not Null, New Null'
                  END
        WHEN exs_report.app_service{suff} IS NULL AND new_report.app_service{suff} IS NOT NULL THEN '5 - Existing Null, New Not Null'
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
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
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

spark.sql(app_v_linear.format(schema=schema, existing_report=existing_report, new_report=new_report, suff=suff)).display()

# COMMAND ----------

spark.sql(app_v_linear_diff.format(schema=schema, existing_report=existing_report, new_report=new_report, suff=suff)).display()

# COMMAND ----------

spark.sql(live_v_timeshifted.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(live_v_timeshifted_diff.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(input_test.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(location_test.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(prev_next_epid_test.format(schema=schema, existing_report=existing_report, new_report=new_report, suff=suff)).display()

# COMMAND ----------

spark.sql(next_prev_channel_callsign_match.format(schema=schema, existing_report=existing_report, new_report=new_report, suff=suff)).display()

# COMMAND ----------

spark.sql(next_prev_network_affiliate_match.format(schema=schema, existing_report=existing_report, new_report=new_report, suff=suff)).display()

# COMMAND ----------

spark.sql(next_prev_show_title_match.format(schema=schema, existing_report=existing_report, new_report=new_report, suff=suff)).display()

# COMMAND ----------

spark.sql(comm_metadata_match.format(schema=schema, existing_report=existing_report, new_report=new_report)).display()

# COMMAND ----------

spark.sql(live_app_service_matach.format(schema=schema, existing_report=existing_report, new_report=new_report, suff=suff)).display()

# COMMAND ----------

stop

# COMMAND ----------

field_to_check = 'live'
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
 AND NVL(exs_report.{field_to_check}, '') != REPLACE(NVL(new_report.{field_to_check}, ''), ',','')
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
, exs_report.prev_episode_id{suff}
, exs_report.prev_network_affiliate{suff}
, exs_report.prev_channel_callsign{suff}
, exs_report.{field_to_check}
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
, new_report.prev_episode_id{suff}
, new_report.prev_network_affiliate{suff}
, new_report.prev_channel_callsign{suff}
, new_report.{field_to_check}
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

field_to_check = 'prev_title'
spark.sql(f"""
WITH mm AS (
    SELECT exs_report.tvid
    , exs_report.ts_start
    , exs_report.ts_end
    , exs_report.{field_to_check} AS exs_{field_to_check}
    , new_report.{field_to_check} AS new_{field_to_check}
    FROM {schema}.{existing_report} AS exs_report
    JOIN {schema}.{new_report} AS new_report
      ON exs_report.tvid = new_report.tvid
     AND exs_report.ts_start = new_report.ts_start
     AND exs_report.ts_end = new_report.ts_end
     AND exs_report.ip <=> new_report.ip
     AND exs_report.mt_start <=> new_report.mt_start
     AND exs_report.value <=> new_report.value
     AND NVL(exs_report.{field_to_check}, '') != REPLACE(NVL(new_report.{field_to_check}, ''), ',','')
)
SELECT exs_{field_to_check}, new_{field_to_check}, COUNT(*)
FROM mm
GROUP BY 1, 2
""").display()

# COMMAND ----------

spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
, new_report.live AS new_live
, exs_report.live AS exs_live
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND (exs_report.{field_to_check} <=> new_report.{field_to_check})
 )
 SELECT new_live, exs_live, COUNT(*)
 FROM mm
 GROUP BY 1, 2""").display()

# COMMAND ----------

spark.sql(f"""
SELECT exs_report.prev_title{suff} IS NULL AS prev_show_null
, exs_report.next_title{suff} IS NULL AS next_show_null
, exs_report.prev_channel_callsign{suff} IS NULL AS prev_callsign_null
, exs_report.next_channel_callsign{suff} IS NULL AS next_callsign_null
, exs_report.live AS exs_live
, exs_report.
, COUNT(*)
 FROM {schema}.{existing_report} AS exs_report
JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND exs_report.duration <=> new_report.duration
 AND exs_report.live IS NOT NULL
 AND new_report.live IS NULL
 GROUP BY 1, 2, 3, 4, 5""").display()

# COMMAND ----------

spark.sql(f"""
SELECT exs_report.prev_title{suff} IS NULL AS prev_show_null
, exs_report.next_title{suff} IS NULL AS next_show_null
, exs_report.prev_network_affiliate{suff} IS NULL AS prev_affiliate_null
, exs_report.next_network_affiliate{suff} IS NULL AS next_affiliate_null
, input_category
-- , CASE WHEN app_service IN ('WatchFree+', 'OBFUSCATED', 'vMVPD') THEN app_service
--        WHEN app_service IS NOT NULL THEN 'Diff App Service'
--        ELSE NULL END AS app_service
, exs_report.live AS exs_live
, COUNT(*)
 FROM {schema}.{new_report} AS exs_report
 GROUP BY 1, 2, 3, 4, 5, 6""").display()

# COMMAND ----------

field_to_check = 'live'
spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
, new_report.live AS new_live
, exs_report.live AS exs_live
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND exs_report.duration <=> new_report.duration
 AND exs_report.{field_to_check} IS NOT NULL
 AND new_report.{field_to_check} IS NOT NULL
)
-- SELECT DATE_TRUNC('MINUTE', vcg.prev_ts_start)
-- , DATE_TRUNC('MINUTE', vcg.prev_ts_end)
-- SELECT vcg.prev_vizio_epg_not_null
-- , vcg.tms_prev_episode_id IS NOT NULL AND vcg.tivo_prev_episode_id IS NULL AS possible_vizio_epg_airing
-- , vcg.live AS golden_live
-- , mm.new_live
-- , mm.exs_live
SELECT vcf.is_live
, DATE_TRUNC('MINUTE', vcf.created_at) AS content_created
, COUNT(*)
FROM dev.detection.viewing_commercials_golden AS vcg
JOIN mm
  ON vcg.tvid = mm.tvid
 AND vcg.session_start = mm.ts_start
 AND vcg.session_end = mm.ts_end
 AND vcg.ip <=> mm.ip
 AND vcg.mt_start <=> mm.mt_start
LEFT JOIN prod.detection.viewing_content_firehose vcf
  ON vcf.fk_tvid = vcg.fk_tvid
 AND vcf.session_start = vcg.prev_ts_start
 AND vcf.session_start >= '2025-05-15 10:00:00'::TIMESTAMP
 AND vcf.session_start < '2025-05-15 14:00:00'::TIMESTAMP
WHERE vcg.session_start_hour = '2025-05-15 12:00:00'::TIMESTAMP
  AND vcg.commercial_client = 'kinetiq'
  AND (vcf.fk_tvid IS NOT NULL OR vcg.prev_ts_start IS NULL)
GROUP BY 1, 2--, 3, 4, 5
 """).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.detection.viewing_commercials_golden
# MAGIC WHERE fk_tvid = 140583588
# MAGIC AND session_start = '2025-06-13T13:13:35'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.mohit_gangwani.testing_golden_commercials_table
# MAGIC WHERE tvid = '10004991_58078_227182039'
# MAGIC AND session_start >= '2025-04-10T00:05:27'
# MAGIC AND session_start < '2025-04-10T00:18:52'

# COMMAND ----------

prev_or_next = 'prev'
# field_name = 'episode_id'
field_name = 'network_affiliate'
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
-- AND exs_report.value <=> new_report.value
 AND NVL(exs_report.{prev_or_next}_{field_name}{suff}, '') != NVL(new_report.{prev_or_next}_{field_name}{suff}, '')
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip
, exs_report.mt_start
, exs_report.{prev_or_next}_{field_name}{suff}
, exs_report.{prev_or_next}_title{suff}
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
, new_report.{prev_or_next}_{field_name}{suff}
, new_report.{prev_or_next}_title{suff}
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
 AND LOWER(regexp_replace(exs_report.prev_title{suff}, '[^a-zA-Z0-9]', '')) != LOWER(regexp_replace(new_report.prev_title{suff}, '[^a-zA-Z0-9]', ''))
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.prev_episode_id{suff}
, exs_report.prev_title{suff}
, exs_report.app_service{suff}
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
, new_report.prev_episode_id{suff}
, new_report.prev_title{suff}
, new_report.app_service{suff}
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
SELECT exs_report.prev_network_affiliate{suff} AS prod_title
, new_report.prev_network_affiliate{suff}      AS golden_report_title
-- , exs_report.prev_episode_id{suff} <=> new_report.prev_episode_id AS prev_epid_match
, exs_report.app_service{suff}
, COUNT(*)
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND exs_report.value <=> new_report.value
 AND TRIM(NVL(LOWER(regexp_replace(exs_report.prev_network_affiliate{suff}, '(\\W+)', '')),'')) != TRIM(NVL(LOWER(regexp_replace(new_report.prev_network_affiliate{suff}, '(\\W+)', '')),''))
 GROUP BY 1, 2, 3
 ORDER BY 4 DESC
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
 AND NVL(exs_report.value, '') <=> NVL(new_report.value, '')
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
ORDER BY tvid, ts_start, ts_end, report_name
LIMIT 1000""").display()

# COMMAND ----------

field_to_check = 'title'
spark.sql(f"""
SELECT new_report.value AS dev_external_id
, exs_report.value AS prod_external_id
, new_report.value = exs_report.value AS external_id_match
, new_report.{field_to_check} AS dev_{field_to_check}
, exs_report.{field_to_check} AS prod_{field_to_check}
, COUNT(*)
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip <=> new_report.ip
 AND exs_report.mt_start <=> new_report.mt_start
 AND NVL(exs_report.{field_to_check}, '') != NVL(new_report.{field_to_check}, '')
 AND exs_report.value <=> new_report.value
 GROUP BY 1, 2, 3, 4, 5
 ORDER BY 6 DESC
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT input_category,input_device,app_service,acrb_clients,appb_clients, COUNT(*)
# MAGIC FROM dev.detection.viewing_commercials_golden
# MAGIC WHERE session_start_hour = '2025-05-14 15:00:00'
# MAGIC GROUP BY 1, 2,3,4,5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT acrb_clients, appb_clients, app_service, COUNT(*)
# MAGIC FROM dev.detection.viewing_commercials_golden
# MAGIC WHERE session_start >= '2025-05-14 15:00:00'
# MAGIC   AND session_start < '2025-05-14 16:00:00'
# MAGIC   AND session_start_hour = '2025-05-14 15:00:00'
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH activity_obf AS (
# MAGIC   SELECT blocked_apps.app_name, override.client_name
# MAGIC   FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC   LEFT JOIN prod.detection.app_customer_activity_distribution_override AS override
# MAGIC     ON blocked_apps.app_name = override.app_name
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC , viewing_obf AS (
# MAGIC   SELECT blocked_apps.app_name, override.client_name
# MAGIC   FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC   LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC     ON blocked_apps.app_name = override.app_name
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC , prev_content AS (
# MAGIC   SELECT fk_tvid, session_start, session_end, fk_content_id, is_live
# MAGIC   FROM prod.detection.viewing_content_firehose AS content
# MAGIC   WHERE session_start >= '2025-05-14 15:00:00'
# MAGIC   AND session_start < '2025-05-14 16:00:00'
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT tvis.category AS input_category
# MAGIC , tvis.input_device AS input_device
# MAGIC , CASE WHEN UPPER(tvis.category) = 'APPS' THEN
# MAGIC         CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+'
# MAGIC             WHEN c.prev_vizio_epg_station IS NULL AND tis.app_name = 'WatchFree+' THEN 'OBFUSCATED'
# MAGIC             WHEN LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND COALESCE(c.prev_show_id, c.tms_prev_show_id) IS NOT NULL THEN NULL
# MAGIC             WHEN LOWER(tis.app_name) = 'unknown' THEN NULL
# MAGIC             ELSE tis.app_name
# MAGIC         END
# MAGIC     WHEN prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD'
# MAGIC END AS app_service
# MAGIC , CASE WHEN UPPER(tvis.category) = 'APPS'
# MAGIC             AND prev_vizio_station.name IS NULL
# MAGIC             AND acrb.app_name IS NOT NULL THEN CASE WHEN acrb.client_name IS NULL THEN 'ALL'
# MAGIC                                                     ELSE acrb.client_name END
# MAGIC END AS acrb_client
# MAGIC , CASE WHEN UPPER(tvis.category) = 'APPS' THEN
# MAGIC         CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN NULL
# MAGIC              WHEN c.prev_vizio_epg_station IS NULL AND tis.app_name = 'WatchFree+' THEN NULL
# MAGIC              WHEN appb.app_name IS NOT NULL THEN CASE WHEN appb.client_name IS NULL THEN 'ALL'
# MAGIC                                                       ELSE appb.client_name END
# MAGIC         END
# MAGIC END AS appb_client
# MAGIC , COUNT(*)
# MAGIC FROM prod.detection.viewing_commercials_firehose AS c
# MAGIC JOIN prod.detection.tv_input_stats_firehose  tvis   
# MAGIC   ON c.session_start >= tvis.create_timestamp
# MAGIC   AND c.session_start < tvis.next_create_timestamp
# MAGIC   AND tvis.create_timestamp <= '2025-05-14 16:00:00'::timestamp
# MAGIC   AND tvis.next_create_timestamp >= '2025-05-14 15:00:00'::timestamp
# MAGIC   AND c.fk_tvid = tvis.fk_tvid  
# MAGIC   AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC LEFT OUTER JOIN prod.detection.tv_inputsource tis   
# MAGIC   ON c.session_start >=  tis.create_timestamp
# MAGIC   AND c.session_start <  tis.next_create_timestamp
# MAGIC   AND tis.create_timestamp <= '2025-05-14 16:00:00'::timestamp
# MAGIC   AND tis.next_create_timestamp >= '2025-05-14 15:00:00'::timestamp
# MAGIC   AND c.fk_tvid = tis.fk_tvid
# MAGIC   AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC LEFT OUTER JOIN prev_content
# MAGIC   ON c.fk_tvid = prev_content.fk_tvid
# MAGIC   AND prev_content.session_start = c.prev_session_start
# MAGIC LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
# MAGIC   ON TRY_CAST(c.prev_vizio_epg_station AS STRING) <=> TRY_CAST(prev_vizio_station.station_id AS STRING)
# MAGIC LEFT OUTER JOIN activity_obf appb
# MAGIC   ON tis.app_name = appb.app_name
# MAGIC LEFT OUTER JOIN viewing_obf AS acrb
# MAGIC   ON tis.app_name = acrb.app_name
# MAGIC WHERE c.session_start >= '2025-05-14 15:00:00'::timestamp
# MAGIC   AND c.session_start < '2025-05-14 16:00:00'::timestamp
# MAGIC   AND c.partition_key >= '2025-05-14 15:00:00'::timestamp::DATE
# MAGIC   AND c.partition_key <= '2025-05-14 15:00:00'::timestamp::DATE
# MAGIC GROUP BY 1, 2, 3, 4, 5

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH activity_obf AS (
# MAGIC   SELECT blocked_apps.app_name, override.client_name
# MAGIC   FROM prod.detection.app_activity_distribution_blacklist AS blocked_apps
# MAGIC   LEFT JOIN prod.detection.app_customer_activity_distribution_override AS override
# MAGIC     ON blocked_apps.app_name = override.app_name
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC , viewing_obf AS (
# MAGIC   SELECT blocked_apps.app_name, override.client_name
# MAGIC   FROM prod.detection.app_viewing_distribution_blacklist AS blocked_apps
# MAGIC   LEFT JOIN prod.detection.app_customer_viewing_distribution_override override
# MAGIC     ON blocked_apps.app_name = override.app_name
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC , prev_content AS (
# MAGIC   SELECT fk_tvid, session_start, session_end, fk_content_id, is_live
# MAGIC   FROM prod.detection.viewing_content_firehose AS content
# MAGIC   WHERE session_start >= '2025-05-13 14:00:00'
# MAGIC   AND session_start < '2025-05-13 15:00:00'
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT UPPER(tvis.category) AS category
# MAGIC , CASE WHEN c.prev_vizio_epg_station IS NOT NULL THEN 'WatchFree+' END AS wfplus
# MAGIC , CASE WHEN c.prev_vizio_epg_station IS NULL AND tis.app_name = 'WatchFree+' THEN 'OBFUSCATED' END AS obfs
# MAGIC , CASE WHEN c.prev_vizio_epg_station IS NULL AND LOWER(tis.app_name) IN ('cbs all access', 'cbs news', 'paramount+', 'fandangonow', 'nbc', 'tnt', 'watch tbs', 'iheartradio','pandora','tv games') AND COALESCE(c.prev_show_id, c.tms_prev_show_id) IS NOT NULL THEN 'NLL' END AS check_three
# MAGIC , CASE WHEN LOWER(tis.app_name) = 'unknown' THEN 'NLL' END AS unkno
# MAGIC , tis.app_name
# MAGIC , CASE WHEN UPPER(tvis.category) != 'APPS' AND prev_content.is_live = true AND LOWER(tvis.input_device) in ('xbox','amazon_fire','apple_tv','chromecast','nintendo switch','playstation 4','playstation 5','roku') THEN 'vMVPD' END AS vmvpd
# MAGIC , COUNT(*)
# MAGIC FROM prod.detection.viewing_commercials_firehose AS c
# MAGIC JOIN prod.detection.tv_input_stats_firehose  tvis   
# MAGIC   ON c.session_start >= tvis.create_timestamp
# MAGIC   AND c.session_start < tvis.next_create_timestamp
# MAGIC   AND tvis.create_timestamp <= '2025-05-13 15:00:00'
# MAGIC   AND tvis.next_create_timestamp >= '2025-05-13 14:00:00'
# MAGIC   AND c.fk_tvid = tvis.fk_tvid  
# MAGIC   AND c.fk_input_source_id = tvis.fk_input_source_id
# MAGIC LEFT OUTER JOIN prod.detection.tv_inputsource tis   
# MAGIC   ON c.session_start >=  (tis.create_timestamp::double)::timestamp
# MAGIC   AND c.session_start <  (tis.next_create_timestamp::double)::timestamp
# MAGIC   AND tis.create_timestamp <= ('2025-05-13T15:00:00'::timestamp::double)::timestamp
# MAGIC   AND tis.next_create_timestamp >= ('2025-05-13T14:00:00'::timestamp::double)::timestamp
# MAGIC   AND c.fk_tvid = tis.fk_tvid
# MAGIC   AND c.fk_input_source_id = tis.fk_input_source_id
# MAGIC LEFT OUTER JOIN prev_content
# MAGIC   ON c.fk_tvid = prev_content.fk_tvid
# MAGIC   AND prev_content.session_start = c.prev_session_start
# MAGIC LEFT OUTER JOIN prod.detection.vizio_epg_station AS prev_vizio_station
# MAGIC   ON TRY_CAST(c.prev_vizio_epg_station AS STRING) <=> TRY_CAST(prev_vizio_station.station_id AS STRING)
# MAGIC LEFT OUTER JOIN activity_obf appb
# MAGIC   ON tis.app_name = appb.app_name
# MAGIC LEFT OUTER JOIN viewing_obf AS acrb
# MAGIC   ON tis.app_name = acrb.app_name
# MAGIC WHERE c.session_start >= '2025-05-13 14:00:00'::timestamp
# MAGIC   AND c.session_start < '2025-05-13 15:00:00'::timestamp
# MAGIC   AND c.partition_key >= '2025-05-13 14:00:00'::timestamp::DATE
# MAGIC   AND c.partition_key <= '2025-05-13 15:00:00'::timestamp::DATE
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_tvs_from_dp5 AS
# MAGIC WITH tm AS (
# MAGIC   SELECT minute_start, minute_stop
# MAGIC   FROM detection.time_minute tm
# MAGIC   WHERE tm.minute_start >= '2025-05-13 21:00:00'
# MAGIC     AND tm.minute_start <= '2025-05-13 22:00:00'
# MAGIC )
# MAGIC , dp4 AS (
# MAGIC   SELECT tm.minute_start
# MAGIC   , vc.tvid
# MAGIC   FROM prod.staging.vizio_content_firehose vc
# MAGIC   JOIN tm
# MAGIC     ON tm.minute_start < vc.ts_end
# MAGIC    AND tm.minute_stop > vc.ts_start
# MAGIC    AND TIMESTAMPDIFF(SECOND, GREATEST(vc.ts_start, tm.minute_start), LEAST(tm.minute_stop, vc.ts_end)) > 0
# MAGIC   WHERE vc.ts_start >= '2025-05-13 21:00:00'
# MAGIC     AND vc.ts_start < '2025-05-13 22:00:00'
# MAGIC   GROUP BY 1, 2
# MAGIC )
# MAGIC , dp5 AS (
# MAGIC   SELECT tm.minute_start
# MAGIC   , vc.tvid
# MAGIC   FROM prod.cooker.vizio_content_firehose vc
# MAGIC   JOIN tm
# MAGIC     ON tm.minute_start < vc.ts_end
# MAGIC    AND tm.minute_stop > vc.ts_start
# MAGIC    AND TIMESTAMPDIFF(SECOND, GREATEST(vc.ts_start, tm.minute_start), LEAST(tm.minute_stop, vc.ts_end)) > 0
# MAGIC   WHERE vc.ts_start >= '2025-05-13 21:00:00'
# MAGIC     AND vc.ts_start < '2025-05-13 22:00:00'
# MAGIC   GROUP BY 1, 2
# MAGIC )
# MAGIC SELECT dp4.minute_start
# MAGIC , dp4.tvid
# MAGIC FROM dp4
# MAGIC LEFT JOIN dp5
# MAGIC   ON dp4.minute_start = dp5.minute_start
# MAGIC  AND dp4.tvid = dp5.tvid
# MAGIC WHERE dp5.tvid IS NULL
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH outage_tvs AS (
# MAGIC   SELECT dp.tvid, COUNT(DISTINCT dp.minute_start) AS minute_outage
# MAGIC   FROM dev.mohit_gangwani.missing_tvs_from_dp5 dp
# MAGIC   WHERE minute_start >= '2025-05-13 21:26:00'
# MAGIC     AND minute_start <= '2025-05-13 21:41:00'
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC SELECT dp.tvid
# MAGIC FROM outage_tvs dp
# MAGIC WHERE minute_outage >= 5
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH outage_tvs AS (
# MAGIC   SELECT dp.tvid, COUNT(DISTINCT dp.minute_start) AS minute_outage
# MAGIC   FROM dev.mohit_gangwani.missing_tvs_from_dp5 dp
# MAGIC   WHERE minute_start >= '2025-05-13 21:26:00'
# MAGIC     AND minute_start <= '2025-05-13 21:41:00'
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC , prev_next_min AS (
# MAGIC   SELECT dp.tvid
# MAGIC   , dp.minute_start
# MAGIC   , NVL(LEAD(dp.minute_start) OVER (PARTITION BY dp.tvid ORDER BY dp.minute_start), '2025-05-13 22:01:00') AS next_start
# MAGIC   , NVL(LAG(dp.minute_start) OVER (PARTITION BY dp.tvid ORDER BY dp.minute_start), '2025-05-13 20:59:00') AS prev_start
# MAGIC   FROM dev.mohit_gangwani.missing_tvs_from_dp5 dp
# MAGIC )
# MAGIC SELECT ins.type, COUNT(DISTINCT dp.tvid)
# MAGIC FROM outage_tvs dp
# MAGIC JOIN dev.mohit_gangwani.missing_tvs_from_dp5 x
# MAGIC   ON dp.tvid = x.tvid
# MAGIC JOIN prev_next_min pnm
# MAGIC   ON x.tvid = pnm.tvid
# MAGIC  AND x.minute_start = pnm.minute_start
# MAGIC JOIN detection.tv_inputsource tis
# MAGIC   ON tis.fk_tvid = dp.tvid
# MAGIC  AND tis.create_timestamp <= pnm.next_start
# MAGIC  AND tis.next_create_timestamp >= pnm.prev_start
# MAGIC JOIN detection.input_source ins
# MAGIC   ON ins.input_source_id = tis.fk_input_source_id
# MAGIC WHERE minute_outage >= 5
# MAGIC   AND x.minute_start >= '2025-05-13 21:26:00'
# MAGIC   AND x.minute_start <= '2025-05-13 21:41:00'
# MAGIC   AND tis.create_timestamp <= '2025-05-13 22:00:00'
# MAGIC   AND tis.next_create_timestamp >= '2025-05-13 21:00:00'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM qa.detection.viewing_commercials_firehose_dedup
# MAGIC WHERE session_start >= CURRENT_DATE - 4
# MAGIC   AND MOD(fk_tvid, 10) = 1
# MAGIC   AND fk_commercial_source_id != 1
# MAGIC LIMIT 10
