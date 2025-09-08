# Databricks notebook source
schema = 'dev.vlad'
new_report = 'r46131_content_adimpact_2025_07_25_20_qa'
existing_report = 'r550_content_adimpact_2025_07_25_20_production'
vendor_name = 'TIVO'
tuner_included = False


# COMMAND ----------

live_suff = '_tms' if vendor_name == 'TMS' else ''
# live_suff = ''
mts_suff = '_tms' if vendor_name == 'TMS' else ''
# mts_suff = ''
suff = '_tms' if vendor_name == 'TMS' else ''
tuner_suff = '_tuner' if tuner_included else ''

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
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{mts_suff}{tuner_suff} <=> new_report.mt_start{mts_suff}{tuner_suff}
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
 , CASE WHEN exs_report.live{live_suff}{tuner_suff} IS NULL THEN 'Null Session' ELSE 'Detected Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 GROUP BY 2
)
UNION
(SELECT 'Golden Report' AS table_name
 , CASE WHEN new_report.live{live_suff}{tuner_suff} IS NULL THEN 'Null Session' ELSE 'Detected Session' END AS session_type
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
    SELECT CASE WHEN exs_report.live{live_suff}{tuner_suff} IS NULL THEN 'Null Session' ELSE 'Detected Session' END AS session_type
    , COUNT(*) AS session_count
    , COUNT(DISTINCT exs_report.tvid) AS total_tvs
    , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
    FROM {schema}.{existing_report} AS exs_report
    GROUP BY 1
)
, new AS (
    SELECT CASE WHEN new_report.live{live_suff}{tuner_suff} IS NULL THEN 'Null Session' ELSE 'Detected Session' END AS session_type
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
 , CASE WHEN exs_report.app_service{tuner_suff} IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 GROUP BY 2
)
UNION
(SELECT 'Golden Report' AS table_name
 , CASE WHEN new_report.app_service{tuner_suff} IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
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
 , CASE WHEN exs_report.app_service{tuner_suff} IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 GROUP BY 2
)
, new AS (SELECT 'Golden Report' AS table_name
 , CASE WHEN new_report.app_service{tuner_suff} IS NOT NULL THEN 'App Session' ELSE 'Non App Session' END AS session_type
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
 , CASE WHEN exs_report.live{live_suff}{tuner_suff} = 't' THEN 'Live Session'
        WHEN exs_report.live{live_suff}{tuner_suff} = 'f' THEN 'Timeshifted Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 WHERE exs_report.live{live_suff}{tuner_suff} IS NOT NULL
 GROUP BY 2
)
UNION
(SELECT 'Golden Report' AS table_name
  , CASE WHEN new_report.live{live_suff}{tuner_suff} = 't' THEN 'Live Session'
        WHEN new_report.live{live_suff}{tuner_suff} = 'f' THEN 'Timeshifted Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT new_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{new_report} AS new_report
 WHERE new_report.live{live_suff}{tuner_suff} IS NOT NULL
 GROUP BY 2
)
"""

# COMMAND ----------

live_v_timeshifted_diff = """
WITH exs AS (SELECT 'Existing Table' AS table_name
 , CASE WHEN exs_report.live{live_suff}{tuner_suff} = 't' THEN 'Live Session'
        WHEN exs_report.live{live_suff}{tuner_suff} = 'f' THEN 'Timeshifted Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT exs_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, exs_report.ts_start, exs_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{existing_report} AS exs_report
 WHERE exs_report.live{live_suff}{tuner_suff} IS NOT NULL
 GROUP BY 2
)
, new AS (SELECT 'Golden Report' AS table_name
  , CASE WHEN new_report.live{live_suff}{tuner_suff} = 't' THEN 'Live Session'
        WHEN new_report.live{live_suff}{tuner_suff} = 'f' THEN 'Timeshifted Session' END AS session_type
 , COUNT(*) AS session_count
 , COUNT(DISTINCT new_report.tvid) AS total_tvs
 , SUM(TIMESTAMPDIFF(SECOND, new_report.ts_start, new_report.ts_end))/3600.0 AS ttl_hrs
 FROM {schema}.{new_report} AS new_report
 WHERE new_report.live{live_suff}{tuner_suff} IS NOT NULL
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
SELECT CASE WHEN exs_report.tvid IS NULL THEN 'Exs Report - Session Missing'
            WHEN new_report.tvid IS NULL THEN 'New Report - Session Missing'
            WHEN NVL(exs_report.input_category{tuner_suff}, '') <=> NVL(new_report.input_category{tuner_suff}, '') THEN 'Match'
            WHEN NULLIF(exs_report.input_category{tuner_suff}, '') IS NULL AND NULLIF(new_report.input_category{tuner_suff}, '') IS NOT NULL THEN 'Exs Report Null, New Report Not Null'
            WHEN NULLIF(new_report.input_category{tuner_suff}, '') IS NULL AND NULLIF(exs_report.input_category{tuner_suff}, '') IS NOT NULL THEN 'New Report Null, Exs Report Not Null'
            ELSE 'No Match'
  END AS input_category_match
, CASE WHEN exs_report.tvid IS NULL THEN 'Exs Report - Session Missing'
       WHEN new_report.tvid IS NULL THEN 'New Report - Session Missing'
       WHEN NVL(exs_report.input_device{tuner_suff}, '') <=> NVL(new_report.input_device{tuner_suff}, '') THEN 'Match'
       WHEN NULLIF(exs_report.input_device{tuner_suff}, '') IS NULL AND NULLIF(new_report.input_device{tuner_suff}, '') IS NOT NULL THEN 'Exs Report Null, New Report Not Null'
       WHEN NULLIF(new_report.input_device{tuner_suff}, '') IS NULL AND NULLIF(exs_report.input_device{tuner_suff}, '') IS NOT NULL THEN 'New Report Null, Exs Report Not Null'
       ELSE 'No Match'
  END AS input_device_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{mts_suff}{tuner_suff} <=> new_report.mt_start{mts_suff}{tuner_suff}
GROUP BY 1, 2
ORDER BY 1, 2"""

# COMMAND ----------

location_test = """
SELECT CASE WHEN exs_report.tvid IS NULL THEN 'Exs Report - Session Missing'
            WHEN new_report.tvid IS NULL THEN 'New Report - Session Missing'
            WHEN NVL(exs_report.zipcode, '') <=> NVL(new_report.zipcode, '') THEN 'Match'
            WHEN NULLIF(exs_report.zipcode, '') IS NULL AND NULLIF(new_report.zipcode, '') IS NOT NULL THEN 'Exs Report Null, New Report Not Null'
            WHEN NULLIF(new_report.zipcode, '') IS NULL AND NULLIF(exs_report.zipcode, '') IS NOT NULL THEN 'New Report Null, Exs Report Not Null'
            ELSE 'No Match'
  END AS zipcode_match
, CASE WHEN exs_report.tvid IS NULL THEN 'Exs Report - Session Missing'
       WHEN new_report.tvid IS NULL THEN 'New Report - Session Missing'
       WHEN NVL(exs_report.dma, '') <=> NVL(new_report.dma, '') THEN 'Match'
       WHEN NULLIF(exs_report.dma, '') IS NULL AND NULLIF(new_report.dma, '') IS NOT NULL THEN 'Exs Report Null, New Report Not Null'
       WHEN NULLIF(new_report.dma, '') IS NULL AND NULLIF(exs_report.dma, '') IS NOT NULL THEN 'New Report Null, Exs Report Not Null'
       ELSE 'No Match'
  END AS dma_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{mts_suff}{tuner_suff} <=> new_report.mt_start{mts_suff}{tuner_suff}
GROUP BY 1, 2
ORDER BY 1, 2;"""

# COMMAND ----------

epid_test = """
SELECT CASE WHEN exs_report.tvid IS NULL THEN '0 - Exs Report - Session Missing'
            WHEN new_report.tvid IS NULL THEN '0 - New Report - Session Missing'
            WHEN NVL(exs_report.episode_id{suff}{tuner_suff}, '') <=> NVL(new_report.episode_id{suff}{tuner_suff}, '') THEN '1 - Match'
            WHEN NULLIF(new_report.episode_id{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(exs_report.episode_id{suff}{tuner_suff}, '') IS NULL THEN '2 - New Not Null, Existing Null'
            WHEN NULLIF(exs_report.episode_id{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(new_report.episode_id{suff}{tuner_suff}, '') IS NULL THEN '3 - Existing Not Null, New Null'
            WHEN NOT(exs_report.episode_id{suff}{tuner_suff} <=> new_report.episode_id{suff}{tuner_suff}) THEN '4 - No Match'
  END AS episode_id_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{mts_suff}{tuner_suff} <=> new_report.mt_start{mts_suff}{tuner_suff}
GROUP BY 1
ORDER BY 1
"""

# COMMAND ----------

show_title_match = """
SELECT CASE WHEN exs_report.tvid IS NULL THEN '0 - Exs Report - Session Missing'
            WHEN new_report.tvid IS NULL THEN '0 - New Report - Session Missing'
            WHEN NVL(exs_report.show_title{suff}{tuner_suff}, '') <=> NVL(new_report.show_title{suff}{tuner_suff}, '') THEN '1 - Match'
            WHEN NULLIF(new_report.show_title{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(exs_report.show_title{suff}{tuner_suff}, '') IS NULL THEN '2 - New Not Null, Existing Null'
            WHEN NULLIF(exs_report.show_title{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(new_report.show_title{suff}{tuner_suff}, '') IS NULL THEN '3 - Existing Not Null, New Null'
            WHEN NOT(exs_report.show_title{suff}{tuner_suff} <=> new_report.show_title{suff}{tuner_suff}) THEN '4 - No Match'
  END AS show_title_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{mts_suff}{tuner_suff} <=> new_report.mt_start{mts_suff}{tuner_suff}
GROUP BY 1
ORDER BY 1
"""

# COMMAND ----------

channel_callsign_match = """
SELECT CASE WHEN exs_report.tvid IS NULL THEN '0 - Exs Report - Session Missing'
            WHEN new_report.tvid IS NULL THEN '0 - New Report - Session Missing'
            WHEN NVL(exs_report.channel_callsign{suff}{tuner_suff}, '') <=> NVL(new_report.channel_callsign{suff}{tuner_suff}, '') THEN '1 - Match'
            WHEN NULLIF(new_report.channel_callsign{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(exs_report.channel_callsign{suff}{tuner_suff}, '') IS NULL THEN '2 - New Not Null, Existing Null'
            WHEN NULLIF(exs_report.channel_callsign{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(new_report.channel_callsign{suff}{tuner_suff}, '') IS NULL THEN '3 - Existing Not Null, New Null'
            WHEN NOT(exs_report.channel_callsign{suff}{tuner_suff} <=> new_report.channel_callsign{suff}{tuner_suff}) THEN '4 - No Match'
  END AS channel_callsign_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs  
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{mts_suff}{tuner_suff} <=> new_report.mt_start{mts_suff}{tuner_suff}
GROUP BY 1
ORDER BY 1
"""

# COMMAND ----------

network_affiliate_match = """
SELECT CASE WHEN exs_report.tvid IS NULL THEN '0 - Exs Report - Session Missing'
            WHEN new_report.tvid IS NULL THEN '0 - New Report - Session Missing'
            WHEN NVL(exs_report.channel_affiliate{suff}{tuner_suff}, '') <=> NVL(new_report.channel_affiliate{suff}{tuner_suff}, '') THEN '1 - Match'
            WHEN NULLIF(new_report.channel_affiliate{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(exs_report.channel_affiliate{suff}{tuner_suff}, '') IS NULL THEN '2 - New Not Null, Existing Null'
            WHEN NULLIF(exs_report.channel_affiliate{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(new_report.channel_affiliate{suff}{tuner_suff}, '') IS NULL THEN '3 - Existing Not Null, New Null'
            WHEN NOT(exs_report.channel_affiliate{suff}{tuner_suff} <=> new_report.channel_affiliate{suff}{tuner_suff}) THEN '4 - No Match'
  END AS channel_affiliate_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{mts_suff}{tuner_suff} <=> new_report.mt_start{mts_suff}{tuner_suff}
GROUP BY 1
ORDER BY 1
"""

# COMMAND ----------

air_date_match = """
SELECT CASE WHEN exs_report.tvid IS NULL THEN '0 - Exs Report - Session Missing'
            WHEN new_report.tvid IS NULL THEN '0 - New Report - Session Missing'
            WHEN NVL(exs_report.air_date{suff}{tuner_suff}, '') <=> NVL(new_report.air_date{suff}{tuner_suff}, '') THEN '1 - Match'
            WHEN NULLIF(new_report.air_date{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(exs_report.air_date{suff}{tuner_suff}, '') IS NULL THEN '2 - New Not Null, Existing Null'
            WHEN NULLIF(exs_report.air_date{suff}{tuner_suff}, '') IS NOT NULL AND NULLIF(new_report.air_date{suff}{tuner_suff}, '') IS NULL THEN '3 - Existing Not Null, New Null'
            WHEN NOT(exs_report.air_date{suff}{tuner_suff} <=> new_report.air_date{suff}{tuner_suff}) THEN '4 - No Match'
  END AS air_date_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{mts_suff}{tuner_suff} <=> new_report.mt_start{mts_suff}{tuner_suff}
GROUP BY 1
ORDER BY 1
"""

# COMMAND ----------

app_service_match = """
SELECT CASE WHEN exs_report.tvid IS NULL THEN '0 - Exs Report - Session Missing'
            WHEN new_report.tvid IS NULL THEN '0 - New Report - Session Missing'
            WHEN NVL(exs_report.app_service{tuner_suff}, '') <=> NVL(new_report.app_service{tuner_suff}, '') THEN '1 - Match'
            WHEN NULLIF(new_report.app_service{tuner_suff}, '') IS NOT NULL AND NULLIF(exs_report.app_service{tuner_suff}, '') IS NULL THEN '2 - New Not Null, Existing Null'
            WHEN NULLIF(exs_report.app_service{tuner_suff}, '') IS NOT NULL AND NULLIF(new_report.app_service{tuner_suff}, '') IS NULL THEN '3 - Existing Not Null, New Null'
            WHEN NOT(exs_report.app_service{tuner_suff} <=> new_report.app_service{tuner_suff}) THEN '4 - No Match'
  END AS app_service_match
, COUNT(*) AS session_count
, COUNT(DISTINCT NVL(exs_report.tvid, new_report.tvid)) AS total_tvs
FROM {schema}.{existing_report} AS exs_report
FULL JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{mts_suff}{tuner_suff} <=> new_report.mt_start{mts_suff}{tuner_suff}
GROUP BY 1
ORDER BY 1
"""

# COMMAND ----------

spark.sql(count_test.format(schema=schema, existing_report=existing_report, new_report=new_report, suff=suff, mts_suff=mts_suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(count_diff.format(schema=schema, existing_report=existing_report, new_report=new_report, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(null_not_null.format(schema=schema, existing_report=existing_report, new_report=new_report, live_suff=live_suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(null_not_null_diff.format(schema=schema, existing_report=existing_report, new_report=new_report, live_suff=live_suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(app_v_linear.format(schema=schema, existing_report=existing_report, new_report=new_report, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(app_v_linear_diff.format(schema=schema, existing_report=existing_report, new_report=new_report, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(live_v_timeshifted.format(schema=schema, existing_report=existing_report, new_report=new_report, live_suff=live_suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(live_v_timeshifted_diff.format(schema=schema, existing_report=existing_report, new_report=new_report, live_suff=live_suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(input_test.format(schema=schema, existing_report=existing_report, new_report=new_report, mts_suff=mts_suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(location_test.format(schema=schema, existing_report=existing_report, new_report=new_report, mts_suff=mts_suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(epid_test.format(schema=schema, existing_report=existing_report, new_report=new_report, mts_suff=mts_suff, suff=suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(channel_callsign_match.format(schema=schema, existing_report=existing_report, new_report=new_report, mts_suff=mts_suff, suff=suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(network_affiliate_match.format(schema=schema, existing_report=existing_report, new_report=new_report, mts_suff=mts_suff, suff=suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(show_title_match.format(schema=schema, existing_report=existing_report, new_report=new_report, mts_suff=mts_suff, suff=suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(air_date_match.format(schema=schema, existing_report=existing_report, new_report=new_report, mts_suff=mts_suff, suff=suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

spark.sql(app_service_match.format(schema=schema, existing_report=existing_report, new_report=new_report, mts_suff=mts_suff, suff=suff, tuner_suff=tuner_suff)).display()

# COMMAND ----------

stop

# COMMAND ----------

spark.sql(f"""
SELECT exs_report.tvid
, new_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.input_category AS exs_input_category
, new_report.input_category AS new_input_category
FROM {schema}.{existing_report} AS exs_report
JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
--  AND exs_report.ip_null_null <=> new_report.ip_null_null
--  AND exs_report.mt_start <=> new_report.mt_start
WHERE NVL(exs_report.input_category, '') != NVL(new_report.input_category, '')
ORDER BY 2
LIMIT 1000
""").display()

# COMMAND ----------

spark.sql(f"""
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.input_category AS exs_input_category
FROM {schema}.{existing_report} AS exs_report
WHERE exs_report.tvid IS NULL
ORDER BY 1, 2
LIMIT 1000
""").display()

# COMMAND ----------

field_to_check = f'episode_title{suff}'
spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip_null
, exs_report.mt_start{mts_suff}
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
-- AND exs_report.mt_start <=> new_report.mt_start
 AND NVL(exs_report.{field_to_check}, '') != NVL(new_report.{field_to_check}, '')
 )
 (SELECT exs_report.tvid
, REPLACE(exs_report.ts_start, '.000+00:00', '') AS ts_start
, REPLACE(exs_report.ts_end, '.000+00:00', '') AS ts_end
, exs_report.show_title{suff}
, exs_report.app_service
--, exs_report.mt_start{mts_suff}
--, exs_report.live{live_suff}
, exs_report.episode_id{suff}
, exs_report.show_title{suff}
, REPLACE(exs_report.air_date, '.000+00:00', '') AS air_date
, exs_report.{field_to_check}
, 'Existing Report' AS report_name
 FROM {schema}.{existing_report} AS exs_report
 JOIN mm
   ON exs_report.tvid = mm.tvid
 AND exs_report.ts_start = mm.ts_start
 AND exs_report.ts_end = mm.ts_end
 AND exs_report.ip_null <=> mm.ip_null
-- AND exs_report.mt_start <=> mm.mt_start
 )
UNION
 (SELECT new_report.tvid
, REPLACE(new_report.ts_start, '.000+00:00', '') AS ts_start
, REPLACE(new_report.ts_end, '.000+00:00', '') AS ts_end
, new_report.show_title{suff}
, new_report.app_service
--, new_report.mt_start{mts_suff}
--, new_report.live{live_suff}
, new_report.episode_id{suff}
, new_report.show_title{suff}
, REPLACE(new_report.air_date, '.000+00:00', '') AS air_date
, new_report.{field_to_check}
, 'New Report' AS report_name
 FROM {schema}.{new_report} AS new_report
 JOIN mm
  ON new_report.tvid = mm.tvid
 AND new_report.ts_start = mm.ts_start
 AND new_report.ts_end = mm.ts_end
 AND new_report.ip_null <=> mm.ip_null
-- AND new_report.mt_start <=> mm.mt_start
 )
ORDER BY tvid, ts_start, report_name
LIMIT 1000""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.detection.viewing_content_golden
# MAGIC WHERE fk_tvid = 101376479
# MAGIC AND session_start = '2025-07-25 17:50:58'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.detection.viewing_content_tuner_golden
# MAGIC WHERE fk_tvid = 153518355
# MAGIC AND session_start = '2025-06-30T00:00:00.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.detection.viewing_content_firehose
# MAGIC WHERE fk_tvid = 134224933
# MAGIC AND session_start = '2025-07-22T11:30:00.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN tms_airdate IS NULL THEN 1 ELSE 0 END AS null_tms_airdate
# MAGIC , CASE WHEN airdate IS NULL THEN 1 ELSE 0 END AS null_tivo_airdate
# MAGIC , COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC WHERE session_start >= CURRENT_DATE - 1
# MAGIC   AND tms_tuner_schedule_id IS NOT NULL
# MAGIC GROUP BY 1, 2
# MAGIC -- WHERE fk_tvid = 100670525
# MAGIC -- AND session_start = '2025-06-23T17:00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM detection.epg_schedule_latest
# MAGIC WHERE schedule_id = 152430886925

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM detection.epg_schedule
# MAGIC WHERE fk_station_id = 100444
# MAGIC AND fk_show_id = 543783
# MAGIC AND airdate = '2025-06-23T17:00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT mt.tvid
# MAGIC , '' AS hash
# MAGIC , mt.zipcode
# MAGIC , mt.dma
# MAGIC , CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
# MAGIC        WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN mt.acrb_clients <=> '|ALL|' AND '' != COALESCE(mt.app_service, '98989898989898') THEN NULL
# MAGIC        WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN 'nielsen' != 'nielsen'  AND mt.nielsen_exclusive THEN NULL
# MAGIC        WHEN False = FALSE          AND mt.vod_station = TRUE THEN NULL
# MAGIC        WHEN 'TMS' = 'TMS'  THEN COALESCE(tuner.tms_episode_id, mt.tms_episode_id)
# MAGIC        WHEN 'TMS' = 'TIVO' THEN COALESCE(tuner.tivo_episode_id, mt.tivo_episode_id)
# MAGIC   END AS episode_id
# MAGIC
# MAGIC , CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
# MAGIC                                             ELSE COALESCE(mt.tivo_title, mt.tms_title) END
# MAGIC        WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN mt.acrb_clients <=> '|ALL|' AND '' != COALESCE(mt.app_service, '98989898989898') THEN NULL
# MAGIC        WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN 'nielsen' != 'nielsen'  AND mt.nielsen_exclusive THEN NULL
# MAGIC        WHEN False = FALSE          AND mt.vod_station = TRUE THEN NULL
# MAGIC        WHEN 'nielsen' = 'nielsen' THEN COALESCE(tuner.tms_title, mt.tms_title)
# MAGIC        WHEN 'TMS' = 'TMS'  THEN COALESCE(tuner.tms_title, tuner.tivo_title, mt.tms_title, mt.tivo_title)
# MAGIC        WHEN 'TMS' = 'TIVO' THEN COALESCE(tuner.tivo_title, tuner.tms_title, mt.tivo_title, mt.tms_title)
# MAGIC   END AS show_title
# MAGIC
# MAGIC , CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
# MAGIC                                             ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
# MAGIC        WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN mt.acrb_clients <=> '|ALL|' AND '' != COALESCE(mt.app_service, '98989898989898') THEN NULL
# MAGIC        WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN 'nielsen' != 'nielsen'  AND mt.nielsen_exclusive THEN NULL
# MAGIC        WHEN False = FALSE          AND mt.vod_station = TRUE THEN NULL
# MAGIC        WHEN 'nielsen' = 'nielsen' THEN COALESCE(tuner.tms_airdate, mt.tms_airdate)
# MAGIC        WHEN 'TMS' = 'TMS'  THEN COALESCE(tuner.tms_airdate, tuner.tivo_airdate, mt.tms_airdate, mt.tivo_airdate)
# MAGIC        WHEN 'TMS' = 'TIVO' THEN COALESCE(tuner.tivo_airdate, tuner.tms_airdate, mt.tivo_airdate, mt.tms_airdate)
# MAGIC   END AS air_date
# MAGIC
# MAGIC , CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
# MAGIC                                             ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
# MAGIC        WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN mt.acrb_clients <=> '|ALL|' AND '' != COALESCE(mt.app_service, '98989898989898') THEN NULL
# MAGIC        WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN 'nielsen' != 'nielsen'  AND mt.nielsen_exclusive THEN NULL
# MAGIC        WHEN False = FALSE          AND mt.vod_station = TRUE THEN NULL
# MAGIC        WHEN 'nielsen' = 'nielsen' THEN COALESCE(tuner.tms_channel_callsign, mt.tms_channel_callsign)
# MAGIC        WHEN 'TMS' = 'TMS'
# MAGIC           THEN COALESCE(tuner.tms_channel_callsign, tuner.tivo_channel_callsign, mt.tms_channel_callsign, mt.tivo_channel_callsign)
# MAGIC        WHEN 'TMS' = 'TIVO'
# MAGIC           THEN COALESCE(tuner.tivo_channel_callsign, tuner.tms_channel_callsign, mt.tivo_channel_callsign, mt.tms_channel_callsign)
# MAGIC   END AS channel_callsign
# MAGIC
# MAGIC , CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
# MAGIC        WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN mt.acrb_clients <=> '|ALL|' AND '' != COALESCE(mt.app_service, '98989898989898') THEN NULL
# MAGIC        WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN 'nielsen' != 'nielsen'  AND mt.nielsen_exclusive   THEN NULL
# MAGIC        WHEN False = FALSE          AND mt.vod_station = TRUE  THEN NULL
# MAGIC        WHEN 'nielsen' = 'nielsen'   AND COALESCE(tuner.tms_airdate, mt.tms_airdate) IS NULL THEN NULL
# MAGIC        ELSE COALESCE(tuner.mt_start, mt.mt_start)
# MAGIC   END AS mt_start
# MAGIC
# MAGIC , mt.session_start AS ts_start
# MAGIC , mt.session_end   AS ts_end
# MAGIC
# MAGIC , CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
# MAGIC                                    ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
# MAGIC        WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN mt.acrb_clients <=> '|ALL|' AND '' != COALESCE(mt.app_service, '98989898989898') THEN NULL
# MAGIC        WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN 'nielsen' != 'nielsen'  AND mt.nielsen_exclusive THEN NULL
# MAGIC        WHEN False = FALSE          AND mt.vod_station = TRUE THEN NULL
# MAGIC        WHEN 'nielsen' = 'nielsen' THEN COALESCE(tuner.tms_channel_affiliate, mt.tms_channel_affiliate)
# MAGIC        WHEN 'TMS' = 'TMS'
# MAGIC           THEN COALESCE(tuner.tms_channel_affiliate, tuner.tivo_channel_affiliate, mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
# MAGIC        WHEN 'TMS' = 'TIVO'
# MAGIC           THEN COALESCE(tuner.tivo_channel_affiliate, tuner.tms_channel_affiliate, mt.tivo_channel_affiliate, mt.tms_channel_affiliate)
# MAGIC   END AS channel_affiliate
# MAGIC
# MAGIC , CASE WHEN mt.vizio_epg_not_null THEN 't'
# MAGIC        WHEN mt.acrb_clients NOT IN ('||', '|ALL|') AND mt.acrb_clients NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN mt.acrb_clients <=> '|ALL|' AND '' != COALESCE(mt.app_service, '98989898989898') THEN NULL
# MAGIC        WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|nielsen|%' THEN NULL
# MAGIC        WHEN 'nielsen' != 'nielsen'  AND mt.nielsen_exclusive THEN NULL
# MAGIC        WHEN False = FALSE AND mt.vod_station = TRUE THEN NULL
# MAGIC        WHEN 'nielsen' = 'nielsen'  AND COALESCE(tuner.tms_airdate, mt.tms_airdate) IS NULL THEN NULL
# MAGIC        WHEN 'nielsen' != 'nielsen' AND COALESCE(tuner.tivo_airdate, tuner.tms_airdate, mt.tivo_airdate, mt.tms_airdate) IS NULL THEN NULL
# MAGIC        ELSE COALESCE(tuner.is_live, mt.is_live)
# MAGIC   END AS live
# MAGIC
# MAGIC , mt.ip_null_address AS ip
# MAGIC , COALESCE(tuner.input_category, mt.input_category) AS input_category
# MAGIC , COALESCE(tuner.input_device, mt.input_device) AS input_device
# MAGIC
# MAGIC , CASE WHEN mt.vizio_epg_not_null = False
# MAGIC        THEN CASE WHEN mt.appb_clients <=> '|ALL|' AND '' <=> mt.app_service THEN COALESCE(tuner.app_service, mt.app_service)
# MAGIC                  WHEN mt.appb_clients IS NOT NULL AND mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|nielsen|%' THEN 'OBFUSCATED'
# MAGIC                  WHEN mt.appb_clients <=> '|ALL|' THEN 'OBFUSCATED'
# MAGIC                  ELSE COALESCE(tuner.app_service, mt.app_service) END
# MAGIC        ELSE COALESCE(tuner.app_service, mt.app_service)
# MAGIC   END AS app_service
# MAGIC
# MAGIC , tuner.tuner_channel_number AS tuner_channel_number
# MAGIC
# MAGIC FROM dev.detection.viewing_content_golden AS mt
# MAGIC LEFT JOIN dev.detection.viewing_content_tuner_golden AS tuner
# MAGIC   ON tuner.fk_tvid = mt.fk_tvid
# MAGIC  AND tuner.session_start = mt.session_start
# MAGIC  AND tuner.session_end = mt.session_end
# MAGIC  AND tuner.ip_null_address <=> mt.ip_null_address
# MAGIC WHERE mt.fk_tvid = 100670525
# MAGIC AND mt.session_start = '2025-06-23T17:00:00.000+00:00'
# MAGIC AND tuner.fk_tvid = 100670525
# MAGIC AND tuner.session_start = '2025-06-23T17:00:00.000+00:00'

# COMMAND ----------

field_name = f'channel_affiliate{suff}'
spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip_null
, exs_report.mt_start
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start <=> new_report.mt_start
 AND NOT(exs_report.{field_name} <=> new_report.{field_name})
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip_null
, exs_report.mt_start
, exs_report.{field_name}
, exs_report.app_service
, 'Existing Report' AS report_name
 FROM {schema}.{existing_report} AS exs_report
 JOIN mm
   ON exs_report.tvid = mm.tvid
 AND exs_report.ts_start = mm.ts_start
 AND exs_report.ts_end = mm.ts_end
 AND exs_report.ip_null <=> mm.ip_null
 AND exs_report.mt_start <=> mm.mt_start)
UNION
 (SELECT new_report.tvid
, new_report.ts_start
, new_report.ts_end
, new_report.ip_null
, new_report.mt_start
, new_report.{field_name}
, new_report.app_service
, 'New Report' AS report_name
 FROM {schema}.{new_report} AS new_report
 JOIN mm
  ON new_report.tvid = mm.tvid
 AND new_report.ts_start = mm.ts_start
 AND new_report.ts_end = mm.ts_end
 AND new_report.ip_null <=> mm.ip_null
 AND new_report.mt_start <=> mm.mt_start)
ORDER BY tvid, ts_start, report_name
LIMIT 1000""").display()

# COMMAND ----------

spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip_null
, exs_report.mt_start
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start <=> new_report.mt_start
 AND LOWER(regexp_replace(exs_report.show_title{suff}, '[^a-zA-Z0-9]', '')) != LOWER(regexp_replace(new_report.show_title{suff}, '[^a-zA-Z0-9]', ''))
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.episode_id{suff}
, exs_report.show_title{suff}
, exs_report.app_service{suff}
, 'Existing Report' AS report_name
 FROM {schema}.{existing_report} AS exs_report
 JOIN mm
   ON exs_report.tvid = mm.tvid
 AND exs_report.ts_start = mm.ts_start
 AND exs_report.ts_end = mm.ts_end
 AND exs_report.ip_null <=> mm.ip_null
 AND exs_report.mt_start <=> mm.mt_start)
UNION
 (SELECT new_report.tvid
, new_report.ts_start
, new_report.ts_end
, new_report.episode_id{suff}
, new_report.show_title{suff}
, new_report.app_service{suff}
, 'New Report' AS report_name
 FROM {schema}.{new_report} AS new_report
 JOIN mm
  ON new_report.tvid = mm.tvid
 AND new_report.ts_start = mm.ts_start
 AND new_report.ts_end = mm.ts_end
 AND new_report.ip_null <=> mm.ip_null
 AND new_report.mt_start <=> mm.mt_start)
ORDER BY tvid, ts_start, report_name
LIMIT 10000""").display()

# COMMAND ----------

spark.sql(f"""
SELECT exs_report.show_title{suff} AS prod_title
, new_report.show_title{suff}      AS golden_report_title
-- , exs_report.prev_episode_id{suff} <=> new_report.prev_episode_id AS prev_epid_match
, exs_report.app_service{suff}
, COUNT(*)
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start <=> new_report.mt_start
 AND TRIM(NVL(LOWER(regexp_replace(exs_report.show_title{suff}, '(\\W+)', '')),'')) != TRIM(NVL(LOWER(regexp_replace(new_report.show_title{suff}, '(\\W+)', '')),''))
 GROUP BY 1, 2, 3
 ORDER BY 4 DESC
LIMIT 1000""").display()

# COMMAND ----------

spark.sql(f"""
SELECT exs_report.episode_id{suff} AS prod_title
, new_report.episode_id{suff}      AS golden_report_title
, exs_report.app_service
, COUNT(*)
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start{suff} <=> new_report.mt_start{suff}
 AND exs_report.episode_id{suff} != new_report.episode_id{suff}
 GROUP BY 1, 2, 3
 ORDER BY 4 DESC
LIMIT 1000""").display()

# COMMAND ----------

spark.sql(f"""
WITH mm AS (
SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.ip_null
, exs_report.mt_start
 FROM {schema}.{existing_report} AS exs_report
 JOIN {schema}.{new_report} AS new_report
  ON exs_report.tvid = new_report.tvid
 AND exs_report.ts_start = new_report.ts_start
 AND exs_report.ts_end = new_report.ts_end
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start <=> new_report.mt_start
 AND NVL(exs_report.value, '') <=> NVL(new_report.value, '')
 AND NVL(exs_report.title, '') != NVL(new_report.title, '')
 )
 (SELECT exs_report.tvid
, exs_report.ts_start
, exs_report.ts_end
, exs_report.value AS external_id
-- , exs_report.ip_null
-- , exs_report.mt_start
, exs_report.title
, 'Existing Report' AS report_name
 FROM {schema}.{existing_report} AS exs_report
 JOIN mm
   ON exs_report.tvid = mm.tvid
 AND exs_report.ts_start = mm.ts_start
 AND exs_report.ts_end = mm.ts_end
 AND exs_report.ip_null <=> mm.ip_null
 AND exs_report.mt_start <=> mm.mt_start)
UNION
 (SELECT new_report.tvid
, new_report.ts_start
, new_report.ts_end
, new_report.value AS external_id
-- , new_report.ip_null
-- , new_report.mt_start
, new_report.title
, 'New Report' AS report_name
 FROM {schema}.{new_report} AS new_report
 JOIN mm
  ON new_report.tvid = mm.tvid
 AND new_report.ts_start = mm.ts_start
 AND new_report.ts_end = mm.ts_end
 AND new_report.ip_null <=> mm.ip_null
 AND new_report.mt_start <=> mm.mt_start)
ORDER BY tvid, ts_start, ts_end, report_name
LIMIT 1000""").display()

# COMMAND ----------

field_to_check = 'duration'
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
 AND exs_report.ip_null <=> new_report.ip_null
 AND exs_report.mt_start <=> new_report.mt_start
 AND NVL(exs_report.{field_to_check}, '') != NVL(new_report.{field_to_check}, '')
 AND exs_report.value <=> new_report.value
 GROUP BY 1, 2, 3, 4, 5
 ORDER BY 6 DESC
""").display()

# COMMAND ----------


