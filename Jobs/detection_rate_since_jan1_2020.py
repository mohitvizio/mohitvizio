# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO dev.mohit_gangwani.detection_rate_since_jan12020
# MAGIC WITH max_date AS (SELECT MAX(session_hour) + INTERVAL 1 DAY AS session_day FROM dev.mohit_gangwani.detection_rate_since_jan12020)
# MAGIC SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC , CASE WHEN (vc.fk_input_source_id NOT IN  (47, 32, 34425, 44, 48) AND (tvis.category = 'HD TV' OR lookup.category = 'MVPD')) THEN 'Linear'
# MAGIC        ELSE 'OTHER' END AS viewing_type
# MAGIC , CASE WHEN fk_content_id = 3468026 THEN 'Null Session' ELSE 'Detected Session' END AS sess_type
# MAGIC , COUNT(*) AS total_sessions
# MAGIC , COUNT(DISTINCT vc.fk_tvid) AS total_tvs
# MAGIC , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC FROM prod.detection.viewing_content_firehose vc
# MAGIC JOIN prod.detection.tv
# MAGIC   ON vc.fk_tvid = tv.tvid
# MAGIC JOIN prod.detection.tv_input_stats_firehose  tvis
# MAGIC   ON vc.fk_tvid = tvis.fk_tvid
# MAGIC  AND vc.fk_input_source_id = tvis.fk_input_source_id
# MAGIC  AND vc.session_start >= tvis.create_timestamp
# MAGIC  AND vc.session_start < tvis.next_create_timestamp
# MAGIC  AND tvis.next_create_timestamp >= (SELECT session_day FROM max_date) - INTERVAL 1 DAY
# MAGIC LEFT JOIN prod.detection.logo_detection ld
# MAGIC   ON vc.fk_tvid = ld.tvid
# MAGIC  AND vc.fk_input_source_id = ld.fk_input_source_id
# MAGIC  AND vc.session_start >= ld.view_ts
# MAGIC  AND vc.session_start < ld.next_match_ts
# MAGIC LEFT JOIN prod.detection.logo_detection_lookup lookup
# MAGIC   ON ld.corrected_logo_id = lookup.id
# MAGIC WHERE vc.session_start >= (SELECT session_day FROM max_date)
# MAGIC   AND vc.session_end < CURRENT_DATE
# MAGIC   AND vc.fk_zoo_id = 17
# MAGIC   AND tvis.total_duration > 0
# MAGIC   AND vc.session_duration > 0
# MAGIC   AND tv.model_name IS NOT NULL
# MAGIC   AND tv.model_name NOT IN ('test', '', 'Convert_Board', 'E60_DV_Panel-less', 'E60_FY17_DV_Panel-less', 'M70_FY17_POH_Panel-less', 'Test_28')
# MAGIC   AND tv.model_name NOT ILIKE 'test%'
# MAGIC GROUP BY 1, 2, 3
