# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('HOUR', session_start), COUNT(*) AS total_sessions, COUNT(CASE WHEN cnt > 1 then cnt END) AS total_duplicate_sessions
# MAGIC FROM (
# MAGIC select fk_tvid, session_start, count(*) AS cnt
# MAGIC from dev.historic.viewing_content_firehose
# MAGIC -- where session_start >= '2024-10-26 18:00:00'
# MAGIC -- AND fk_content_id != 3468026
# MAGIC WHERE tuner_program_id IS NOT NULL
# MAGIC group by fk_tvid, session_start)
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('HOUR', session_start), COUNT(*) AS total_sessions, COUNT(CASE WHEN cnt > 1 then cnt END) AS total_duplicate_sessions
# MAGIC FROM (
# MAGIC select fk_tvid, session_start, count(*) AS cnt
# MAGIC from prod.table_backup.viewing_content_firehose_08_14_24 
# MAGIC where session_start >= '2024-08-26 18:00:00'
# MAGIC AND fk_content_id != 3468026
# MAGIC group by fk_tvid, session_start)
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC (SELECT 'New Prod', DATE_TRUNC('HOUR', session_start), COUNT(*), COUNT(DISTINCT fk_tvid), SUM(session_duration)/3600.0
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC WHERE session_start >= '2024-08-25T00:00:00'
# MAGIC AND fk_zoo_id = 17
# MAGIC GROUP BY 2)
# MAGIC UNION
# MAGIC (SELECT 'Old Prod', DATE_TRUNC('HOUR', session_start), COUNT(*), COUNT(DISTINCT fk_tvid), SUM(session_duration)/3600.0
# MAGIC FROM prod.table_backup.viewing_content_firehose_08_14_24
# MAGIC WHERE session_start >= '2024-08-25T00:00:00'
# MAGIC AND fk_zoo_id = 17
# MAGIC GROUP BY 2)
# MAGIC ORDER BY 2 DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select DATE_TRUNC('DAY', session_start), count(*) AS cnt
# MAGIC from prod.historic.viewing_content_firehose
# MAGIC WHERE tuner_program_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH exs_table AS (
# MAGIC   SELECT DATE_TRUNC('Hour', old_table.session_start) AS session_hour
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT old_table.fk_tvid) AS total_tvs
# MAGIC  , SUM(session_duration)/3600.0 AS ttl_hrs
# MAGIC  FROM prod.table_backup.viewing_content_firehose_08_14_24 AS old_table
# MAGIC  WHERE session_start >= '2024-08-25T00:00:00'
# MAGIC AND fk_zoo_id = 17
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC , new_table AS (
# MAGIC   SELECT DATE_TRUNC('Hour', new_table.session_start) AS session_hour
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT new_table.fk_tvid) AS total_tvs
# MAGIC  , SUM(session_duration)/3600.0 AS ttl_hrs
# MAGIC  FROM prod.detection.viewing_content_firehose AS new_table
# MAGIC  WHERE session_start >= '2024-08-25T00:00:00'
# MAGIC AND fk_zoo_id = 17
# MAGIC  GROUP BY 1
# MAGIC )
# MAGIC SELECT NVL(e.session_hour, n.session_hour) AS session_hour
# MAGIC , e.session_count AS existing_session_count
# MAGIC , n.session_count AS new_session_count
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC , e.ttl_hrs AS existing_ttl_hrs
# MAGIC , n.ttl_hrs AS new_ttl_hrs
# MAGIC , (e.session_count - n.session_count)/(e.session_count*.01) AS session_count_diff_perc
# MAGIC , (e.total_tvs - n.total_tvs)/(e.total_tvs*.01) AS tv_count_diff_perc
# MAGIC , (e.ttl_hrs - n.ttl_hrs)/(e.ttl_hrs*.01) AS duration_diff_perc
# MAGIC FROM exs_table e
# MAGIC FULL OUTER JOIN new_table n
# MAGIC  ON e.session_hour <=> n.session_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH exs_table AS (
# MAGIC   SELECT DATE_TRUNC('Hour', old_table.session_start) AS session_hour
# MAGIC   , CASE WHEN old_table.fk_content_id != 3468026 THEN 'Detected Session'
# MAGIC          WHEN COALESCE(old_table.tuner_channel_id, old_table.tuner_program_id) IS NOT NULL THEN 'Tuner'
# MAGIC          WHEN old_table.vizio_epg_airing IS NOT NULL THEN 'WF+'
# MAGIC          ELSE 'z-Null Session' END AS session_type
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT old_table.fk_tvid) AS total_tvs
# MAGIC  , SUM(session_duration)/3600.0 AS ttl_hrs
# MAGIC  FROM prod.table_backup.viewing_content_firehose_08_14_24 AS old_table
# MAGIC  WHERE session_start >= '2024-08-25T00:00:00'
# MAGIC AND fk_zoo_id = 17
# MAGIC  GROUP BY 1, 2
# MAGIC )
# MAGIC , new_table AS (
# MAGIC   SELECT DATE_TRUNC('Hour', new_table.session_start) AS session_hour
# MAGIC   , CASE WHEN new_table.fk_content_id != 3468026 THEN 'Detected Session'
# MAGIC          WHEN COALESCE(new_table.tms_tuner_channel_id, new_table.tms_tuner_program_id, new_table.tuner_channel_id, new_table.tuner_program_id) IS NOT NULL THEN 'Tuner'
# MAGIC          WHEN new_table.vizio_epg_airing IS NOT NULL THEN 'WF+'
# MAGIC          ELSE 'z-Null Session' END AS session_type
# MAGIC  , COUNT(*) AS session_count
# MAGIC  , COUNT(DISTINCT new_table.fk_tvid) AS total_tvs
# MAGIC  , SUM(session_duration)/3600.0 AS ttl_hrs
# MAGIC  FROM prod.detection.viewing_content_firehose AS new_table
# MAGIC  WHERE session_start >= '2024-08-25T00:00:00'
# MAGIC AND fk_zoo_id = 17
# MAGIC  GROUP BY 1, 2
# MAGIC )
# MAGIC SELECT NVL(e.session_hour, n.session_hour) AS session_hour
# MAGIC , NVL(e.session_type, n.session_type) AS session_type
# MAGIC , e.session_count AS existing_session_count
# MAGIC , n.session_count AS new_session_count
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC , e.ttl_hrs AS existing_ttl_hrs
# MAGIC , n.ttl_hrs AS new_ttl_hrs
# MAGIC , (e.session_count - n.session_count)/(e.session_count*.01) AS session_count_diff_perc
# MAGIC , (e.total_tvs - n.total_tvs)/(e.total_tvs*.01) AS tv_count_diff_perc
# MAGIC , (e.ttl_hrs - n.ttl_hrs)/(e.ttl_hrs*.01) AS duration_diff_perc
# MAGIC FROM exs_table e
# MAGIC FULL OUTER JOIN new_table n
# MAGIC  ON e.session_hour <=> n.session_hour
# MAGIC  AND e.session_type <=> n.session_type

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DATE_TRUNC('HOUR', session_start), COUNT(*), MIN(session_start), MAX(session_start)
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC WHERE session_start >= '2024-07-24T16:00:00'
# MAGIC GROUP BY 1;

# COMMAND ----------

# DBTITLE 1,Overall Test
# MAGIC %sql
# MAGIC WITH existing_vcf AS (
# MAGIC   SELECT DATE_TRUNC('DAY', session_start) AS session_hour
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , ROUND(AVG(session_duration*1.0), 2) AS avg_sessions_duration
# MAGIC   , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC   , ROUND(SUM(session_duration)/3600.0, 0) AS total_duration
# MAGIC   , ROUND(1.0*SUM(CASE WHEN fk_content_id != 3468026 THEN session_duration END) / (total_duration*36.0), 3) AS dect_rate
# MAGIC   , COUNT(DISTINCT fk_show_id) AS total_shows
# MAGIC   , COUNT(DISTINCT fk_station_id) AS total_stations
# MAGIC FROM (
# MAGIC   SELECT vc.fk_tvid, session_start, session_end, session_duration, runtime, fk_content_id, fk_show_id, fk_station_id, ROW_NUMBER() OVER (PARTITION BY vc.fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC   FROM prod.table_backup.viewing_content_firehose_08_14_24 vc
# MAGIC   WHERE session_start>='2024-08-01T00:00:00'
# MAGIC     AND fk_zoo_id=17
# MAGIC     AND partition_key >= '2024-08-01'
# MAGIC     AND session_duration > 0
# MAGIC   ) vc
# MAGIC WHERE vc.rn = 1
# MAGIC GROUP BY 1)
# MAGIC , new_vcf AS
# MAGIC (SELECT DATE_TRUNC('DAY', session_start) AS session_hour
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC , ROUND(AVG(session_duration*1.0), 2) AS avg_sessions_duration
# MAGIC , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC , ROUND(SUM(session_duration)/3600.0, 0) AS total_duration
# MAGIC , ROUND(1.0*SUM(CASE WHEN fk_content_id != 3468026 THEN session_duration END) / (total_duration*36.0), 3) AS dect_rate
# MAGIC , COUNT(DISTINCT fk_show_id) AS total_shows
# MAGIC , COUNT(DISTINCT fk_station_id) AS total_stations
# MAGIC FROM (
# MAGIC   SELECT vc.fk_tvid, session_start, session_end, session_duration, runtime, fk_content_id, fk_show_id, fk_station_id, ROW_NUMBER() OVER (PARTITION BY vc.fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC   FROM prod.detection.viewing_content_firehose vc
# MAGIC   WHERE session_start>='2024-08-01T00:00:00'
# MAGIC     AND fk_zoo_id=17
# MAGIC     AND partition_key >= '2024-08-01'
# MAGIC     AND session_duration > 0
# MAGIC   ) vc
# MAGIC WHERE vc.rn = 1
# MAGIC
# MAGIC GROUP BY 1)
# MAGIC SELECT e.session_hour
# MAGIC , e.sessions_count AS existing_sessions_count
# MAGIC , n.sessions_count AS new_sessions_count
# MAGIC -- , (e.sessions_count - n.sessions_count)/(e.sessions_count*.01) AS diff_in_sessions_count
# MAGIC , e.avg_sessions_duration AS existing_avg_sessions_duration
# MAGIC , n.avg_sessions_duration AS new_avg_sessions_duration
# MAGIC -- , (e.avg_sessions_duration - n.avg_sessions_duration)/(e.avg_sessions_duration*.01) AS diff_in_avg_sessions_duration
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC -- , (e.total_tvs - n.total_tvs)/(e.total_tvs*.01) AS diff_in_total_tvs
# MAGIC , e.total_duration AS existing_total_duration
# MAGIC , n.total_duration AS new_total_duration
# MAGIC -- , (e.total_duration - n.total_duration)/(e.total_duration*.01) AS diff_in_total_duration
# MAGIC , e.dect_rate AS existing_dect_rate
# MAGIC , n.dect_rate AS new_dect_rate
# MAGIC -- , (e.dect_rate - n.dect_rate)/(e.dect_rate*.01) AS diff_in_dect_rate
# MAGIC , e.total_shows AS existing_total_shows
# MAGIC , n.total_shows AS new_total_shows
# MAGIC -- , (e.total_shows - n.total_shows)/(e.total_shows*.01) AS diff_in_total_shows
# MAGIC , e.total_stations AS existing_total_stations
# MAGIC , n.total_stations AS new_total_stations
# MAGIC -- , (e.total_stations - n.total_stations)/(e.total_stations*.01) AS diff_in_total_stations
# MAGIC FROM existing_vcf e
# MAGIC LEFT JOIN new_vcf n ON e.session_hour=n.session_hour

# COMMAND ----------

# DBTITLE 1,Existing Missing Prev + Next Station Count
# MAGIC %sql
# MAGIC WITH existing_vcf AS (
# MAGIC   SELECT DATE_TRUNC('DAY', session_start) AS session_hour
# MAGIC   , CASE WHEN fk_content_id = 3468026 THEN 'Null Session' ELSE 'Detected Session' END AS session_type
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC   , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC   , ROUND(SUM(session_duration)/3600.0, 0) AS total_duration
# MAGIC   FROM (
# MAGIC     SELECT vc.fk_tvid, session_start, session_end, session_duration, fk_content_id, ROW_NUMBER() OVER (PARTITION BY vc.fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM prod.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC     ) vc
# MAGIC   WHERE vc.rn = 1
# MAGIC   GROUP BY 1,2
# MAGIC )
# MAGIC , new_vcf AS (
# MAGIC   SELECT DATE_TRUNC('DAY', session_start) AS session_hour
# MAGIC   , CASE WHEN fk_content_id = 3468026 THEN 'Null Session' ELSE 'Detected Session' END AS session_type
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC   , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC   , ROUND(SUM(session_duration)/3600.0, 0) AS total_duration
# MAGIC   FROM (
# MAGIC     SELECT vc.fk_tvid, session_start, session_end, session_duration, fk_content_id, ROW_NUMBER() OVER (PARTITION BY vc.fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM dev.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC   ) vc
# MAGIC   WHERE vc.rn = 1
# MAGIC   GROUP BY 1,2
# MAGIC )
# MAGIC SELECT e.session_hour
# MAGIC , e.session_type
# MAGIC , e.sessions_count AS existing_sessions_count
# MAGIC , n.sessions_count AS new_sessions_count
# MAGIC -- , (e.sessions_count - n.sessions_count)/(e.sessions_count*.01) AS diff_in_sessions_count
# MAGIC , e.avg_sessions_duration AS existing_avg_sessions_duration
# MAGIC , n.avg_sessions_duration AS new_avg_sessions_duration
# MAGIC -- , (e.avg_sessions_duration - n.avg_sessions_duration)/(e.avg_sessions_duration*.01) AS diff_in_avg_sessions_duration
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC -- , (e.total_tvs - n.total_tvs)/(e.total_tvs*.01) AS diff_in_total_tvs
# MAGIC , e.total_duration AS existing_total_duration
# MAGIC , n.total_duration AS new_total_duration
# MAGIC -- , (e.total_duration - n.total_duration)/(e.total_duration*.01) AS diff_in_total_duration
# MAGIC FROM existing_vcf e
# MAGIC LEFT JOIN new_vcf n ON e.session_hour=n.session_hour AND e.session_type = n.session_type

# COMMAND ----------

# DBTITLE 1,Live v Timeshifted Sessions
# MAGIC %sql
# MAGIC WITH existing_vcf AS (
# MAGIC   SELECT DATE_TRUNC('DAY', session_start) AS session_hour
# MAGIC   , CASE WHEN is_live THEN 'Live Session' ELSE 'TimeShifted Session' END AS liveness
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC   , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC   , ROUND(SUM(session_duration)/3600.0, 0) AS total_duration
# MAGIC   FROM (
# MAGIC     SELECT vc.fk_tvid, session_start, session_end, session_duration, is_live, ROW_NUMBER() OVER (PARTITION BY vc.fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM prod.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC       AND fk_content_id != 3468026
# MAGIC   ) vc
# MAGIC   WHERE vc.rn = 1
# MAGIC   GROUP BY 1,2
# MAGIC )
# MAGIC , new_vcf AS (
# MAGIC   SELECT DATE_TRUNC('DAY', session_start) AS session_hour
# MAGIC   , CASE WHEN is_live THEN 'Live Session' ELSE 'TimeShifted Session' END AS liveness
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC   , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC   , ROUND(SUM(session_duration)/3600.0, 0) AS total_duration
# MAGIC   FROM (
# MAGIC     SELECT vc.fk_tvid, session_start, session_end, session_duration, is_live, ROW_NUMBER() OVER (PARTITION BY vc.fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM dev.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC       AND fk_content_id != 3468026
# MAGIC   ) vc
# MAGIC   WHERE vc.rn = 1
# MAGIC   GROUP BY 1,2
# MAGIC )
# MAGIC SELECT e.session_hour
# MAGIC , e.liveness
# MAGIC , e.sessions_count AS existing_sessions_count
# MAGIC , n.sessions_count AS new_sessions_count
# MAGIC -- , (e.sessions_count - n.sessions_count)/(e.sessions_count*.01) AS diff_in_sessions_count
# MAGIC , e.avg_sessions_duration AS existing_avg_sessions_duration
# MAGIC , n.avg_sessions_duration AS new_avg_sessions_duration
# MAGIC -- , (e.avg_sessions_duration - n.avg_sessions_duration)/(e.avg_sessions_duration*.01) AS diff_in_avg_sessions_duration
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC -- , (e.total_tvs - n.total_tvs)/(e.total_tvs*.01) AS diff_in_total_tvs
# MAGIC , e.total_duration AS existing_total_duration
# MAGIC , n.total_duration AS new_total_duration
# MAGIC -- , (e.total_duration - n.total_duration)/(e.total_duration*.01) AS diff_in_total_duration
# MAGIC FROM existing_vcf e
# MAGIC LEFT JOIN new_vcf n ON e.session_hour=n.session_hour AND e.liveness = n.liveness

# COMMAND ----------

# DBTITLE 1,Missing TiVo Values From New Table
# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('DAY', exisiting_content.session_start) AS session_hour
# MAGIC , CASE WHEN exisiting_content.fk_show_id IS NULL AND new_content.fk_show_id IS NULL THEN 'Both Tables Null'
# MAGIC        WHEN exisiting_content.fk_show_id IS NULL AND new_content.fk_show_id IS NOT NULL THEN 'show_id Null in Existing Table'
# MAGIC        WHEN exisiting_content.fk_show_id IS NOT NULL AND new_content.fk_show_id IS NULL THEN 'show_id Null in New Table'
# MAGIC        WHEN exisiting_content.fk_show_id IS NOT NULL AND new_content.fk_show_id IS NOT NULL THEN 'show_id Present in Both Tables' END AS show_id_check
# MAGIC , CASE WHEN exisiting_content.fk_station_id IS NULL AND new_content.fk_station_id IS NULL THEN 'Both Tables Null'
# MAGIC        WHEN exisiting_content.fk_station_id IS NULL AND new_content.fk_station_id IS NOT NULL THEN 'station_id Null in Existing Table'
# MAGIC        WHEN exisiting_content.fk_station_id IS NOT NULL AND new_content.fk_station_id IS NULL THEN 'station_id Null in New Table'
# MAGIC        WHEN exisiting_content.fk_station_id IS NOT NULL AND new_content.fk_station_id IS NOT NULL THEN 'station_id Present in Both Tables' END AS station_id_check
# MAGIC , CASE WHEN exisiting_content.airdate IS NULL AND new_content.airdate IS NULL THEN 'Both Tables Null'
# MAGIC        WHEN exisiting_content.airdate IS NULL AND new_content.airdate IS NOT NULL THEN 'airdate Null in Existing Table'
# MAGIC        WHEN exisiting_content.airdate IS NOT NULL AND new_content.airdate IS NULL THEN 'airdate Null in New Table'
# MAGIC        WHEN exisiting_content.airdate IS NOT NULL AND new_content.airdate IS NOT NULL THEN 'airdate Present in Both Tables' END AS airdate_check
# MAGIC , CASE WHEN new_content.fk_tvid IS NULL THEN 'Missing Row in New Table' ELSE 'Row Present in Both Tables' END AS missing_row
# MAGIC , COUNT(*) AS row_count
# MAGIC FROM (
# MAGIC   SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC   FROM (
# MAGIC     SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM prod.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC   ) vc
# MAGIC   WHERE rn = 1
# MAGIC ) AS exisiting_content
# MAGIC LEFT JOIN (
# MAGIC   SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC   FROM (
# MAGIC     SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM dev.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC   ) vc
# MAGIC   WHERE rn = 1
# MAGIC ) AS new_content
# MAGIC   ON new_content.fk_tvid = exisiting_content.fk_tvid
# MAGIC  AND new_content.session_end = exisiting_content.session_end
# MAGIC  AND new_content.session_start = exisiting_content.session_start
# MAGIC GROUP BY 1, 2, 3, 4,5
# MAGIC ORDER BY 1

# COMMAND ----------

# DBTITLE 1,Missing TiVo Values From the Existing Table
# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('DAY', new_content.session_start) AS session_hour
# MAGIC , CASE WHEN new_content.fk_show_id IS NULL AND exisiting_content.fk_show_id IS NULL THEN 'Both Tables Null'
# MAGIC        WHEN new_content.fk_show_id IS NULL AND exisiting_content.fk_show_id IS NOT NULL THEN 'show_id Null in Existing Table'
# MAGIC        WHEN new_content.fk_show_id IS NOT NULL AND exisiting_content.fk_show_id IS NULL THEN 'show_id Null in New Table'
# MAGIC        WHEN new_content.fk_show_id IS NOT NULL AND exisiting_content.fk_show_id IS NOT NULL THEN 'show_id Present in Both Tables' END AS show_id_check
# MAGIC , CASE WHEN new_content.fk_station_id IS NULL AND exisiting_content.fk_station_id IS NULL THEN 'Both Tables Null'
# MAGIC        WHEN new_content.fk_station_id IS NULL AND exisiting_content.fk_station_id IS NOT NULL THEN 'station_id Null in Existing Table'
# MAGIC        WHEN new_content.fk_station_id IS NOT NULL AND exisiting_content.fk_station_id IS NULL THEN 'station_id Null in New Table'
# MAGIC        WHEN new_content.fk_station_id IS NOT NULL AND exisiting_content.fk_station_id IS NOT NULL THEN 'station_id Present in Both Tables' END AS station_id_check
# MAGIC , CASE WHEN new_content.airdate IS NULL AND exisiting_content.airdate IS NULL THEN 'Both Tables Null'
# MAGIC        WHEN new_content.airdate IS NULL AND exisiting_content.airdate IS NOT NULL THEN 'airdate Null in Existing Table'
# MAGIC        WHEN new_content.airdate IS NOT NULL AND exisiting_content.airdate IS NULL THEN 'airdate Null in New Table'
# MAGIC        WHEN new_content.airdate IS NOT NULL AND exisiting_content.airdate IS NOT NULL THEN 'airdate Present in Both Tables' END AS airdate_check
# MAGIC , CASE WHEN exisiting_content.fk_tvid IS NULL THEN 'Missing Row in Existing Table' ELSE 'Row Present in Both Tables' END AS missing_row
# MAGIC , COUNT(*) AS row_count
# MAGIC FROM (
# MAGIC   SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC   FROM (
# MAGIC     SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM dev.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC   ) vc
# MAGIC   WHERE rn = 1
# MAGIC ) AS new_content
# MAGIC LEFT JOIN (
# MAGIC   SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC   FROM (
# MAGIC     SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM prod.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC   ) vc
# MAGIC   WHERE rn = 1
# MAGIC ) AS exisiting_content
# MAGIC   ON new_content.fk_tvid = exisiting_content.fk_tvid
# MAGIC  AND new_content.session_end = exisiting_content.session_end
# MAGIC  AND new_content.session_start = exisiting_content.session_start
# MAGIC GROUP BY 1, 2, 3, 4,5
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('DAY', new_content.session_start) AS session_hour
# MAGIC , CASE WHEN new_content.fk_show_id IS NULL AND exisiting_content.fk_show_id IS NULL THEN 'Both Tables Null'
# MAGIC        WHEN new_content.fk_show_id IS NULL AND exisiting_content.fk_show_id IS NOT NULL THEN 'show_id Null in Existing Table'
# MAGIC        WHEN new_content.fk_show_id IS NOT NULL AND exisiting_content.fk_show_id IS NULL THEN 'show_id Null in New Table'
# MAGIC        WHEN new_content.fk_show_id IS NOT NULL AND exisiting_content.fk_show_id IS NOT NULL THEN 'show_id Present in Both Tables' END AS show_id_check
# MAGIC , CASE WHEN new_content.fk_station_id IS NULL AND exisiting_content.fk_station_id IS NULL THEN 'Both Tables Null'
# MAGIC        WHEN new_content.fk_station_id IS NULL AND exisiting_content.fk_station_id IS NOT NULL THEN 'station_id Null in Existing Table'
# MAGIC        WHEN new_content.fk_station_id IS NOT NULL AND exisiting_content.fk_station_id IS NULL THEN 'station_id Null in New Table'
# MAGIC        WHEN new_content.fk_station_id IS NOT NULL AND exisiting_content.fk_station_id IS NOT NULL THEN 'station_id Present in Both Tables' END AS station_id_check
# MAGIC , CASE WHEN new_content.airdate IS NULL AND exisiting_content.airdate IS NULL THEN 'Both Tables Null'
# MAGIC        WHEN new_content.airdate IS NULL AND exisiting_content.airdate IS NOT NULL THEN 'airdate Null in Existing Table'
# MAGIC        WHEN new_content.airdate IS NOT NULL AND exisiting_content.airdate IS NULL THEN 'airdate Null in New Table'
# MAGIC        WHEN new_content.airdate IS NOT NULL AND exisiting_content.airdate IS NOT NULL THEN 'airdate Present in Both Tables' END AS airdate_check
# MAGIC , CASE WHEN exisiting_content.fk_tvid IS NULL THEN 'Missing Row in Existing Table' ELSE 'Row Present in Both Tables' END AS missing_row
# MAGIC , COUNT(*) AS row_count
# MAGIC FROM (
# MAGIC   SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC   FROM (
# MAGIC     SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM dev.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC   ) vc
# MAGIC   WHERE rn = 1
# MAGIC ) AS new_content
# MAGIC LEFT JOIN (
# MAGIC   SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC   FROM (
# MAGIC     SELECT fk_tvid, session_start, session_end, session_duration, fk_show_id, fk_station_id, airdate
# MAGIC     , ROW_NUMBER() OVER (PARTITION BY fk_tvid, session_start, session_end ORDER BY COALESCE(fk_station_id, fk_content_id)) AS rn
# MAGIC     FROM prod.detection.viewing_content_firehose vc
# MAGIC     WHERE session_start>='2024-08-22T00:00:00'
# MAGIC       AND fk_zoo_id=17
# MAGIC       AND partition_key >= '2024-08-22'
# MAGIC       AND session_duration > 0
# MAGIC   ) vc
# MAGIC   WHERE rn = 1
# MAGIC ) AS exisiting_content
# MAGIC   ON new_content.fk_tvid = exisiting_content.fk_tvid
# MAGIC  AND new_content.session_end = exisiting_content.session_end
# MAGIC  AND new_content.session_start = exisiting_content.session_start
# MAGIC GROUP BY 1, 2, 3, 4,5
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('DAY', session_start) AS session_hour
# MAGIC , CASE WHEN tms_station_id IS NOT NULL AND fk_station_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_station_id IS NOT NULL AND fk_station_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_station_id IS NULL AND fk_station_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_station_id IS NULL AND fk_station_id IS NULL THEN '0. Both Null' END AS station_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC , SUM(session_duration)/3600.0 AS total_duration
# MAGIC , COUNT(DISTINCT fk_show_id) AS total_tivo_shows
# MAGIC , COUNT(DISTINCT fk_station_id) AS total_tivo_stations
# MAGIC , COUNT(DISTINCT tms_show_id) AS total_tms_shows
# MAGIC , COUNT(DISTINCT tms_station_id) AS total_tms_stations
# MAGIC FROM (
# MAGIC   SELECT vc.fk_tvid, vc.session_start, vc.session_end, vc.session_duration, vc.fk_station_id, vc.tms_station_id, vc.fk_show_id, vc.tms_show_id
# MAGIC   , ROW_NUMBER() OVER (PARTITION BY vc.fk_tvid, vc.session_start, vc.session_end ORDER BY COALESCE(vc.fk_station_id, vc.fk_content_id)) AS rn
# MAGIC   FROM dev.detection.viewing_content_firehose vc
# MAGIC   WHERE session_start>='2024-08-20T00:00:00'
# MAGIC     AND fk_zoo_id=17
# MAGIC     AND partition_key >= '2024-08-20'
# MAGIC     AND session_duration > 0
# MAGIC   ) vc
# MAGIC WHERE vc.rn = 1
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', session_start) AS session_hour
# MAGIC , CASE WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NULL THEN '0. Both Null' END AS station_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC , SUM(session_duration)/3600.0 AS total_duration
# MAGIC , COUNT(DISTINCT tuner_program_id) AS total_tivo_shows
# MAGIC , COUNT(DISTINCT tuner_channel_id) AS total_tivo_stations
# MAGIC , COUNT(DISTINCT tms_tuner_program_id) AS total_tms_shows
# MAGIC , COUNT(DISTINCT tms_tuner_channel_id) AS total_tms_stations
# MAGIC -- FROM (
# MAGIC --   SELECT vc.fk_tvid, vc.session_start, vc.session_end, vc.session_duration, vc.tuner_channel_id, vc.tms_tuner_channel_id, vc.fk_show_id, vc.tms_show_id
# MAGIC --   , ROW_NUMBER() OVER (PARTITION BY vc.fk_tvid, vc.session_start, vc.session_end ORDER BY COALESCE(vc.tuner_channel_id, vc.fk_content_id)) AS rn
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC WHERE session_start >= '2023-05-01T00:00:00'
# MAGIC   AND session_start < '2024-04-01T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2022-05-01'
# MAGIC   AND partition_key <= '2024-04-01'
# MAGIC   AND session_duration > 0
# MAGIC   AND coalesce(tms_tuner_channel_id, tuner_channel_id) IS NOT NULL
# MAGIC GROUP BY 1, 2
# MAGIC --   ) vc
# MAGIC -- WHERE vc.rn = 1
# MAGIC -- GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', session_start) AS session_hour
# MAGIC , CASE WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NULL THEN '0. Both Null' END AS station_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC -- , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC -- , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC -- , SUM(session_duration)/3600.0 AS total_duration
# MAGIC -- , COUNT(DISTINCT tuner_program_id) AS total_tivo_shows
# MAGIC -- , COUNT(DISTINCT tuner_channel_id) AS total_tivo_stations
# MAGIC -- , COUNT(DISTINCT tms_tuner_program_id) AS total_tms_shows
# MAGIC -- , COUNT(DISTINCT tms_tuner_channel_id) AS total_tms_stations
# MAGIC -- FROM (
# MAGIC --   SELECT vc.fk_tvid, vc.session_start, vc.session_end, vc.session_duration, vc.tuner_channel_id, vc.tms_tuner_channel_id, vc.fk_show_id, vc.tms_show_id
# MAGIC --   , ROW_NUMBER() OVER (PARTITION BY vc.fk_tvid, vc.session_start, vc.session_end ORDER BY COALESCE(vc.tuner_channel_id, vc.fk_content_id)) AS rn
# MAGIC   FROM prod.detection.viewing_content_firehose vc
# MAGIC WHERE session_start >= '2023-05-01T00:00:00'
# MAGIC   AND session_start < '2024-04-01T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2023-05-01'
# MAGIC   AND partition_key <= '2024-04-01'
# MAGIC     AND session_duration > 0
# MAGIC     AND coalesce(tms_tuner_channel_id, tuner_channel_id) IS NOT NULL
# MAGIC GROUP BY 1, 2
# MAGIC --   ) vc
# MAGIC -- WHERE vc.rn = 1
# MAGIC -- GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', session_start) AS session_hour
# MAGIC , CASE WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NULL THEN '0. Both Null' END AS show_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC -- , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC -- , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC -- , SUM(session_duration)/3600.0 AS total_duration
# MAGIC -- , COUNT(DISTINCT tuner_program_id) AS total_tivo_shows
# MAGIC -- , COUNT(DISTINCT tuner_channel_id) AS total_tivo_stations
# MAGIC -- , COUNT(DISTINCT tms_tuner_program_id) AS total_tms_shows
# MAGIC -- , COUNT(DISTINCT tms_tuner_channel_id) AS total_tms_stations
# MAGIC   FROM prod.detection.viewing_content_firehose vc
# MAGIC WHERE session_start >= '2023-11-01T00:00:00'
# MAGIC   AND session_start < '2024-08-31T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2023-11-01'
# MAGIC   AND partition_key <= '2024-08-31'
# MAGIC     AND session_duration > 0
# MAGIC     AND coalesce(tms_tuner_program_id, tuner_program_id) IS NOT NULL
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', session_start) AS session_time
# MAGIC , CASE WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NULL THEN '0. Both Null' END AS show_id_check
# MAGIC , CASE WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NULL THEN '0. Both Null' END AS station_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC -- , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC -- , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC -- , SUM(session_duration)/3600.0 AS total_duration
# MAGIC -- , COUNT(DISTINCT tuner_program_id) AS total_tivo_shows
# MAGIC -- , COUNT(DISTINCT tuner_channel_id) AS total_tivo_stations
# MAGIC -- , COUNT(DISTINCT tms_tuner_program_id) AS total_tms_shows
# MAGIC -- , COUNT(DISTINCT tms_tuner_channel_id) AS total_tms_stations
# MAGIC FROM prod.detection.viewing_content_firehose vc
# MAGIC WHERE session_start >= '2023-11-01T00:00:00'
# MAGIC   AND session_start < '2024-08-31T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2023-11-01'
# MAGIC   AND partition_key <= '2024-08-31'
# MAGIC   AND session_duration > 0
# MAGIC   AND coalesce(tms_tuner_program_id, tuner_program_id, tms_tuner_channel_id, tuner_channel_id) IS NOT NULL
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', session_start) AS session_hour
# MAGIC , CASE WHEN st.station_id IS NULL THEN 'Bad Station Mapping' ELSE 'Good Station Mapping' END AS station_id_check
# MAGIC , CASE WHEN sh.show_id IS NULL THEN 'Bad Show Mapping' ELSE 'Good Show Mapping' END AS show_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC -- , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC -- , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC -- , SUM(session_duration)/3600.0 AS total_duration
# MAGIC , COUNT(DISTINCT tuner_program_id) AS total_tivo_shows
# MAGIC , COUNT(DISTINCT tuner_channel_id) AS total_tivo_stations
# MAGIC   FROM prod.detection.viewing_content_firehose vc
# MAGIC   LEFT JOIN detection.epg_station st
# MAGIC     ON st.station_id = vc.tuner_channel_id
# MAGIC    AND st.vendor_name = 'TIVO'
# MAGIC   LEFT JOIN prod.detection.epg_show sh
# MAGIC     ON sh.show_id = vc.tuner_program_id
# MAGIC    AND sh.vendor_name = 'TIVO'
# MAGIC WHERE session_start >= '2023-11-01T00:00:00'
# MAGIC   AND session_start < '2024-08-31T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2023-11-01'
# MAGIC   AND partition_key <= '2024-08-31'
# MAGIC     AND session_duration > 0
# MAGIC     AND coalesce(tuner_channel_id, tuner_program_id) IS NOT NULL
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', session_start) AS session_hour
# MAGIC , CASE WHEN vc.tms_tuner_channel_id IS NOT NULL AND st.station_id IS NULL THEN 'Bad Station Mapping' ELSE 'Good Station Mapping' END AS station_id_check
# MAGIC , CASE WHEN vc.tms_tuner_program_id IS NOT NULL AND sh.show_id    IS NULL THEN 'Bad Show Mapping'    ELSE 'Good Show Mapping'    END AS show_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC -- , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC -- , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC -- , SUM(session_duration)/3600.0 AS total_duration
# MAGIC -- , COUNT(DISTINCT tms_tuner_program_id) AS total_shows
# MAGIC -- , COUNT(DISTINCT tms_tuner_channel_id) AS total_stations
# MAGIC FROM prod.detection.viewing_content_firehose vc
# MAGIC LEFT JOIN prod.detection.epg_station st
# MAGIC   ON st.station_id = vc.tms_tuner_channel_id
# MAGIC   AND st.vendor_name = 'TMS'
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.tms_tuner_program_id
# MAGIC   AND sh.vendor_name = 'TMS'
# MAGIC WHERE session_start >= '2023-11-01T00:00:00'
# MAGIC   AND session_start < '2024-08-31T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2023-11-01'
# MAGIC   AND partition_key <= '2024-08-31'
# MAGIC     AND session_duration > 0
# MAGIC     AND tms_tuner_program_id IS NOT NULL
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('DAY', session_start) AS session_time
# MAGIC , CASE WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NULL THEN '0. Both Null' END AS show_id_check
# MAGIC , CASE WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NULL THEN '0. Both Null' END AS station_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC -- , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC -- , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC -- , SUM(session_duration)/3600.0 AS total_duration
# MAGIC -- , COUNT(DISTINCT tuner_program_id) AS total_tivo_shows
# MAGIC -- , COUNT(DISTINCT tuner_channel_id) AS total_tivo_stations
# MAGIC -- , COUNT(DISTINCT tms_tuner_program_id) AS total_tms_shows
# MAGIC -- , COUNT(DISTINCT tms_tuner_channel_id) AS total_tms_stations
# MAGIC FROM prod.detection.viewing_content_firehose vc
# MAGIC WHERE session_start >= '2024-08-01T00:00:00'
# MAGIC   AND session_start < '2024-09-30T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2024-08-01'
# MAGIC   AND partition_key <= '2024-09-30'
# MAGIC   AND session_duration > 0
# MAGIC   AND coalesce(tms_tuner_program_id, tuner_program_id, tms_tuner_channel_id, tuner_channel_id) IS NOT NULL
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('HOUR', session_start) AS session_time
# MAGIC , CASE WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NULL THEN '0. Both Null' END AS show_id_check
# MAGIC , CASE WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NULL THEN '0. Both Null' END AS station_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC -- , AVG(session_duration*1.0) AS avg_sessions_duration
# MAGIC -- , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC -- , SUM(session_duration)/3600.0 AS total_duration
# MAGIC -- , COUNT(DISTINCT tuner_program_id) AS total_tivo_shows
# MAGIC -- , COUNT(DISTINCT tuner_channel_id) AS total_tivo_stations
# MAGIC -- , COUNT(DISTINCT tms_tuner_program_id) AS total_tms_shows
# MAGIC -- , COUNT(DISTINCT tms_tuner_channel_id) AS total_tms_stations
# MAGIC FROM prod.detection.viewing_content_firehose vc
# MAGIC WHERE session_start >= '2024-08-08T00:00:00'
# MAGIC   AND session_start < '2024-08-11T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2024-08-08'
# MAGIC   AND partition_key <= '2024-08-11'
# MAGIC   AND session_duration > 0
# MAGIC   AND coalesce(tms_tuner_program_id, tuner_program_id, tms_tuner_channel_id, tuner_channel_id) IS NOT NULL
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', session_start) AS session_hour
# MAGIC , st.station_call_sign
# MAGIC -- , sh.title
# MAGIC , COUNT(*)
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC LEFT JOIN detection.epg_station st
# MAGIC   ON st.station_id = vc.tms_tuner_channel_id
# MAGIC   AND st.vendor_name = 'TMS'
# MAGIC -- LEFT JOIN detection.epg_show sh
# MAGIC --   ON sh.show_id = vc.tms_tuner_program_id
# MAGIC --   AND sh.vendor_name = 'TMS'
# MAGIC WHERE session_start >= '2023-05-01T00:00:00'
# MAGIC   AND session_start < '2024-04-01T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2023-05-01'
# MAGIC   AND partition_key <= '2024-04-01'
# MAGIC   AND session_duration > 0
# MAGIC   AND tms_tuner_channel_id IS NOT NULL
# MAGIC   AND tms_tuner_channel_id != 98989898
# MAGIC   AND st.station_id IS NULL
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.detection.viewing_content_firehose vc
# MAGIC WHERE session_start >= '2024-08-09T00:00:00'
# MAGIC   AND session_start < '2024-08-09T16:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key = '2024-08-09'
# MAGIC   AND session_duration > 0
# MAGIC   AND fk_tvid=26630180
# MAGIC ORDER BY fk_tvid, session_start
# MAGIC LIMIT 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', session_start) AS session_hour
# MAGIC -- , st.station_call_sign
# MAGIC , sh.title
# MAGIC , COUNT(*)
# MAGIC -- , COUNT(DISTINCT vc.tms_tuner_program_id)
# MAGIC -- SELECT vc.*
# MAGIC FROM prod.detection.viewing_content_firehose vc
# MAGIC LEFT JOIN detection.epg_show sh
# MAGIC   ON sh.show_id = vc.tms_tuner_program_id
# MAGIC   AND sh.vendor_name = 'TMS'
# MAGIC JOIN detection.epg_show tivo_sh
# MAGIC   ON tivo_sh.show_id = vc.tms_tuner_program_id
# MAGIC   AND tivo_sh.vendor_name = 'TIVO'
# MAGIC WHERE session_start >= '2023-05-01T00:00:00'
# MAGIC   AND session_start < '2024-04-01T00:00:00'
# MAGIC   AND fk_zoo_id=17
# MAGIC   AND partition_key >= '2023-05-01'
# MAGIC   AND partition_key <= '2024-04-01'
# MAGIC   AND session_duration > 0
# MAGIC   AND tms_tuner_program_id IS NOT NULL
# MAGIC   AND tms_tuner_program_id != 98989898
# MAGIC   AND sh.show_id IS NULL
# MAGIC   -- AND tivo_sh.show_id IS NOT NULL
# MAGIC -- LIMIT 100
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM detection.epg_show
# MAGIC WHERE show_id IN (2339460,2337135,
# MAGIC 2327746,
# MAGIC 2325135,
# MAGIC 2323684,
# MAGIC 2399963,
# MAGIC 2344280,
# MAGIC 2338870)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('HOUR', ex_table.session_start) AS session_hour
# MAGIC , CASE WHEN ex_table.fk_content_id = 3468026 THEN 'Null Session' ELSE 'Detected Session' END AS session_type
# MAGIC , CASE WHEN ex_table.is_live THEN 'Live Session' WHEN ex_table.fk_content_id != 3468026 AND NOT ex_table.is_live THEN 'Time-Shifted Session' ELSE 'Null Session' END AS session_type
# MAGIC , CASE WHEN new_table.fk_tvid IS NULL THEN 'Missing Row From New Table' END AS missing_row
# MAGIC , CASE WHEN ex_table.is_live != new_table.is_live THEN 'Liveness Mismatch' END AS liveness_match
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC , AVG(ex_table.session_duration*1.0) AS avg_sessions_duration
# MAGIC , COUNT(Distinct ex_table.fk_tvid) AS total_tvs
# MAGIC , SUM(ex_table.session_duration)/3600.0 AS total_duration
# MAGIC FROM prod.detection.viewing_content_firehose ex_table
# MAGIC LEFT JOIN dev.detection.viewing_content_firehose new_table
# MAGIC   ON ex_table.fk_tvid = new_table.fk_tvid
# MAGIC  AND ex_table.session_start = new_table.session_start
# MAGIC  AND ex_table.session_end = new_table.session_end
# MAGIC  AND new_table.session_start>='2024-08-20T00:00:00'
# MAGIC  AND new_table.partition_key >= '2024-08-20'
# MAGIC  AND new_table.fk_zoo_id=17
# MAGIC WHERE ex_table.session_start>='2024-08-20T00:00:00'
# MAGIC   -- AND ex_table.session_start<'2024-07-31T14:00:00'
# MAGIC   AND ex_table.partition_key >= '2024-08-20'
# MAGIC   AND ex_table.fk_zoo_id=17
# MAGIC   -- AND ex_table.vizio_epg_airing IS NULL
# MAGIC GROUP BY 1,2,3,4,5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('HOUR', new_table.session_start) AS session_hour
# MAGIC , CASE WHEN new_table.fk_content_id = 3468026 THEN 'Null Session' ELSE 'Detected Session' END AS session_type
# MAGIC , CASE WHEN new_table.is_live THEN 'Live Session' WHEN new_table.fk_content_id != 3468026 AND NOT new_table.is_live THEN 'Time-Shifted Session' ELSE 'Null Session' END AS session_type
# MAGIC , CASE WHEN ex_table.fk_tvid IS NULL THEN 'Missing Row From Existing Table' END AS missing_row
# MAGIC , CASE WHEN new_table.is_live != ex_table.is_live THEN 'Liveness Mismatch' END AS liveness_match
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC , AVG(new_table.session_duration*1.0) AS avg_sessions_duration
# MAGIC , COUNT(Distinct new_table.fk_tvid) AS total_tvs
# MAGIC , SUM(new_table.session_duration)/3600.0 AS total_duration
# MAGIC FROM dev.detection.viewing_content_firehose new_table
# MAGIC LEFT JOIN prod.detection.viewing_content_firehose ex_table
# MAGIC   ON new_table.fk_tvid = ex_table.fk_tvid
# MAGIC  AND new_table.session_start = ex_table.session_start
# MAGIC  AND new_table.session_end = ex_table.session_end
# MAGIC  AND ex_table.session_start>='2024-08-20T00:00:00'
# MAGIC  AND ex_table.partition_key >= '2024-08-20'
# MAGIC  AND ex_table.fk_zoo_id=17
# MAGIC WHERE new_table.session_start>='2024-08-20T00:00:00'
# MAGIC   -- AND new_table.session_start<'2024-07-31T14:00:00'
# MAGIC   AND new_table.partition_key >= '2024-08-20'
# MAGIC   AND new_table.fk_zoo_id=17
# MAGIC GROUP BY 1,2,3,4,5
# MAGIC ORDER BY 1, 2, 3, 4, 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT new_table.*
# MAGIC FROM dev.detection.viewing_content_firehose new_table
# MAGIC LEFT JOIN prod.detection.viewing_content_firehose ex_table
# MAGIC   ON new_table.fk_tvid = ex_table.fk_tvid
# MAGIC  AND new_table.session_start = ex_table.session_start
# MAGIC  AND new_table.session_end = ex_table.session_end
# MAGIC  AND ex_table.session_start>='2024-08-20T19:00:00'
# MAGIC  AND ex_table.partition_key >= '2024-08-20'
# MAGIC  AND ex_table.fk_zoo_id=17
# MAGIC WHERE new_table.session_start>='2024-08-20T19:00:00'
# MAGIC   -- AND new_table.session_start<'2024-07-31T14:00:00'
# MAGIC   AND new_table.partition_key >= '2024-08-20'
# MAGIC   AND new_table.fk_zoo_id=17
# MAGIC   AND ex_table.fk_tvid IS NULL
# MAGIC ORDER BY new_table.fk_tvid, new_table.session_start
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ex_table.*
# MAGIC FROM prod.detection.viewing_content_firehose ex_table
# MAGIC LEFT JOIN dev.detection.viewing_content_firehose new_table
# MAGIC   ON new_table.fk_tvid = ex_table.fk_tvid
# MAGIC  AND new_table.session_start = ex_table.session_start
# MAGIC  AND new_table.session_end = ex_table.session_end
# MAGIC  AND new_table.session_start>='2024-07-29T19:00:00'
# MAGIC  AND new_table.partition_key >= '2024-07-29'
# MAGIC  AND new_table.fk_zoo_id=17
# MAGIC WHERE ex_table.session_start>='2024-07-29T19:00:00'
# MAGIC   AND ex_table.session_start<'2024-07-31T14:00:00'
# MAGIC   AND ex_table.partition_key >= '2024-07-29'
# MAGIC   AND ex_table.fk_zoo_id=17
# MAGIC   AND new_table.fk_tvid IS NULL
# MAGIC ORDER BY ex_table.fk_tvid, ex_table.session_start
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC (SELECT 'Existing VCF' AS table_name, fk_tvid, session_start, session_end, session_duration, media_time_start, fk_station_id, fk_show_id, airdate, tuner_channel_id, vizio_epg_airing
# MAGIC FROM prod.detection.viewing_content_firehose vc
# MAGIC WHERE session_start >= '2024-07-29T19:00:00'
# MAGIC AND session_start<'2024-07-31T14:00:00'
# MAGIC   AND partition_key >= '2024-07-29'
# MAGIC AND fk_tvid IN (21381661, 21726633, 21919905)
# MAGIC --, 21960333, 22493971, 22945992, 23066570, 23129076, 23165462, 23340527, 23449972, 23699902, 23730119, 23761822, 23887265, 24132853, 24408652, 24664395, 25030410, 25078656, 25275394, 25559283, 25571260, 25655887, 25905361, 25910965, 25920916, 26244685, 26265392, 26397414, 26468970, 26569057, 26654985, 26664225, 26708061, 26743463, 26808584, 26811907, 26814748, 26841930, 26863931, 26928531, 26934095, 26943108, 26944283, 27009574, 27037137, 27099014, 27119513, 27124648, 27167730, 27169797, 27191202, 27206411, 27208725, 27256419, 27294197, 27441346, 27520498, 27558295, 27646996, 27656739, 27676923, 27685491, 27719884, 27725725, 27726610)
# MAGIC )
# MAGIC UNION
# MAGIC (SELECT 'New VCF' AS table_name, fk_tvid, session_start, session_end, session_duration, media_time_start, fk_station_id, fk_show_id, airdate, tuner_channel_id, vizio_epg_airing
# MAGIC FROM dev.detection.viewing_content_firehose
# MAGIC WHERE session_start >= '2024-07-29T19:00:00'
# MAGIC AND session_start<'2024-07-31T14:00:00'
# MAGIC   AND partition_key >= '2024-07-29'
# MAGIC AND fk_tvid IN (21381661, 21726633, 21919905)
# MAGIC --, 21960333, 22493971, 22945992, 23066570, 23129076, 23165462, 23340527, 23449972, 23699902, 23730119, 23761822, 23887265, 24132853, 24408652, 24664395, 25030410, 25078656, 25275394, 25559283, 25571260, 25655887, 25905361, 25910965, 25920916, 26244685, 26265392, 26397414, 26468970, 26569057, 26654985, 26664225, 26708061, 26743463, 26808584, 26811907, 26814748, 26841930, 26863931, 26928531, 26934095, 26943108, 26944283, 27009574, 27037137, 27099014, 27119513, 27124648, 27167730, 27169797, 27191202, 27206411, 27208725, 27256419, 27294197, 27441346, 27520498, 27558295, 27646996, 27656739, 27676923, 27685491, 27719884, 27725725, 27726610)
# MAGIC )
# MAGIC ORDER BY 2, 3, 1

# COMMAND ----------

# DBTITLE 1,Duplicate Session Count
# MAGIC %sql
# MAGIC SELECT table_name, cnt, COUNT(*)
# MAGIC FROM (
# MAGIC   (
# MAGIC   SELECT fk_tvid, session_start, session_end, 'New VCF' AS table_name, COUNT(*) AS cnt
# MAGIC   FROM dev.detection.viewing_content_firehose
# MAGIC   WHERE session_start >= '2024-07-29T19:00:00'
# MAGIC   AND session_start<'2024-07-31T14:00:00'
# MAGIC     AND partition_key >= '2024-07-29'
# MAGIC     AND fk_zoo_id = 17
# MAGIC     AND session_duration > 0
# MAGIC   GROUP BY 1, 2, 3)
# MAGIC   UNION
# MAGIC   (SELECT fk_tvid, session_start, session_end, 'Existing VCF' AS table_name, COUNT(*) AS cnt
# MAGIC   FROM prod.detection.viewing_content_firehose
# MAGIC   WHERE session_start >= '2024-07-29T19:00:00'
# MAGIC   AND session_start<'2024-07-31T14:00:00'
# MAGIC     AND partition_key >= '2024-07-29'
# MAGIC     AND fk_zoo_id = 17
# MAGIC     AND session_duration > 0
# MAGIC   GROUP BY 1, 2, 3)
# MAGIC )
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT vc.*
# MAGIC FROM (SELECT fk_tvid, session_start, session_end, COUNT(*) AS cnt
# MAGIC   FROM prod.detection.viewing_content_firehose
# MAGIC   WHERE session_start >= '2024-07-29T19:00:00'
# MAGIC   AND session_start<'2024-07-31T14:00:00'
# MAGIC     AND partition_key >= '2024-07-29'
# MAGIC     AND fk_zoo_id = 17
# MAGIC     AND session_duration > 0
# MAGIC   GROUP BY 1, 2, 3) a
# MAGIC JOIN prod.detection.viewing_content_firehose vc
# MAGIC   ON vc.fk_tvid = a.fk_tvid
# MAGIC  AND a.session_start = vc.session_start
# MAGIC  AND a.session_end = vc.session_end
# MAGIC WHERE a.cnt >= 5
# MAGIC AND vc.session_start >= '2024-07-29T19:00:00'
# MAGIC   AND vc.session_start<'2024-07-31T14:00:00'
# MAGIC     AND vc.partition_key >= '2024-07-29'
# MAGIC     AND vc.fk_zoo_id = 17
# MAGIC     AND session_duration > 0
# MAGIC ORDER BY vc.fk_tvid, vc.session_start, vc.session_end

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.tms_tuner_schedule_id IS NULL AS tms_tuner_schedule_id
# MAGIC , c.tms_tuner_channel_id IS NULL AS tms_tuner_channel_id
# MAGIC , c.tms_tuner_program_id IS NULL AS tms_tuner_program_id
# MAGIC , c.tuner_schedule_id IS NULL AS tuner_schedule_id
# MAGIC , c.tuner_channel_id IS NULL AS  tuner_channel_id
# MAGIC , c.tuner_program_id IS NULL AS  tuner_program_id
# MAGIC , COUNT(*)
# MAGIC FROM dev.detection.viewing_content_firehose c
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC GROUP BY 1, 2, 3, 4, 5, 6

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.fk_station_id IS NULL, c.fk_show_id IS NULL, c.airdate IS NULL, c.fk_schedule_id IS NULL, COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC -- AND COALESCE(c.tms_tuner_channel_id, c.tms_tuner_program_id, c.tuner_channel_id, c.tuner_program_id) IS NOT NULL
# MAGIC GROUP BY 1, 2, 3, 4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.tms_station_id IS NULL, c.tms_show_id IS NULL, c.tms_airdate IS NULL, c.tms_schedule_id IS NULL, COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND COALESCE(c.tms_tuner_channel_id, c.tms_tuner_program_id, c.tuner_channel_id, c.tuner_program_id) IS NOT NULL
# MAGIC GROUP BY 1, 2, 3, 4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sh.show_id IS NULL, COUNT(*), SUM(c.session_duration)/(3600*24)
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON sh.show_id = c.fk_show_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.fk_show_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sh.show_id IS NULL, COUNT(*), SUM(c.session_duration)/(3600*24)
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON sh.show_id = c.tms_show_id
# MAGIC  AND sh.vendor_name = 'TMS'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.tms_show_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC (SELECT 'TMS' AS vendor_name, DATE_TRUNC('HOUR', c.session_start) AS session_hour, st.station_id IS NULL AS missing_value, COUNT(*) AS session_count
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_station st
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON st.station_id = c.tms_tuner_channel_id
# MAGIC  AND st.vendor_name = 'TMS'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.tms_tuner_channel_id IS NOT NULL
# MAGIC GROUP BY 1, 2, 3)
# MAGIC UNION
# MAGIC (SELECT 'TiVo' AS vendor_name, DATE_TRUNC('HOUR', c.session_start) AS session_hour, st.station_id IS NULL AS missing_value, COUNT(*) AS session_count
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_station st
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON st.station_id = c.tuner_channel_id
# MAGIC  AND st.vendor_name = 'TIVO'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.tuner_channel_id IS NOT NULL
# MAGIC GROUP BY 1, 2, 3)

# COMMAND ----------

# MAGIC %sql
# MAGIC (SELECT 'TMS' AS vendor_name, DATE_TRUNC('HOUR', c.session_start) AS session_hour, st.station_id IS NULL AS missing_value, COUNT(*) AS session_count
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_station st
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON st.station_id = c.tms_tuner_channel_id
# MAGIC  AND st.vendor_name = 'TMS'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.tms_tuner_channel_id IS NOT NULL
# MAGIC GROUP BY 1, 2, 3)
# MAGIC UNION
# MAGIC (SELECT 'TiVo' AS vendor_name, DATE_TRUNC('HOUR', c.session_start) AS session_hour, st.station_id IS NULL AS missing_value, COUNT(*) AS session_count
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_station st
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON st.station_id = c.tuner_channel_id
# MAGIC  AND st.vendor_name = 'TIVO'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.tuner_channel_id IS NOT NULL
# MAGIC GROUP BY 1, 2, 3)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sh.show_id IS NULL, COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON sh.show_id = c.tms_tuner_program_id
# MAGIC  AND sh.vendor_name = 'TMS'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.tms_tuner_program_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sh.show_id IS NULL, COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON sh.show_id = c.tuner_program_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.tuner_program_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT st.station_id IS NULL, COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_station st
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON st.station_id = c.fk_station_id
# MAGIC  AND st.vendor_name = 'TIVO'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.fk_station_id IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sch.schedule_id IS NULL, COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_schedule sch
# MAGIC   -- ON sch.schedule_id = c.fk_schedule_id
# MAGIC   ON sch.fk_show_id = c.fk_show_id
# MAGIC   AND sch.airdate = c.airdate
# MAGIC   AND sch.fk_station_id = c.fk_station_id
# MAGIC  AND sch.vendor_name = 'TIVO'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.fk_show_id IS NOT NULL
# MAGIC AND c.fk_station_id IS NOT NULL
# MAGIC AND c.airdate IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ISNOTNULL(sch.schedule_id), COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose c
# MAGIC LEFT JOIN prod.detection.epg_schedule sch
# MAGIC   ON sch.schedule_id = c.tms_schedule_id
# MAGIC   -- ON sch.fk_show_id = c.tms_show_id
# MAGIC   -- AND sch.airdate = c.tms_airdate
# MAGIC   -- AND sch.fk_station_id = c.tms_station_id
# MAGIC  AND sch.vendor_name = 'TMS'
# MAGIC WHERE c.session_start >= '2024-08-06T00:00:00'::TIMESTAMP
# MAGIC AND c.fk_zoo_id = 17
# MAGIC AND c.tms_show_id IS NOT NULL
# MAGIC AND c.tms_station_id IS NOT NULL
# MAGIC AND c.tms_airdate IS NOT NULL
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(session_start)
# MAGIC , CASE WHEN fk_station_id IS NOT NULL AND tms_station_id IS NOT NULL THEN 'TIVO + TMS not null'
# MAGIC        WHEN fk_station_id IS NULL AND tms_station_id IS NOT NULL THEN 'TIVO not null, TMS null'
# MAGIC        WHEN fk_station_id IS NOT NULL AND tms_station_id IS NULL THEN 'TIVO null, TMS not null'
# MAGIC        WHEN fk_station_id IS NULL AND tms_station_id IS NULL THEN 'TIVO + TMS null' END AS station_id_check
# MAGIC , COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC WHERE session_start >= '2024-08-06T00:00:00.000'
# MAGIC AND partition_key >= '2024-08-06'
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM stage.detection.epg_station WHERE station_id IN (91488, 90366, 93839)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM detection.inscape_station_map WHERE inscape_call_sign = 'BBCAHD'

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_tvid AS (
# MAGIC   SELECT tvid, token
# MAGIC   , ROW_NUMBER() OVER (PARTITION BY token ORDER BY joined_date DESC) AS rn
# MAGIC   FROM detection.tv
# MAGIC   WHERE tv.oem = 'VIZIO'
# MAGIC ),
# MAGIC one_year_active AS (
# MAGIC   SELECT fk_tvid
# MAGIC   FROM detection.tv_activity ta
# MAGIC   WHERE ta.session_end >= CURRENT_DATE - 365
# MAGIC     AND TIMESTAMPDIFF(SECOND, ta.session_start, ta.session_end) > 0
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC -- SELECT COUNT(DISTINCT ld.token)
# MAGIC SELECT MOD(ld.tvid, 23), COUNT(DISTINCT ld.token)
# MAGIC FROM latest_tvid ld
# MAGIC JOIN one_year_active ta
# MAGIC   ON ta.fk_tvid = ld.tvid
# MAGIC JOIN prod.detection.tv_zoo_latest_daily tv_zoo
# MAGIC   ON tv_zoo.tvid = ld.tvid
# MAGIC  AND tv_zoo.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
# MAGIC JOIN prod.detection.tv_settings_latest_daily AS tv_settings
# MAGIC   ON ld.tvid = tv_settings.tvid
# MAGIC  AND UPPER(tv_settings.country_name) = 'USA'
# MAGIC JOIN prod.detection.tv_populations AS u
# MAGIC   ON ld.tvid = u.fk_tvid
# MAGIC JOIN prod.detection.populations AS pop
# MAGIC   ON u.fk_population_id = pop.population_id 
# MAGIC  AND pop.population_name = 'opted_in'
# MAGIC JOIN prod.detection.tv_geolocation_latest_daily tv_geo
# MAGIC   ON ld.tvid = tv_geo.tvid
# MAGIC  AND UPPER(tv_geo.country_code) = 'US'
# MAGIC WHERE ld.rn = 1
# MAGIC GROUP BY 1
