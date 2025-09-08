# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT DATE_TRUNC('HOUR', session_start), COUNT(*)--, MIN(session_start), MAX(session_start)
# MAGIC FROM dev.detection.viewing_commercials_firehose
# MAGIC WHERE session_start >= '2024-08-07T16:00:00'
# MAGIC AND fk_zoo_id = 17
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 DESC;

# COMMAND ----------

# DBTITLE 1,Overall Test
# MAGIC %sql
# MAGIC WITH existing_vcf AS (
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , AVG(vc.session_duration*1.0) AS avg_sessions_duration
# MAGIC   , COUNT(Distinct vc.fk_tvid) AS total_tvs
# MAGIC   , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_commercial_id) AS dist_comm_count
# MAGIC   FROM prod.detection.viewing_commercials_firehose vc
# MAGIC WHERE vc.session_start >= '2024-08-07T16:00:00'
# MAGIC   AND vc.session_start < '2024-08-08T12:00:00.000'
# MAGIC   AND vc.partition_key >= '2024-08-07'
# MAGIC -- WHERE vc.session_start>='2024-08-02T00:00:00'
# MAGIC --   AND vc.session_start < '2024-08-07T00:00:00'
# MAGIC   AND vc.fk_zoo_id=17
# MAGIC   -- AND vc.partition_key >= '2024-08-02'
# MAGIC   -- AND vc.partition_key <= '2024-08-07'
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC GROUP BY 1)
# MAGIC , new_vcf AS(
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , AVG(vc.session_duration*1.0) AS avg_sessions_duration
# MAGIC   , COUNT(Distinct fk_tvid) AS total_tvs
# MAGIC   , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_commercial_id) AS dist_comm_count
# MAGIC   FROM dev.detection.viewing_commercials_firehose vc
# MAGIC WHERE vc.session_start >= '2024-08-07T16:00:00'
# MAGIC   AND vc.session_start < '2024-08-08T12:00:00.000'
# MAGIC   AND vc.partition_key >= '2024-08-07'
# MAGIC -- WHERE vc.session_start>='2024-08-02T00:00:00'
# MAGIC --   AND vc.session_start < '2024-08-07T00:00:00'
# MAGIC   AND vc.fk_zoo_id=17
# MAGIC   -- AND vc.partition_key >= '2024-08-02'
# MAGIC   -- AND vc.partition_key <= '2024-08-07'
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC   GROUP BY 1)
# MAGIC SELECT e.session_hour
# MAGIC , e.sessions_count AS existing_sessions_count
# MAGIC , n.sessions_count AS new_sessions_count
# MAGIC , e.avg_sessions_duration AS existing_avg_sessions_duration
# MAGIC , n.avg_sessions_duration AS new_avg_sessions_duration
# MAGIC , e.total_tvs AS existing_total_tvs
# MAGIC , n.total_tvs AS new_total_tvs
# MAGIC , e.total_duration AS existing_total_duration
# MAGIC , n.total_duration AS new_total_duration
# MAGIC , e.dist_comm_count AS existing_dist_comm_count
# MAGIC , n.dist_comm_count AS new_dist_comm_count
# MAGIC FROM existing_vcf e
# MAGIC LEFT JOIN new_vcf n ON e.session_hour=n.session_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH existing_vcf AS (
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , CASE WHEN vc.prev_show_id IS NULL AND vc.next_show_id IS NULL THEN 'Prev and Next Null'
# MAGIC         WHEN vc.prev_show_id IS NULL AND vc.next_show_id IS NOT NULL THEN 'Prev Null, Next not Null'
# MAGIC         WHEN vc.prev_show_id IS NOT NULL AND vc.next_show_id IS NULL THEN 'Prev not Null, Next Null'
# MAGIC         WHEN vc.prev_show_id IS NOT NULL AND vc.next_show_id IS NOT NULL THEN 'Prev and Next not Null'
# MAGIC   END AS show_id_check
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , COUNT(Distinct vc.fk_tvid) AS total_tvs
# MAGIC   , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_commercial_id) AS dist_comm_count
# MAGIC   FROM prod.detection.viewing_commercials_firehose vc
# MAGIC   WHERE vc.session_start >= '2024-08-07T16:00:00'
# MAGIC     AND vc.session_start < '2024-08-08T12:00:00.000'
# MAGIC     AND vc.partition_key >= '2024-08-07'
# MAGIC   -- WHERE vc.session_start>='2024-08-02T00:00:00'
# MAGIC   --   AND vc.session_start < '2024-08-07T00:00:00'
# MAGIC     AND vc.fk_zoo_id=17
# MAGIC     -- AND vc.partition_key >= '2024-08-02'
# MAGIC     -- AND vc.partition_key <= '2024-08-07'
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC   GROUP BY 1, 2)
# MAGIC , new_vcf AS (
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , CASE WHEN vc.prev_show_id IS NULL AND vc.next_show_id IS NULL THEN 'Prev and Next Null'
# MAGIC          WHEN vc.prev_show_id IS NULL AND vc.next_show_id IS NOT NULL THEN 'Prev Null, Next not Null'
# MAGIC          WHEN vc.prev_show_id IS NOT NULL AND vc.next_show_id IS NULL THEN 'Prev not Null, Next Null'
# MAGIC          WHEN vc.prev_show_id IS NOT NULL AND vc.next_show_id IS NOT NULL THEN 'Prev and Next not Null'
# MAGIC   END AS show_id_check
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , COUNT(Distinct vc.fk_tvid) AS total_tvs
# MAGIC   , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_commercial_id) AS dist_comm_count
# MAGIC   FROM dev.detection.viewing_commercials_firehose vc
# MAGIC   WHERE vc.session_start >= '2024-08-07T16:00:00'
# MAGIC   AND vc.session_start < '2024-08-08T12:00:00.000'
# MAGIC     AND vc.partition_key >= '2024-08-07'
# MAGIC   -- WHERE vc.session_start>='2024-08-02T00:00:00'
# MAGIC   --   AND vc.session_start < '2024-08-07T00:00:00'
# MAGIC     AND vc.fk_zoo_id=17
# MAGIC     -- AND vc.partition_key >= '2024-08-02'
# MAGIC     -- AND vc.partition_key <= '2024-08-07'
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC GROUP BY 1, 2)
# MAGIC SELECT e.session_hour
# MAGIC , e.show_id_check
# MAGIC , e.sessions_count AS existing_sessions_count
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , CASE WHEN vc.prev_show_id IS NULL AND vc.next_show_id IS NULL THEN 'Prev and Next Null'
# MAGIC          WHEN vc.prev_show_id IS NULL AND vc.next_show_id IS NOT NULL THEN 'Prev Null, Next not Null'
# MAGIC          WHEN vc.prev_show_id IS NOT NULL AND vc.next_show_id IS NULL THEN 'Prev not Null, Next Null'
# MAGIC          WHEN vc.prev_show_id IS NOT NULL AND vc.next_show_id IS NOT NULL THEN 'Prev and Next not Null'
# MAGIC   END AS show_id_check
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , COUNT(Distinct vc.fk_tvid) AS total_tvs
# MAGIC   , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_commercial_id) AS dist_comm_count
# MAGIC   FROM dev.detection.viewing_commercials_firehose vc
# MAGIC   WHERE vc.session_start >= '2024-08-07T16:00:00'
# MAGIC   AND vc.session_start < '2024-08-08T12:00:00.000'
# MAGIC     AND vc.partition_key >= '2024-08-07'
# MAGIC   -- WHERE vc.session_start>='2024-08-02T00:00:00'
# MAGIC   --   AND vc.session_start < '2024-08-07T00:00:00'
# MAGIC     AND vc.fk_zoo_id=17
# MAGIC     -- AND vc.partition_key >= '2024-08-02'
# MAGIC     -- AND vc.partition_key <= '2024-08-07'
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , CASE WHEN vc.tms_prev_show_id IS NULL     AND vc.tms_next_show_id IS NULL THEN 'Prev and Next Null'
# MAGIC          WHEN vc.tms_prev_show_id IS NULL     AND vc.tms_next_show_id IS NOT NULL THEN 'Prev Null, Next not Null'
# MAGIC          WHEN vc.tms_prev_show_id IS NOT NULL AND vc.tms_next_show_id IS NULL THEN 'Prev not Null, Next Null'
# MAGIC          WHEN vc.tms_prev_show_id IS NOT NULL AND vc.tms_next_show_id IS NOT NULL THEN 'Prev and Next not Null'
# MAGIC   END AS show_id_check
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , COUNT(Distinct vc.fk_tvid) AS total_tvs
# MAGIC   , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_commercial_id) AS dist_comm_count
# MAGIC   FROM dev.detection.viewing_commercials_firehose vc
# MAGIC   WHERE vc.session_start >= '2024-08-07T16:00:00'
# MAGIC   AND vc.session_start < '2024-08-08T12:00:00.000'
# MAGIC     AND vc.partition_key >= '2024-08-07'
# MAGIC   -- WHERE vc.session_start>='2024-08-02T00:00:00'
# MAGIC   --   AND vc.session_start < '2024-08-07T00:00:00'
# MAGIC     AND vc.fk_zoo_id=17
# MAGIC     -- AND vc.partition_key >= '2024-08-02'
# MAGIC     -- AND vc.partition_key <= '2024-08-07'
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# DBTITLE 1,TMS + TiVo Prev Show ID Check
# MAGIC %sql
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , CASE WHEN vc.prev_show_id IS NULL     AND vc.tms_prev_show_id IS NULL THEN 'TiVo and TMS Null'
# MAGIC          WHEN vc.prev_show_id IS NULL     AND vc.tms_prev_show_id IS NOT NULL THEN 'TiVo Null, TMS not Null'
# MAGIC          WHEN vc.prev_show_id IS NOT NULL AND vc.tms_prev_show_id IS NULL THEN 'TiVo not Null, TMS Null'
# MAGIC          WHEN vc.prev_show_id IS NOT NULL AND vc.tms_prev_show_id IS NOT NULL THEN 'TiVo and TMS not Null'
# MAGIC   END AS show_id_check
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , COUNT(Distinct vc.fk_tvid) AS total_tvs
# MAGIC   , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_commercial_id) AS dist_comm_count
# MAGIC   FROM dev.detection.viewing_commercials_firehose vc
# MAGIC   WHERE vc.session_start >= '2024-08-07T16:00:00'
# MAGIC   AND vc.session_start < '2024-08-08T12:00:00.000'
# MAGIC     AND vc.partition_key >= '2024-08-07'
# MAGIC   -- WHERE vc.session_start>='2024-08-02T00:00:00'
# MAGIC   --   AND vc.session_start < '2024-08-07T00:00:00'
# MAGIC     AND vc.fk_zoo_id=17
# MAGIC     -- AND vc.partition_key >= '2024-08-02'
# MAGIC     -- AND vc.partition_key <= '2024-08-07'
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# DBTITLE 1,TMS + TiVo Next Show ID Check
# MAGIC %sql
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , CASE WHEN vc.next_show_id IS NULL     AND vc.tms_next_show_id IS NULL THEN 'TiVo and TMS Null'
# MAGIC          WHEN vc.next_show_id IS NULL     AND vc.tms_next_show_id IS NOT NULL THEN 'TiVo Null, TMS not Null'
# MAGIC          WHEN vc.next_show_id IS NOT NULL AND vc.tms_next_show_id IS NULL THEN 'TiVo not Null, TMS Null'
# MAGIC          WHEN vc.next_show_id IS NOT NULL AND vc.tms_next_show_id IS NOT NULL THEN 'TiVo and TMS not Null'
# MAGIC   END AS show_id_check
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , COUNT(Distinct vc.fk_tvid) AS total_tvs
# MAGIC   , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_commercial_id) AS dist_comm_count
# MAGIC   FROM dev.detection.viewing_commercials_firehose vc
# MAGIC   WHERE vc.session_start >= '2024-08-07T16:00:00'
# MAGIC   AND vc.session_start < '2024-08-08T12:00:00.000'
# MAGIC     AND vc.partition_key >= '2024-08-07'
# MAGIC   -- WHERE vc.session_start>='2024-08-02T00:00:00'
# MAGIC   --   AND vc.session_start < '2024-08-07T00:00:00'
# MAGIC     AND vc.fk_zoo_id=17
# MAGIC     -- AND vc.partition_key >= '2024-08-02'
# MAGIC     -- AND vc.partition_key <= '2024-08-07'
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# DBTITLE 1,TMS + TiVo Previous Show ID Check
# MAGIC %sql
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , CASE WHEN vc.prev_station_id IS NULL     AND vc.tms_prev_station_id IS NULL THEN 'TiVo and TMS Null'
# MAGIC          WHEN vc.prev_station_id IS NULL     AND vc.tms_prev_station_id IS NOT NULL THEN 'TiVo Null, TMS not Null'
# MAGIC          WHEN vc.prev_station_id IS NOT NULL AND vc.tms_prev_station_id IS NULL THEN 'TiVo not Null, TMS Null'
# MAGIC          WHEN vc.prev_station_id IS NOT NULL AND vc.tms_prev_station_id IS NOT NULL THEN 'TiVo and TMS not Null'
# MAGIC   END AS station_id_check
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   , COUNT(Distinct vc.fk_tvid) AS total_tvs
# MAGIC   , SUM(vc.session_duration)/3600.0 AS total_duration
# MAGIC   , COUNT(DISTINCT vc.fk_commercial_id) AS dist_comm_count
# MAGIC   FROM dev.detection.viewing_commercials_firehose vc
# MAGIC   WHERE vc.session_start >= '2024-08-07T16:00:00'
# MAGIC   AND vc.session_start < '2024-08-08T12:00:00.000'
# MAGIC     AND vc.partition_key >= '2024-08-07'
# MAGIC   -- WHERE vc.session_start>='2024-08-02T00:00:00'
# MAGIC   --   AND vc.session_start < '2024-08-07T00:00:00'
# MAGIC     AND vc.fk_zoo_id=17
# MAGIC     -- AND vc.partition_key >= '2024-08-02'
# MAGIC     -- AND vc.partition_key <= '2024-08-07'
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC GROUP BY 1, 2

# COMMAND ----------

, CASE WHEN tms_prev_tuner_program_id IS NOT NULL AND prev_tuner_program_id IS NOT NULL THEN '0. In Both'
       WHEN tms_prev_tuner_program_id IS NOT NULL AND prev_tuner_program_id IS NULL THEN '9. TMS present, not TiVo'
       WHEN tms_prev_tuner_program_id IS NULL AND prev_tuner_program_id IS NOT NULL THEN '3. TiVo present not TMS'
       WHEN tms_prev_tuner_program_id IS NULL AND prev_tuner_program_id IS NULL THEN '0. Both Null' END AS prev_show_id_check
, CASE WHEN tms_next_tuner_program_id IS NOT NULL AND next_tuner_program_id IS NOT NULL THEN '0. In Both'
       WHEN tms_next_tuner_program_id IS NOT NULL AND next_tuner_program_id IS NULL THEN '9. TMS present, not TiVo'
       WHEN tms_next_tuner_program_id IS NULL AND next_tuner_program_id IS NOT NULL THEN '3. TiVo present not TMS'
       WHEN tms_next_tuner_program_id IS NULL AND next_tuner_program_id IS NULL THEN '0. Both Null' END AS next_show_id_check
, CASE WHEN tms_prev_tuner_channel_id IS NOT NULL AND prev_tuner_channel_id IS NOT NULL THEN '0. In Both'
       WHEN tms_prev_tuner_channel_id IS NOT NULL AND prev_tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
       WHEN tms_prev_tuner_channel_id IS NULL AND prev_tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
       WHEN tms_prev_tuner_channel_id IS NULL AND prev_tuner_channel_id IS NULL THEN '0. Both Null' END AS prev_station_id_check
, CASE WHEN tms_next_tuner_channel_id IS NOT NULL AND next_tuner_channel_id IS NOT NULL THEN '0. In Both'
     WHEN tms_next_tuner_channel_id IS NOT NULL AND next_tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
     WHEN tms_next_tuner_channel_id IS NULL AND next_tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
     WHEN tms_next_tuner_channel_id IS NULL AND next_tuner_channel_id IS NULL THEN '0. Both Null' END AS next_station_id_check

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SHOW COLUMNS FROM prod.detection.viewing_commercials_firehose

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SHOW COLUMNS FROM prod.historic.viewing_commercials_firehose

# COMMAND ----------

# DBTITLE 1,TMS + TiVo Next Station ID Check
# MAGIC %sql
# MAGIC   SELECT DATE_TRUNC('DAY', vc.session_start) AS session_hour
# MAGIC   , CASE WHEN prev_tms_tuner_program_id IS NOT NULL AND prev_tuner_program_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN prev_tms_tuner_program_id IS NOT NULL AND prev_tuner_program_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN prev_tms_tuner_program_id IS NULL AND prev_tuner_program_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN prev_tms_tuner_program_id IS NULL AND prev_tuner_program_id IS NULL THEN '0. Both Null' END AS prev_show_id_check
# MAGIC   , CASE WHEN next_tms_tuner_program_id IS NOT NULL AND next_tuner_program_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN next_tms_tuner_program_id IS NOT NULL AND next_tuner_program_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN next_tms_tuner_program_id IS NULL AND next_tuner_program_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN next_tms_tuner_program_id IS NULL AND next_tuner_program_id IS NULL THEN '0. Both Null' END AS next_show_id_check
# MAGIC   , CASE WHEN prev_tms_tuner_channel_id IS NOT NULL AND prev_tuner_channel_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN prev_tms_tuner_channel_id IS NOT NULL AND prev_tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN prev_tms_tuner_channel_id IS NULL AND prev_tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN prev_tms_tuner_channel_id IS NULL AND prev_tuner_channel_id IS NULL THEN '0. Both Null' END AS prev_station_id_check
# MAGIC   , CASE WHEN next_tms_tuner_channel_id IS NOT NULL AND next_tuner_channel_id IS NOT NULL THEN '0. In Both'
# MAGIC      WHEN next_tms_tuner_channel_id IS NOT NULL AND next_tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC      WHEN next_tms_tuner_channel_id IS NULL AND next_tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC      WHEN next_tms_tuner_channel_id IS NULL AND next_tuner_channel_id IS NULL THEN '0. Both Null' END AS next_station_id_check
# MAGIC   , COUNT(*)*1.0 AS sessions_count
# MAGIC   FROM prod.historic.viewing_commercials_firehose vc
# MAGIC   WHERE date_format(vc.partition_key,'yyyy-MM') in  ('2023-10', '2023-11', '2023-12','2024-01')
# MAGIC     AND vc.fk_zoo_id=17
# MAGIC     AND vc.session_duration > 0
# MAGIC     AND MOD(vc.fk_tvid, 100) = 0
# MAGIC GROUP BY 1, 2, 3, 4, 5

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC INSERT INTO dev.detection.viewing_commercials_firehose
# MAGIC  SELECT * FROM prod.detection.viewing_commercials_firehose
# MAGIC  WHERE partition_key in ('2024-01-10', '2024-01-15')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT new_table.*
# MAGIC FROM dev.detection.viewing_content_firehose new_table
# MAGIC LEFT JOIN prod.detection.viewing_content_firehose ex_table
# MAGIC   ON new_table.fk_tvid = ex_table.fk_tvid
# MAGIC  AND new_table.session_start = ex_table.session_start
# MAGIC  AND new_table.session_end = ex_table.session_end
# MAGIC  AND ex_table.session_start>='2024-07-29T19:00:00'
# MAGIC  AND ex_table.partition_key >= '2024-07-29'
# MAGIC  AND ex_table.fk_zoo_id=17
# MAGIC WHERE new_table.session_start>='2024-07-29T19:00:00'
# MAGIC   AND new_table.session_start<'2024-07-31T14:00:00'
# MAGIC   AND new_table.partition_key >= '2024-07-29'
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


