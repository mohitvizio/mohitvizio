# Databricks notebook source
# MAGIC %sql
# MAGIC -- SELECT COUNT(*)
# MAGIC SELECT *
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC WHERE tms_tuner_program_id IS NOT NULL
# MAGIC   AND tms_tuner_channel_id IS NOT NULL
# MAGIC   AND partition_key >= '2023-05-01'
# MAGIC   AND partition_key <= '2023-06-01'
# MAGIC
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT TIMESTAMPDIFF(DAY, airdate_tv2, session_start), COUNT(*)
# MAGIC SELECT DATE(session_start), COUNT(*)
# MAGIC FROM prod.detection.tuner_sessionized
# MAGIC WHERE session_start >= '2023-05-01 00:00:00'
# MAGIC   AND session_start < '2023-05-10 00:00:00'
# MAGIC AND airdate_tv2 IS NOT NULL
# MAGIC GROUP BY DATE(session_start) ORDER BY DATE(session_start)
# MAGIC LIMIT 1000
# MAGIC -- GROUP BY 1

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE {schema_name}.{table_name} AS
# MAGIC SELECT DISTINCT vc.fk_tvid
# MAGIC , vc.session_start
# MAGIC , vc.session_end
# MAGIC , sch.fk_station_id                                                          -- Will be used to fill in TiVo station id
# MAGIC , sch_lat.airdate                                                            -- Will be used to fill in TiVo airdate
# MAGIC , sch_lat.fk_show_id                                                         -- Will be used to fill in TiVo show id
# MAGIC , sch_lat.schedule_id                                                        -- Will be used to fill in TiVo schedule id
# MAGIC FROM prod.detection.viewing_content_firehose vc
# MAGIC JOIN prod.detection.inscape_station_map AS tivo_map
# MAGIC   ON tivo_map.inscape_station_id = vc.tms_tuner_channel_id
# MAGIC  AND tivo_map.mapped_vendor = 'TIVO'
# MAGIC JOIN prod.detection.epg_schedule_latest AS sch_lat
# MAGIC   ON sch_lat.fk_station_id = tivo_map.mapped_vendor_station_id
# MAGIC  AND sch_lat.vendor_name = 'TIVO'
# MAGIC  AND TIMESTAMPADD(SECOND, vc.media_time_start, vc.tms_airdate) > sch_lat.airdate
# MAGIC  AND TIMESTAMPADD(SECOND, vc.media_time_start, vc.tms_airdate) <= sch_lat.airdate_end
# MAGIC  AND sch_lat.airdate >= '2023-05-01 00:00:00'::TIMESTAMP - INTERVAL 2 DAYS
# MAGIC  AND sch_lat.airdate <= '2023-06-01 00:00:00'::TIMESTAMP + INTERVAL 1 DAY
# MAGIC WHERE vc.session_start >= '2023-05-01 00:00:00'          -- Start Date
# MAGIC   AND vc.session_start < '2023-06-01 00:00:00'           -- End Date
# MAGIC   AND vc.media_time_start IS NOT NULL
# MAGIC   AND vc.tms_airdate IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO prod.detection.viewing_content_firehose AS target
# MAGIC USING {schema_name}.{table_name} AS tivo_tuner
# MAGIC   ON target.fk_tvid = tivo_tuner.fk_tvid
# MAGIC  AND target.session_start = tivo_tuner.session_start
# MAGIC  AND target.session_end = tivo_tuner.session_end
# MAGIC WHEN MATCHED THEN UPDATE SET target.tuner_channel_id = tivo_tuner.fk_station_id
# MAGIC , target.tuner_program_id = tivo_tuner.fk_show_id
# MAGIC , target.tuner_schedule_id = tivo_tuner.schedule_id
# MAGIC , target.airdate = tivo_tuner.airdate
# MAGIC WHERE target.session_start >= '2023-05-01 00:00:00'          -- Start Date
# MAGIC   AND target.session_start < '2023-06-01 00:00:00'           -- End Date
# MAGIC   AND target.media_time_start IS NOT NULL
# MAGIC   AND target.tms_airdate IS NOT NULL
# MAGIC   AND target.tms_tuner_program_id IS NOT NULL
# MAGIC   AND target.tms_tuner_channel_id IS NOT NULL

# COMMAND ----------


