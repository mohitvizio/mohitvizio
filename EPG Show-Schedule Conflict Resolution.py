# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import date, datetime, timedelta

# COMMAND ----------

# MAGIC %run ./redshift_connect

# COMMAND ----------

redshift_read_env = set_redshift_params('warm')

# COMMAND ----------

query = f'''
SELECT show_id, database_key, title, vendor_name
FROM detection.epg_show
GROUP BY 1, 2, 3, 4
'''

redshift_show_df = query_redshift(query, redshift_read_env)
redshift_show_df.cache()

# COMMAND ----------

redshift_show_df.write.mode("overwrite").saveAsTable("dev.mohit_gangwani.redshift_epg_show")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(rds.*)
# MAGIC FROM dev.mohit_gangwani.redshift_epg_show rds
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = rds.show_id
# MAGIC --  AND sh.database_key != rds.database_key
# MAGIC  AND sh.vendor_name != rds.vendor_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rds.*, sh.show_id, sh.database_key, sh.title, sh.created_at
# MAGIC FROM dev.mohit_gangwani.redshift_epg_show rds
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = rds.show_id
# MAGIC  AND sh.database_key != rds.database_key
# MAGIC  AND sh.vendor_name = rds.vendor_name
# MAGIC WHERE sh.show_id IS NOT NULL
# MAGIC   AND rds.vendor_name = 'TIVO'
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT vendor_name, COUNT(*)
# MAGIC FROM prod.detection.viewing_content_firehose
# MAGIC JOIN prod.detection.epg_show
# MAGIC   ON fk_show_id = epg_show.show_id
# MAGIC WHERE session_start < ‘2024-04-01’
# MAGIC   AND partition_key < ‘2024-04-01’
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(session_start), MAX(session_start)
# MAGIC FROM prod.historic.viewing_content_firehose

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.historical_bad_database_keys;
# MAGIC CREATE TABLE dev.mohit_gangwani.historical_bad_database_keys AS
# MAGIC SELECT rds.show_id, rds.database_key, rds.title, vc.airdate, vc.fk_station_id, COUNT(*) AS sessions_affected
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC LEFT JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = vc.fk_show_id
# MAGIC   AND rds.vendor_name = 'TIVO'
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC   AND sh.vendor_name = 'TIVO'
# MAGIC   AND sh.database_key != rds.database_key
# MAGIC WHERE vc.fk_zoo_id=17
# MAGIC   AND vc.session_duration > 0
# MAGIC   AND vc.fk_show_id IS NOT NULL
# MAGIC   AND sh.show_id IS NOT NULL
# MAGIC   AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC GROUP BY 1, 2, 3, 4, 5

# COMMAND ----------

# MAGIC %md
# MAGIC # Issues to fix
# MAGIC 1. Show ID doesn't exist in prod.epg_show
# MAGIC 2. Show ID doesn't exist in prod.epg_show, but database does exist
# MAGIC 3. Show ID does exist in prod.epg_show, but it is not for TiVo
# MAGIC 4. Show ID does exist in prod.epg_show, and it is for TiVo, but the databse key doesn't align
# MAGIC
# MAGIC #### Steps:
# MAGIC 1. Create Four Tables - for all four conditions
# MAGIC     1. Show ID doesn't exist
# MAGIC     2. DB key exists for TiVo, but Show ID is different
# MAGIC     3. Show ID maps to TMS
# MAGIC     4. Show ID is for TiVo, but DB key is different
# MAGIC 2. Delete Condition 2 and Condition 3 show IDs from the table from Condition 1
# MAGIC 3. Insert the Table from Condition 1 to EPG show
# MAGIC 4. Create mapping table to change the following in viewing tables:
# MAGIC     1. Show ID is not present, but DB key is
# MAGIC 5. Insert the following in EPG show with new show ID and create mapping table to change the old with the new
# MAGIC     1. Show ID maps to TMs
# MAGIC     2. Show ID is for TiVo but the DB key is not the same and the DB key doesn't exist in EPG Show

# COMMAND ----------

# DBTITLE 1,Step 1: Condition 1: Find all missing Show IDs
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.missing_shows_in_epg_show_condition_one;
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_in_epg_show_condition_one AS
# MAGIC SELECT vc.fk_show_id, vc.airdate, vc.fk_station_id
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE vc.fk_zoo_id=17
# MAGIC   AND vc.session_duration > 0
# MAGIC   AND sh.show_id IS NULL
# MAGIC   AND vc.fk_show_id IS NOT NULL
# MAGIC   AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC   AND vc.partition_key < '2024-03-26'
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# DBTITLE 1,Step 1: Condition 2: Show ID doesn't exists, but the DB key is in EPG Show
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.historical_bad_show_id_condition_two;
# MAGIC CREATE TABLE dev.mohit_gangwani.historical_bad_show_id_condition_two AS
# MAGIC SELECT rds.show_id AS bad_show_id, sh.show_id AS existing_show_id, rds.database_key, vc.airdate, vc.fk_station_id
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = vc.fk_show_id
# MAGIC   AND rds.vendor_name = 'TIVO'
# MAGIC JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id != rds.show_id
# MAGIC   AND sh.vendor_name = 'TIVO'
# MAGIC   AND sh.database_key = rds.database_key
# MAGIC WHERE vc.fk_zoo_id=17
# MAGIC   AND vc.session_duration > 0
# MAGIC   AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC   AND vc.partition_key < '2024-03-26'
# MAGIC GROUP BY 1, 2, 3, 4, 5

# COMMAND ----------

# DBTITLE 1,Step 1: Condition 3: Show ID maps to TMS
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.historical_bad_show_id_condition_three;
# MAGIC CREATE TABLE dev.mohit_gangwani.historical_bad_show_id_condition_three AS
# MAGIC SELECT vc.fk_show_id, vc.airdate, vc.fk_station_id
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC   AND sh.vendor_name = 'TMS'
# MAGIC WHERE vc.fk_zoo_id=17
# MAGIC   AND vc.session_duration > 0
# MAGIC   AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC   AND vc.partition_key < '2024-03-26'
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# DBTITLE 1,Step 1: Condition 4: Show ID exists, DB Key is different
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.historical_bad_database_keys_condition_four;
# MAGIC CREATE TABLE dev.mohit_gangwani.historical_bad_database_keys_condition_four AS
# MAGIC SELECT rds.show_id, rds.database_key, rds.title, vc.airdate, vc.fk_station_id
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = vc.fk_show_id
# MAGIC   AND rds.vendor_name = 'TIVO'
# MAGIC JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC   AND sh.vendor_name = 'TIVO'
# MAGIC   AND sh.database_key != rds.database_key
# MAGIC WHERE vc.fk_zoo_id=17
# MAGIC   AND vc.session_duration > 0
# MAGIC   AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC   AND vc.partition_key < '2024-03-26'
# MAGIC GROUP BY 1, 2, 3, 4, 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(DISTINCT fk_show_id) FROM dev.mohit_gangwani.missing_shows_in_epg_show_condition_one;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(DISTINCT show_id) FROM dev.mohit_gangwani.historical_bad_show_id_condition_two;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(DISTINCT fk_show_id) FROM dev.mohit_gangwani.historical_bad_show_id_condition_three;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(DISTINCT show_id) FROM dev.mohit_gangwani.historical_bad_database_keys_condition_four;

# COMMAND ----------

# DBTITLE 1,Step 2: Delete C2 and C3 from C1
# MAGIC %sql
# MAGIC WITH show_id_to_delete AS (
# MAGIC     SELECT show_id FROM dev.mohit_gangwani.historical_bad_show_id_condition_two
# MAGIC     GROUP BY 1
# MAGIC     UNION
# MAGIC     SELECT fk_show_id FROM dev.mohit_gangwani.historical_bad_show_id_condition_three
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC DELETE FROM dev.mohit_gangwani.missing_shows_in_epg_show_condition_one AS c_one
# MAGIC WHERE EXISTS (
# MAGIC     SELECT show_id FROM show_id_to_delete sitd
# MAGIC     WHERE sitd.show_id = c_one.fk_show_id
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), COUNT(DISTINCT fk_show_id) FROM dev.mohit_gangwani.missing_shows_in_epg_show_condition_one;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_step_three AS
# MAGIC SELECT DISTINCT sh.*
# MAGIC FROM dev.mohit_gangwani.redshift_epg_show sh
# MAGIC JOIN dev.mohit_gangwani.missing_shows_in_epg_show_condition_one msei ON msei.fk_show_id = sh.show_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dev.mohit_gangwani.missing_shows_step_three

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_step_four AS
# MAGIC SELECT * FROM dev.mohit_gangwani.historical_bad_show_id_condition_two;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.mohit_gangwani.missing_shows_step_four LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_from_c3_and_c4 AS
# MAGIC SELECT fk_show_id FROM dev.mohit_gangwani.historical_bad_show_id_condition_three
# MAGIC GROUP BY 1
# MAGIC UNION 
# MAGIC SELECT show_id FROM dev.mohit_gangwani.historical_bad_database_keys_condition_four
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_step_five_final AS
# MAGIC SELECT msf.fk_show_id AS bad_show_id, sh.*
# MAGIC FROM dev.mohit_gangwani.redshift_epg_show sh
# MAGIC JOIN dev.mohit_gangwani.missing_shows_from_c3_and_c4 msf
# MAGIC   ON msf.fk_show_id = sh.show_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dev.mohit_gangwani.missing_shows_step_five_final

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT DATE_TRUNC('MONTH', vc.session_start) AS session_month, 
# MAGIC SELECT sh.show_id IS NULL AS missing_show, COUNT(*) AS session_count, COUNT(DISTINCT vc.fk_show_id) AS show_count
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC LEFT JOIN dev.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE vc.fk_show_id IS NOT NULL
# MAGIC   AND vc.session_start < '2024-03-26'
# MAGIC   AND vc.partition_key < '2024-03-26'
# MAGIC   AND vc.fk_zoo_id = 17
# MAGIC GROUP BY 1--, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT DATE_TRUNC('MONTH', vc.session_start) AS session_month, 
# MAGIC SELECT vc.fk_show_id, COUNT(*) AS session_count
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC LEFT JOIN dev.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE vc.fk_show_id IS NOT NULL
# MAGIC   AND sh.show_id IS NULL
# MAGIC   AND vc.session_start < '2024-03-26'
# MAGIC   AND vc.partition_key < '2024-03-26'
# MAGIC   AND vc.fk_zoo_id = 17
# MAGIC GROUP BY 1--, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS session_count, COUNT(DISTINCT vc.fk_show_id)
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC JOIN dev.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC  AND sh.vendor_name = 'TMS'
# MAGIC WHERE vc.fk_show_id IS NOT NULL
# MAGIC   AND vc.session_start < '2024-03-26'
# MAGIC   AND vc.partition_key < '2024-03-26'
# MAGIC   AND vc.fk_zoo_id = 17

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS session_count, COUNT(DISTINCT vc.fk_show_id)
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC JOIN dev.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC  AND sh.vendor_name = 'TMS'
# MAGIC JOIN dev.detection.epg_show tivo_sh
# MAGIC   ON tivo_sh.show_id = vc.fk_show_id
# MAGIC  AND tivo_sh.vendor_name = 'TIVO'
# MAGIC WHERE vc.fk_show_id IS NOT NULL
# MAGIC   AND vc.session_start < '2024-03-26'
# MAGIC   AND vc.partition_key < '2024-03-26'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.detection.epg_show
# MAGIC WHERE show_id IN (8217874,
# MAGIC 8383092,
# MAGIC 8878008,
# MAGIC 8382626,
# MAGIC 8721360,
# MAGIC 8685958,
# MAGIC 8716872,
# MAGIC 8190559,
# MAGIC 8155952,
# MAGIC 9051784)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.mohit_gangwani.missing_shows_in_epg_show_condition_one
# MAGIC WHERE fk_show_id IN (8217874,
# MAGIC 8383092,
# MAGIC 8878008,
# MAGIC 8382626,
# MAGIC 8721360,
# MAGIC 8685958,
# MAGIC 8716872,
# MAGIC 8190559,
# MAGIC 8155952,
# MAGIC 9051784)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT vc.fk_show_id AS tms_show_id, rds.database_key AS tivo_dbkey, dev_show.show_id AS tivo_show_id
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC   AND sh.vendor_name = 'TMS'
# MAGIC JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = vc.fk_show_id
# MAGIC LEFT JOIN dev.detection.epg_show dev_show
# MAGIC   ON dev_show.database_key = rds.database_key
# MAGIC  AND dev_show.vendor_name = 'TIVO'
# MAGIC WHERE vc.fk_zoo_id=17
# MAGIC   AND vc.session_duration > 0
# MAGIC   AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC   AND vc.partition_key < '2024-03-26'
# MAGIC   AND vc.fk_show_id IN (8217874,8383092,8878008,8382626,8721360,8685958,8716872,8190559,8155952,9051784)
# MAGIC GROUP BY 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.detection.epg_show
# MAGIC WHERE show_id IN ('13539916', '13539920', '13539924', '13539928', '13539917', '13539921', '13539925', '13539918', '13539922', '13539919')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.mohit_gangwani.final_replacement_for_tms_tivo AS
# MAGIC SELECT tms_sh.show_id AS tms_show_id, tivo_sh.show_id AS tivo_show_id
# MAGIC FROM prod.detection.epg_show tms_sh
# MAGIC JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = tms_sh.show_id
# MAGIC JOIN dev.detection.epg_show tivo_sh
# MAGIC   ON tivo_sh.database_key = rds.database_key
# MAGIC WHERE tms_sh.vendor_name = 'TMS'
# MAGIC   AND tivo_sh.vendor_name = 'TIVO'
# MAGIC   AND tivo_sh.show_id IN ('13539916', '13539920', '13539924', '13539928', '13539917', '13539921', '13539925', '13539918', '13539922', '13539919')
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.mohit_gangwani.final_replacement_for_tms_tivo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM staging.vizio_attrcomm_firehose
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT partition_key, COUNT(DISTINCT vcf.fk_tvid) as detected_tv_cnt
# MAGIC FROM prod.detection.viewing_commercials_firehose_dedup_cfe_merge vcf
# MAGIC JOIN prod.detection.experian_demography_historical ex
# MAGIC   ON vcf.fk_tvid = ex.tvid
# MAGIC   AND DATE_TRUNC('MONTH', session_start) - interval '1 month' = DATE_TRUNC('MONTH', match_date)
# MAGIC -- WHERE vcf.partition_key >= '2024-09-01'
# MAGIC   -- AND vcf.partition_key < '2024-09-27'
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT partition_key, COUNT(*)
# MAGIC FROM prod.detection.viewing_commercials_firehose_dedup_cfe_merge
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT partition_key, COUNT(DISTINCT vcf.fk_tvid) as detected_tv_cnt
# MAGIC FROM prod.historic.viewing_commercials_firehose_dedup_cfe_merge vcf
# MAGIC JOIN prod.detection.experian_demography_historical ex
# MAGIC   ON vcf.fk_tvid = ex.tvid
# MAGIC   AND DATE_TRUNC('MONTH', session_start) - interval '1 month' = DATE_TRUNC('MONTH', match_date)
# MAGIC WHERE vcf.partition_key >= '2024-01-01'
# MAGIC   AND vcf.partition_key < '2024-04-01'
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', match_date), COUNT(*) FROM detection.experian_demography_historical
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT partition_key, COUNT(*)
# MAGIC FROM dev.historic.viewing_commercials_firehose_updated
# MAGIC WHERE MOD(fk_tvid, 100) = 0
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT sh.show_id IS NULL, COUNT(*)
# MAGIC FROM dev.historic.viewing_commercials_firehose_updated vc
# MAGIC LEFT JOIN detection.epg_show sh
# MAGIC   ON sh.show_id = vc.prev_show_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE vc.prev_show_id IS NOT NULL
# MAGIC   AND vc.fk_zoo_id = 17
# MAGIC   AND vc.partition_key >= '2023-09-01'
# MAGIC   AND vc.partition_key < '2023-10-01'
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sh.show_id IS NULL, COUNT(*)
# MAGIC FROM dev.historic.viewing_commercials_firehose_updated vc
# MAGIC LEFT JOIN detection.epg_show sh
# MAGIC   ON sh.show_id = vc.next_show_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE vc.next_show_id IS NOT NULL
# MAGIC   AND vc.fk_zoo_id = 17
# MAGIC   AND vc.partition_key >= '2023-09-01'
# MAGIC   AND vc.partition_key < '2023-10-01'
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '|'||array_join(collect_set(LOWER(cl.client_name)), '|')||'|'  AS customer
# MAGIC FROM detection.clients cl

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT vc.prev_show_id), COUNT(*)
# MAGIC FROM prod.detection.viewing_commercials_firehose vc
# MAGIC JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = vc.prev_show_id
# MAGIC JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.prev_show_id
# MAGIC   AND sh.database_key != rds.database_key
# MAGIC WHERE vc.fk_zoo_id=17
# MAGIC   AND vc.session_duration > 0
# MAGIC   AND vc.session_start >= '2023-09-01 00:00:00'
# MAGIC   AND vc.session_start < '2023-10-01 00:00:00'
# MAGIC   AND vc.partition_key >= '2023-09-01'
# MAGIC   AND vc.partition_key < '2023-10-01'
# MAGIC   AND rds.vendor_name = 'TIVO'
# MAGIC   AND sh.vendor_name = 'TIVO'
# MAGIC   AND MOD(vc.fk_tvid, 10) = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT vc.next_show_id)
# MAGIC FROM prod.detection.viewing_commercials_firehose vc
# MAGIC JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = vc.next_show_id
# MAGIC JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.next_show_id
# MAGIC   AND sh.database_key != rds.database_key
# MAGIC WHERE vc.fk_zoo_id=17
# MAGIC   AND vc.session_duration > 0
# MAGIC   AND vc.session_start >= '2023-09-01 00:00:00'
# MAGIC   AND vc.session_start < '2023-10-01 00:00:00'
# MAGIC   AND vc.partition_key >= '2023-09-01'
# MAGIC   AND vc.partition_key < '2023-10-01'
# MAGIC   AND rds.vendor_name = 'TIVO'
# MAGIC   AND sh.vendor_name = 'TIVO'
# MAGIC   AND MOD(vc.fk_tvid, 10) = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(session_start), COUNT(*)
# MAGIC FROM dev.detection.viewing_commercials_firehose_dedup
# MAGIC WHERE fk_zoo_id = 17
# MAGIC AND session_start >= '2024-10-13'
# MAGIC -- AND partition_key >= '2024-10-13'
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.fixing_september_october_data;
# MAGIC CREATE TABLE dev.mohit_gangwani.fixing_september_october_data DISTKEY (fk_tvid) COMPOUND SORTKEY (fk_tvid, session_start, airdate) AS
# MAGIC SELECT DISTINCT vc.fk_tvid
# MAGIC , vc.session_start
# MAGIC , vc.session_end
# MAGIC , vc.airdate
# MAGIC , CASE WHEN tms_station.station_id IS NOT NULL THEN vc.tuner_channel_id END AS tms_tuner_channel_id
# MAGIC , CASE WHEN tms_show.show_id IS NOT NULL THEN vc.tuner_program_id END AS tms_tuner_program_id
# MAGIC , CASE WHEN tms_station.station_id IS NOT NULL AND tms_show.show_id IS NOT NULL THEN vc.tuner_schedule_id END AS tms_tuner_schedule_id
# MAGIC , CASE WHEN tivo_station.station_id IS NOT NULL THEN vc.tuner_channel_id END AS tuner_channel_id
# MAGIC , CASE WHEN tivo_show.show_id IS NOT NULL THEN vc.tuner_program_id END AS tuner_program_id
# MAGIC , CASE WHEN tivo_station.station_id IS NOT NULL AND tivo_show.show_id IS NOT NULL THEN vc.tuner_schedule_id END AS tuner_schedule_id
# MAGIC , TIMESTAMPADD(SECOND, vc.media_time_start, COALESCE(vc.tms_airdate, vc.airdate)) AS mts_calc
# MAGIC FROM prod.historic.viewing_content_firehose vc
# MAGIC LEFT JOIN detection.epg_station tms_station
# MAGIC   ON tms_station.station_id = vc.tuner_channel_id
# MAGIC  AND tms_station.vendor_name = 'TMS'
# MAGIC LEFT JOIN detection.epg_station tivo_station
# MAGIC   ON tivo_station.station_id = vc.tuner_channel_id
# MAGIC  AND tivo_station.vendor_name = 'TIVO'
# MAGIC LEFT JOIN detection.epg_show tms_show
# MAGIC   ON tms_show.show_id = vc.tuner_program_id
# MAGIC  AND tms_show.vendor_name = 'TMS'
# MAGIC LEFT JOIN detection.epg_show tivo_show
# MAGIC   ON tivo_show.show_id = vc.tuner_program_id
# MAGIC  AND tivo_show.vendor_name = 'TIVO'
# MAGIC WHERE vc.session_start >= '2023-09-01 00:00:00'
# MAGIC   AND vc.session_start < '2023-10-20 00:00:00'
# MAGIC   AND vc.day >= '2023-09-01'
# MAGIC   AND vc.day < '2023-10-20'
# MAGIC   AND vc.media_time_start IS NOT NULL
# MAGIC   AND vc.airdate IS NOT NULL
# MAGIC   AND COALESCE(vc.tuner_channel_id, vc.tuner_program_id, vc.tms_tuner_program_id, vc.tms_tuner_channel_id) IS NOT NULL;
