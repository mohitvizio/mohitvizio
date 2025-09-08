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

# %sql
# SELECT COUNT(rds.*)
# FROM dev.mohit_gangwani.redshift_epg_show rds
# LEFT JOIN prod.detection.epg_show sh
#   ON sh.show_id = rds.show_id
# --  AND sh.database_key != rds.database_key
#  AND sh.vendor_name != rds.vendor_name

# COMMAND ----------

# %sql
# SELECT rds.*, sh.show_id, sh.database_key, sh.title, sh.created_at
# FROM dev.mohit_gangwani.redshift_epg_show rds
# LEFT JOIN prod.detection.epg_show sh
#   ON sh.show_id = rds.show_id
#  AND sh.database_key != rds.database_key
#  AND sh.vendor_name = rds.vendor_name
# WHERE sh.show_id IS NOT NULL
#   AND rds.vendor_name = 'TIVO'
# LIMIT 1000

# COMMAND ----------

# %sql
# SELECT vendor_name, COUNT(*)
# FROM prod.detection.viewing_content_firehose
# JOIN prod.detection.epg_show
#   ON fk_show_id = epg_show.show_id
# WHERE session_start < ‘2024-04-01’
#   AND partition_key < ‘2024-04-01’
# GROUP BY 1

# COMMAND ----------

# %sql
# SELECT MIN(session_start), MAX(session_start)
# FROM prod.historic.viewing_content_firehose

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM unit_tests.final_tivo_tuner_table LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.historical_bad_database_keys_tuner;
# MAGIC CREATE TABLE dev.mohit_gangwani.historical_bad_database_keys_tuner AS
# MAGIC SELECT rds.show_id
# MAGIC , rds.database_key
# MAGIC , COALESCE(vc.airdate, vc.tms_airdate) AS airdate
# MAGIC , vc.tuner_channel_id
# MAGIC , vc.tuner_program_id
# MAGIC , COUNT(*) AS sessions_affected
# MAGIC FROM unit_tests.final_tivo_tuner_table vc
# MAGIC LEFT JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = vc.tuner_channel_id
# MAGIC   AND rds.vendor_name = 'TIVO'
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.tuner_channel_id
# MAGIC   AND sh.vendor_name = 'TIVO'
# MAGIC   AND sh.database_key != rds.database_key
# MAGIC WHERE vc.tuner_channel_id IS NOT NULL
# MAGIC   AND sh.show_id IS NOT NULL
# MAGIC   AND vc.session_start >= '2023-05-01'
# MAGIC   AND vc.session_start < '2023-09-01'
# MAGIC GROUP BY 1, 2, 3, 4, 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('MONTH', session_start) AS session_hour
# MAGIC , CASE WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_program_id IS NOT NULL AND tuner_program_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_program_id IS NULL AND tuner_program_id IS NULL THEN '0. Both Null' END AS show_id_check
# MAGIC , CASE WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NOT NULL THEN '0. In Both'
# MAGIC        WHEN tms_tuner_channel_id IS NOT NULL AND tuner_channel_id IS NULL THEN '9. TMS present, not TiVo'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NOT NULL THEN '3. TiVo present not TMS'
# MAGIC        WHEN tms_tuner_channel_id IS NULL AND tuner_channel_id IS NULL THEN '0. Both Null' END AS station_id_check
# MAGIC , COUNT(*)*1.0 AS sessions_count
# MAGIC   FROM unit_tests.final_tivo_tuner_table vc
# MAGIC GROUP BY 1, 2, 3;

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
# MAGIC 2. Delete Condition 2, 3, and 4 show IDs from the table from Condition 1
# MAGIC 3. Insert the Table from Condition 1 to EPG show
# MAGIC 4. Create mapping table to change the following in viewing tables:
# MAGIC     1. Show ID is not present, but DB key is
# MAGIC 5. Insert the following in EPG show with new show ID and create mapping table to change the old with the new
# MAGIC     1. Show ID maps to TMs
# MAGIC     2. Show ID is for TiVo but the DB key is not the same and the DB key doesn't exist in EPG Show

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.mohit_gangwani.prod_tvs_for_tuner AS
# MAGIC SELECT tvid
# MAGIC FROM detection.tv_zoo_latest_daily
# MAGIC WHERE zoo_id = 17
# MAGIC GROUP BY 1;

# COMMAND ----------

# DBTITLE 1,Step 1: Condition 1: Find all missing Show IDs
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.missing_shows_in_epg_show_condition_one_tuner;
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_in_epg_show_condition_one_tuner AS
# MAGIC SELECT vc.tuner_program_id, vc.airdate AS airdate, vc.tuner_channel_id
# MAGIC FROM unit_tests.final_tivo_tuner_table vc
# MAGIC JOIN dev.mohit_gangwani.prod_tvs_for_tuner prod
# MAGIC   ON prod.tvid = vc.fk_tvid
# MAGIC LEFT JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.tuner_program_id
# MAGIC  AND sh.vendor_name = 'TIVO'
# MAGIC WHERE sh.show_id IS NULL
# MAGIC   AND vc.tuner_program_id IS NOT NULL
# MAGIC   -- AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC   -- AND vc.partition_key < '2024-03-26'
# MAGIC GROUP BY 1, 2, 3;
# MAGIC
# MAGIC SELECT COUNT(*), COUNT(DISTINCT tuner_program_id) FROM dev.mohit_gangwani.missing_shows_in_epg_show_condition_one_tuner;

# COMMAND ----------

# DBTITLE 1,Step 1: Condition 2: Show ID doesn't exists, but the DB key is in EPG Show
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.historical_bad_show_id_condition_two_tuner;
# MAGIC CREATE TABLE dev.mohit_gangwani.historical_bad_show_id_condition_two_tuner AS
# MAGIC SELECT rds.show_id AS bad_show_id, sh.show_id AS existing_show_id, rds.database_key, vc.airdate, vc.tuner_channel_id
# MAGIC FROM unit_tests.final_tivo_tuner_table vc
# MAGIC JOIN dev.mohit_gangwani.prod_tvs_for_tuner prod
# MAGIC   ON prod.tvid = vc.fk_tvid
# MAGIC JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = vc.tuner_program_id
# MAGIC   AND rds.vendor_name = 'TIVO'
# MAGIC JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id != rds.show_id
# MAGIC   AND sh.vendor_name = 'TIVO'
# MAGIC   AND sh.database_key = rds.database_key
# MAGIC -- WHERE vc.fk_zoo_id=17
# MAGIC --   AND vc.session_duration > 0
# MAGIC --   AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC --   AND vc.partition_key < '2024-03-26'
# MAGIC GROUP BY 1, 2, 3, 4, 5;
# MAGIC
# MAGIC SELECT COUNT(*), COUNT(DISTINCT bad_show_id) FROM dev.mohit_gangwani.historical_bad_show_id_condition_two;

# COMMAND ----------

# DBTITLE 1,Step 1: Condition 3: Show ID maps to TMS
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.historical_bad_show_id_condition_three_tuner;
# MAGIC CREATE TABLE dev.mohit_gangwani.historical_bad_show_id_condition_three_tuner AS
# MAGIC SELECT vc.tuner_program_id, vc.airdate, vc.tuner_channel_id
# MAGIC FROM unit_tests.final_tivo_tuner_table vc
# MAGIC JOIN dev.mohit_gangwani.prod_tvs_for_tuner prod
# MAGIC   ON prod.tvid = vc.fk_tvid
# MAGIC JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.tuner_program_id
# MAGIC  AND sh.vendor_name = 'TMS'
# MAGIC -- WHERE vc.fk_zoo_id=17
# MAGIC --   AND vc.session_duration > 0
# MAGIC --   AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC --   AND vc.partition_key < '2024-03-26'
# MAGIC GROUP BY 1, 2, 3;
# MAGIC
# MAGIC SELECT COUNT(*), COUNT(DISTINCT tuner_program_id) FROM dev.mohit_gangwani.historical_bad_show_id_condition_three_tuner;

# COMMAND ----------

# DBTITLE 1,Step 1: Condition 4: Show ID exists, DB Key is different
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.historical_bad_database_keys_condition_four_tuner;
# MAGIC CREATE TABLE dev.mohit_gangwani.historical_bad_database_keys_condition_four_tuner AS
# MAGIC SELECT rds.show_id, rds.database_key, rds.title, vc.airdate, vc.tuner_channel_id
# MAGIC FROM unit_tests.final_tivo_tuner_table vc
# MAGIC JOIN dev.mohit_gangwani.prod_tvs_for_tuner prod
# MAGIC   ON prod.tvid = vc.fk_tvid
# MAGIC JOIN dev.mohit_gangwani.redshift_epg_show rds
# MAGIC   ON rds.show_id = vc.tuner_program_id
# MAGIC   AND rds.vendor_name = 'TIVO'
# MAGIC JOIN prod.detection.epg_show sh
# MAGIC   ON sh.show_id = vc.tuner_program_id
# MAGIC   AND sh.vendor_name = 'TIVO'
# MAGIC   AND sh.database_key != rds.database_key
# MAGIC -- WHERE vc.fk_zoo_id=17
# MAGIC --   AND vc.session_duration > 0
# MAGIC --   AND vc.session_start < '2024-03-26 00:00:00'
# MAGIC --   AND vc.partition_key < '2024-03-26'
# MAGIC GROUP BY 1, 2, 3, 4, 5;
# MAGIC
# MAGIC SELECT COUNT(*), COUNT(DISTINCT show_id) FROM dev.mohit_gangwani.historical_bad_database_keys_condition_four_tuner;

# COMMAND ----------

# DBTITLE 1,Step 2: Delete C2, C3, and C4 from C1
# MAGIC %sql
# MAGIC WITH show_id_to_delete AS (
# MAGIC     SELECT bad_show_id AS show_id FROM dev.mohit_gangwani.historical_bad_show_id_condition_two_tuner
# MAGIC     GROUP BY 1
# MAGIC     UNION
# MAGIC     SELECT tuner_program_id AS show_id FROM dev.mohit_gangwani.historical_bad_show_id_condition_three_tuner
# MAGIC     GROUP BY 1
# MAGIC     UNION
# MAGIC     SELECT show_id FROM dev.mohit_gangwani.historical_bad_database_keys_condition_four_tuner
# MAGIC     GROUP BY 1
# MAGIC )
# MAGIC DELETE FROM dev.mohit_gangwani.missing_shows_in_epg_show_condition_one_tuner AS c_one
# MAGIC WHERE EXISTS (
# MAGIC     SELECT show_id FROM show_id_to_delete sitd
# MAGIC     WHERE sitd.show_id = c_one.tuner_program_id
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Step 3: Creating Table to be inserted into EPG Show AS-IS
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.missing_shows_step_three_tuner;
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_step_three_tuner AS
# MAGIC SELECT DISTINCT sh.*
# MAGIC FROM dev.mohit_gangwani.redshift_epg_show sh
# MAGIC JOIN dev.mohit_gangwani.missing_shows_in_epg_show_condition_one_tuner msei
# MAGIC   ON msei.tuner_program_id = sh.show_id;
# MAGIC   
# MAGIC SELECT COUNT(*) FROM dev.mohit_gangwani.missing_shows_step_three_tuner;

# COMMAND ----------

# DBTITLE 1,Step 4: The mapping already exists within the step 1-4
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.missing_shows_step_four_tuner;
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_step_four_tuner AS
# MAGIC SELECT *
# MAGIC FROM dev.mohit_gangwani.historical_bad_show_id_condition_two_tuner;
# MAGIC
# MAGIC SELECT COUNT(*), COUNT(DISTINCT bad_show_id), COUNT(DISTINCT existing_show_id)
# MAGIC FROM dev.mohit_gangwani.missing_shows_step_four_tuner;

# COMMAND ----------

# DBTITLE 1,@Alina: Please run the command at the end to modify the unit test table
# MAGIC %sql
# MAGIC MERGE INTO unit_tests.final_tivo_tuner_table target
# MAGIC USING dev.mohit_gangwani.missing_shows_step_four_tuner fix
# MAGIC ON target.tuner_program_id = fix.bad_show_id
# MAGIC AND target.tuner_channel_id = fix.tuner_channel_id
# MAGIC AND target.airdate = fix.airdate
# MAGIC WHEN MATCHED THEN UPDATE SET target.tuner_program_id = fix.existing_show_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.missing_shows_from_c3_and_c4_tuner;
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_from_c3_and_c4_tuner AS
# MAGIC SELECT tuner_program_id FROM dev.mohit_gangwani.historical_bad_show_id_condition_three_tuner
# MAGIC GROUP BY 1
# MAGIC UNION
# MAGIC SELECT show_id FROM dev.mohit_gangwani.historical_bad_database_keys_condition_four_tuner
# MAGIC GROUP BY 1;
# MAGIC
# MAGIC SELECT COUNT(*), COUNT(DISTINCT tuner_program_id)
# MAGIC FROM dev.mohit_gangwani.missing_shows_from_c3_and_c4_tuner;

# COMMAND ----------

# DBTITLE 1,Step 5: Insert the following in EPG show with new show ID
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dev.mohit_gangwani.missing_shows_step_five_final_tuner;
# MAGIC CREATE TABLE dev.mohit_gangwani.missing_shows_step_five_final_tuner AS
# MAGIC SELECT sh.*
# MAGIC FROM dev.mohit_gangwani.redshift_epg_show sh
# MAGIC JOIN dev.mohit_gangwani.missing_shows_from_c3_and_c4_tuner msf
# MAGIC   ON msf.tuner_program_id = sh.show_id;
# MAGIC
# MAGIC SELECT COUNT(*), COUNT(DISTINCT show_id)
# MAGIC FROM dev.mohit_gangwani.missing_shows_step_five_final_tuner;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.mohit_gangwani.missing_shows_step_five_final_tuner
# MAGIC LIMIT 19

# COMMAND ----------

# DBTITLE 1,@Alina to run after all replacement in unit test is done
# MAGIC %sql
# MAGIC MERGE INTO prod.detection.viewing_content_firehose target
# MAGIC USING unit_test.final_tivo_tuner_table fix
# MAGIC ON target.fk_tvid = fix.fk_tvid
# MAGIC AND target.session_start = fix.session_start
# MAGIC AND target.session_end = fix.session_end
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC target.fk_show_id = fix.existing_show_id
# MAGIC , target.tuner_program_id = fix.tuner_program_id
# MAGIC , target.tuner_channel_id = fix.tuner_channel_id
# MAGIC , target.airdate = fix.airdate
# MAGIC , targe.tms_tuner_channel_id = fix.tms_tuner_channel_id

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM detection.tv_input_stats_firehose
# MAGIC WHERE fk_tvid = 30247591
# MAGIC -- AND next_create_timestamp >= '2024-10-14'
# MAGIC  AND create_timestamp >= '2024-10-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM prod.detection.tv_inputsource tvi
# MAGIC JOIN detection.input_source ins
# MAGIC   ON ins.input_source_id = tvi.fk_input_source_id
# MAGIC WHERE fk_tvid = 30247591
# MAGIC AND next_create_timestamp >= '2024-10-01'
# MAGIC ORDER BY create_timestamp DESC
# MAGIC
# MAGIC --  AND create_timestamp >= '2024-10-14'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_temp.viewing_content_firehose.staging_df
# MAGIC WHERE fk_tvid = 30247591
# MAGIC  AND session_start >= '2024-10-14'
# MAGIC  AND session_end <= '2024-10-15'

# COMMAND ----------


