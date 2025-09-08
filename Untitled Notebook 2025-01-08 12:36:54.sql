-- Databricks notebook source
SELECT * FROM stage.staging.vizio_content_firehose
WHERE tvid = 3267355
AND DATE(ts_start) = '2025-01-07'

-- COMMAND ----------

SELECT * FROM stage.cooker.final_result_content_split_multinode
WHERE tvid = 3267355
AND DATE(ts_start) = '2025-01-07'

-- COMMAND ----------

SELECT * FROM stage.detection.nodma

-- COMMAND ----------

SELECT * FROM prod.detection.nodma

-- COMMAND ----------

SELECT * FROM qa.public.nodma_df_stage

-- COMMAND ----------


