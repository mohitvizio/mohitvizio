# Databricks notebook source
# MAGIC %run ./redshift_connect

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import desc

import pandas as pd
import datetime as dt
from dateutil.parser import parse
from pyspark.sql.types import *
import collections
import random

import boto3

# COMMAND ----------

# MAGIC %md
# MAGIC Explain below

# COMMAND ----------

# get inscape station info from warm
redshift = set_redshift_params('warm')
query = f'''
SELECT ism.*
FROM detection.inscape_station_map AS ism
JOIN detection.epg_station st
  ON st.station_id = ism.mapped_vendor_station_id
 AND st.vendor_name = ism.mapped_vendor
WHERE mapped_vendor = 'TIVO'
 AND st.created_at <= '2023-10-01'
'''

# COMMAND ----------

tivo_station = query_redshift(query, redshift)
tivo_station.createOrReplaceTempView("tivo_station")

# COMMAND ----------

# MAGIC %md
# MAGIC Explain below

# COMMAND ----------

columns = ['source_id','start_date','start_time','duration','program_id','partition_key','created_at']

date_start = dt.datetime(2023,11,1)
date_end = dt.datetime(2023,12,1)

# COMMAND ----------

tivo_station.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Explain below

# COMMAND ----------

tivo_schedule = (spark.table('stage_staging.tivo_schedule_full')
                 .selectExpr(*columns)
                 )

tivo_schedule = (tivo_schedule
                 .withColumn('start_date',to_date(col('start_date'),'yyyyMMdd'))
                 .withColumn('partition_key',to_date(col('partition_key'),'yyyyMMdd'))
                 .withColumn('created_at',to_timestamp(col('created_at')))
                 .withColumn('start_datetime',to_timestamp(concat(col("start_date"), lit(" "), col("start_time")) ))
                 )

tivo_station = (spark.table('tivo_station')
                  .where("mapped_vendor = 'TIVO'")
                  .selectExpr(['mapped_vendor_station_num']))

filtered_schedule = (tivo_schedule.join(tivo_station, tivo_schedule.source_id == tivo_station.mapped_vendor_station_num).select(tivo_schedule["*"]))

filtered_schedule = filtered_schedule.filter((filtered_schedule.start_datetime>=date_start) & (filtered_schedule.start_datetime<date_end))

filtered_schedule = filtered_schedule.where('start_datetime >= created_at')

filtered_schedule = (filtered_schedule
                     .withColumn('end_datetime', expr("from_unixtime(unix_timestamp(start_datetime) + duration)")))

filtered_schedule = filtered_schedule.withColumn(
    'row_num', row_number().over(Window.partitionBy(['source_id','start_date','start_time']).orderBy(
        [desc("created_at"), desc("partition_key")])))

filtered_schedule = filtered_schedule.where('row_num = 1')

# filtered_schedule = (filtered_schedule
#                      .withColumn('next_start', lead('start_datetime').over(Window.partitionBy(['source_id']).orderBy(['start_datetime', desc("created_at")])))
#                      .withColumn('prev_end', lag('start_datetime').over(Window.partitionBy(['source_id']).orderBy(['start_datetime', desc("created_at")])))
#                      .withColumn('next_create_ts', lead('create_timestamp').over(Window.partitionBy(['source_id']).orderBy(['start_datetime', desc("created_at")])))
#                      .withColumn('prev_create_ts', lag('create_timestamp').over(Window.partitionBy(['source_id']).orderBy(['start_datetime', desc("created_at")]))))

# filtered_schedule = (filtered_schedule
#                      .withColumn('bad_airdate', when(
#                          (col('prev_end') > col('start_datetime')) & (col('prev_create_ts') > col('create_timestamp')), 1)
#                                  .otherwise((col('next_start') < col('end_datetime')) & (col('next_create_ts') > col('start_datetime')), 1))
#                      )

# COMMAND ----------

# filtered_schedule = (filtered_schedule
#                      .withColumn('next_start', lead(filtered_schedule['start_datetime']).over(Window.partitionBy(['source_id']).orderBy("start_datetime")))
#                      .withColumn('next_create', lead(filtered_schedule['create_timestamp']).over(Window.partitionBy(['source_id']).orderBy("start_datetime")))
#                      .withColumn('prev_end', lag(filtered_schedule['end_datetime']).over(Window.partitionBy(['source_id']).orderBy("start_datetime")))
#                      .withColumn('prev_create', lag(filtered_schedule['create_timestamp']).over(Window.partitionBy(['source_id']).orderBy("start_datetime"))))

# filtered_schedule = (filtered_schedule
#                      .withColumn('check1', filtered_schedule['next_start'] < filtered_schedule['end_datetime'])
#                      .withColumn('check2', filtered_schedule['next_create'] < filtered_schedule['create_timestamp'])
#                      .withColumn('check3', filtered_schedule['prev_end'] > filtered_schedule['start_datetime'])
#                      .withColumn('check4', filtered_schedule['prev_create'] > filtered_schedule['create_timestamp']))

# COMMAND ----------

columns = ['source_id','start_date','start_time','duration','program_id','partition_key','created_at']
intraday_schedule = (spark.table('stage_staging.tivo_schedule_intraday').selectExpr(*columns))

intraday_schedule = (intraday_schedule
                     .withColumn('start_date',to_date(col('start_date'),'yyyyMMdd'))
                     .withColumn('partition_key',to_date(col('partition_key'),'yyyyMMdd'))
                     .withColumn('created_at',to_timestamp(col('created_at')))
                     .withColumn('start_datetime',to_timestamp(concat(col("start_date"), lit(" "), col("start_time")))))

tivo_station = (spark.table('tivo_station')
                .where("mapped_vendor = 'TIVO'")
                .selectExpr(['mapped_vendor_station_num']))

intraday_filtered = (intraday_schedule.join(tivo_station, intraday_schedule.source_id == tivo_station.mapped_vendor_station_num).select(intraday_schedule["*"]))

intraday_filtered = intraday_filtered.filter((intraday_filtered.start_datetime>=date_start) & (intraday_filtered.start_datetime<date_end))

intraday_filtered = intraday_filtered.filter(
    (intraday_filtered.start_datetime >= intraday_filtered.created_at) &
    (intraday_filtered.created_at >= (intraday_filtered.start_datetime - expr('INTERVAL 26 HOURS'))))

# intraday_filtered = intraday_filtered.filter(intraday_filtered.created_at >= (intraday_filtered.start_datetime - expr('INTERVAL 26 HOURS')))

intraday_filtered = (intraday_filtered
                     .withColumn('end_datetime', expr("from_unixtime(unix_timestamp(start_datetime) + duration)")))

intraday_filtered = intraday_filtered.withColumn(
    'row_num', row_number().over(Window.partitionBy(['source_id','start_date','start_time']).orderBy(
        [desc("created_at"), desc("partition_key")])))

intraday_filtered = intraday_filtered.where('row_num = 1')

# COMMAND ----------

# MAGIC %md
# MAGIC Explain below

# COMMAND ----------

print(filtered_schedule.count())
# print(filtered_schedule.show(500))

# COMMAND ----------

print(intraday_filtered.count())
# print(intraday_filtered.show(500))

# COMMAND ----------

filtered_schedule.groupBy('start_date', 'source_id').count().groupBy('start_date').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Explain below

# COMMAND ----------

intraday_filtered.groupBy('start_date', 'source_id').count().groupBy('start_date').count().display()

# COMMAND ----------

print(intraday_filtered.agg({"duration": "sum"}).show()/3600)

# COMMAND ----------

intraday_stations = intraday_filtered.select('source_id', 'start_date').distinct()

filter_conditions = [
    filtered_schedule.source_id == intraday_stations.source_id,
    filtered_schedule.start_date == intraday_stations.start_date
]

filtered_schedule = (filtered_schedule.join(intraday_stations, filter_conditions).select(filtered_schedule["*"]))

# COMMAND ----------

filtered_schedule.groupBy('start_date', 'source_id').count().groupBy('start_date').count().display()

# COMMAND ----------

# filtered_schedule = filtered_schedule.withColumn('table_name', lit('Existing'))
# intraday_filtered = intraday_filtered.withColumn('table_name', lit('Intraday'))

# filtered_schedule.union(intraday_filtered).display()

# COMMAND ----------

print(filtered_schedule.count())

# COMMAND ----------

conds = [
    filtered_schedule.source_id == intraday_filtered.source_id,
    # filtered_schedule.start_datetime == intraday_filtered.start_datetime,
    intraday_filtered.start_datetime >= filtered_schedule.start_datetime,
    intraday_filtered.start_datetime < filtered_schedule.end_datetime,
    filtered_schedule.program_id == intraday_filtered.program_id
]

fix_schedule = intraday_filtered.join(filtered_schedule, conds).select(intraday_filtered["*"])

print(fix_schedule.distinct().count())
print(fix_schedule.agg({"duration": "sum"}).show())

# COMMAND ----------

additional_conds = [
    filtered_schedule.source_id == intraday_filtered.source_id,
    filtered_schedule.start_datetime == intraday_filtered.start_datetime,
    # intraday_filtered.start_datetime >= filtered_schedule.start_datetime,
    # intraday_filtered.start_datetime < filtered_schedule.end_datetime,
    filtered_schedule.program_id == intraday_filtered.program_id
]

fix_schedule = intraday_filtered.join(filtered_schedule, additional_conds).select(intraday_filtered["*"])

print(fix_schedule.distinct().count())
print(fix_schedule.agg({"duration": "sum"}).show())

# COMMAND ----------

additional_conds = [
    filtered_schedule.source_id == intraday_filtered.source_id,
    filtered_schedule.start_datetime == intraday_filtered.start_datetime,
    # intraday_filtered.start_datetime >= filtered_schedule.start_datetime,
    # intraday_filtered.start_datetime < filtered_schedule.end_datetime,
    filtered_schedule.program_id != intraday_filtered.program_id
]

fix_schedule = intraday_filtered.join(filtered_schedule, additional_conds).select(intraday_filtered["*"]).distinct()

print(fix_schedule.agg({"duration": "sum"}).show())
print(fix_schedule.count())

# COMMAND ----------

additional_conds = [
    filtered_schedule.source_id == intraday_filtered.source_id,
    # filtered_schedule.start_datetime == intraday_filtered.start_datetime,
    intraday_filtered.start_datetime >= filtered_schedule.start_datetime,
    intraday_filtered.start_datetime < filtered_schedule.end_datetime,
    filtered_schedule.program_id != intraday_filtered.program_id
]

fix_schedule = intraday_filtered.join(filtered_schedule, additional_conds).select(intraday_filtered["*"]).distinct()

print(fix_schedule.agg({"duration": "sum"}).show())
print(fix_schedule.count())

# COMMAND ----------

intraday_filtered.join(filtered_schedule, additional_conds).select(intraday_filtered["*"]).distinct().display()

# COMMAND ----------

additional_conds = [
    filtered_schedule.source_id == intraday_filtered.source_id,
    # filtered_schedule.start_datetime == intraday_filtered.start_datetime,
    intraday_filtered.start_datetime >= filtered_schedule.start_datetime,
    intraday_filtered.start_datetime < filtered_schedule.end_datetime,
    filtered_schedule.program_id != intraday_filtered.program_id,
    filtered_schedule.created_at < intraday_filtered.created_at
]

filtered_schedule.join(intraday_filtered, additional_conds).select(filtered_schedule["*"]).distinct().count()

# COMMAND ----------

filtered_schedule.join(intraday_filtered, additional_conds).select(filtered_schedule["*"]).distinct().display()

# COMMAND ----------

mismatch = filtered_schedule.join(intraday_filtered, additional_conds).select(filtered_schedule["*"]).distinct()
mismatch = (mismatch
            .withColumn('start_minus_one_hour', mismatch.start_datetime - expr('INTERVAL 1 HOURS'))
            .withColumn('end_plus_one_hour', mismatch.end_datetime + expr('INTERVAL 1 HOURS')))

mismatch = mismatch.select('source_id', 'start_minus_one_hour', 'end_plus_one_hour', 'created_at').distinct()

mm_conds = [
    filtered_schedule.source_id == mismatch.source_id,
    filtered_schedule.start_datetime >= mismatch.start_minus_one_hour,
    filtered_schedule.start_datetime < mismatch.end_plus_one_hour
]

xyz = filtered_schedule.join(mismatch, mm_conds).select(filtered_schedule["*"]).distinct()
print(xyz.count())

# COMMAND ----------

xyz.display()

# COMMAND ----------

intraday_filtered.join(filtered_schedule, additional_conds).select(intraday_filtered["*"]).distinct().count()

# COMMAND ----------

intraday_filtered.join(filtered_schedule, additional_conds).select(intraday_filtered["*"]).distinct().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT vc.fk_tvid AS tvid
# MAGIC , CASE WHEN vc.fk_tvid = 159151117 THEN 'YTTV' ELSE 'DirecTV' END AS service_name
# MAGIC , vc.fk_zoo_id
# MAGIC , LEFT(sh.title, 40) AS show_name
# MAGIC , st.station_call_sign AS stt_cs
# MAGIC , vc.airdate
# MAGIC , vc.runtime AS show_dur
# MAGIC , vc.session_start AS sess_start
# MAGIC , vc.session_end AS sess_end
# MAGIC , vc.session_duration AS sess_dur
# MAGIC , CASE WHEN vc.is_live = True THEN 'Live'
# MAGIC        WHEN vc.is_live = False THEN 'TS' END AS live
# MAGIC , vc.audio_contri AS aud_ctrb
# MAGIC , DATE_TRUNC('SECOND', vc.created_at) AS created_at
# MAGIC FROM detection.viewing_content_firehose vc
# MAGIC
# MAGIC LEFT JOIN detection.epg_show sh
# MAGIC   ON sh.show_id = vc.fk_show_id
# MAGIC
# MAGIC LEFT JOIN detection.inscape_station_map ism
# MAGIC   ON ism.inscape_station_id = vc.fk_station_id
# MAGIC  AND sh.vendor_name = ism.mapped_vendor
# MAGIC
# MAGIC LEFT JOIN detection.epg_station st
# MAGIC   ON st.station_id = ism.mapped_vendor_station_id
# MAGIC  AND st.vendor_name = ism.mapped_vendor
# MAGIC
# MAGIC WHERE vc.fk_tvid IN (159151117, 165869790)
# MAGIC   AND vc.session_start >= '2024-03-06 17:45'
# MAGIC   AND vc.session_start < '2024-03-06 22:00' 
# MAGIC ORDER BY vc.fk_tvid, vc.session_start, vc.session_end

# COMMAND ----------


