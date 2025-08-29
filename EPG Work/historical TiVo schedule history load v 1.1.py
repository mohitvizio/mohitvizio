# Databricks notebook source
#For prod:
#   update redshift_read_env to hotc (current reference notebook won't support this)
#   update redshift_write_database to detection

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import date, datetime, timedelta

# COMMAND ----------

# MAGIC %run ./redshift_connect

# COMMAND ----------

redshift_read_env = set_redshift_params('warm') # dev/QA

redshift_write_env = set_redshift_params('warm') # dev
# redshift_write_env = set_redshift_params('warm') # QA

redshift_write_database = 'mgangwani' # dev
# redshift_write_database = 'dev_detection' # qa/prod
# redshift_write_table = f'{redshift_write_database}.epg_schedule_latest'
redshift_write_table = f'{redshift_write_database}.historical_tivo_schedule'

# COMMAND ----------

s3_directory = 'tms-daily'

vendor_name = 'TMS'

detection_database = 'detection'

EPG_STATION = f'{detection_database}.epg_station'
EPG_SHOW = f'{detection_database}.epg_show'

min_dt = date(2023,8,24)
max_dt = date(2023,8,29)
# max_dt = date.today()

dt_list = [min_dt + timedelta(days = n) for n in range((max_dt - min_dt).days)]

# COMMAND ----------

#redshift query to pull in epg_station data
query = f'''
SELECT
    distinct
        station_id
        ,station_num
FROM
    {EPG_STATION}
'''

epg_station_df = query_redshift(query, redshift_read_env)
epg_station_df.cache()



# COMMAND ----------

#redshift query to pull in epg_show data
query = f'''
SELECT
    show_id
    ,database_key
FROM
    {EPG_SHOW}
'''

epg_show_df = query_redshift(query, redshift_read_env)
epg_show_df.cache()

# COMMAND ----------

def create_sched_file_path(directory, dt):
    dt_str = dt.strftime('%Y%m%d')

    file_path = f's3://{directory}/skedrec.txt-{dt_str}.txt'

    return file_path

# COMMAND ----------

# pull s3 file as a dataframe

def create_raw_sched_df(sched_file_path):
    schema_tms_schedule = StructType([StructField('tf_station_num', StringType(), True), StructField('tf_database_key', StringType(), True), StructField('tf_air_date', StringType(), True), StructField('tf_air_time', StringType(), True), StructField('tf_duration', StringType(), True), StructField('tf_part_num', IntegerType(), True), StructField('tf_num_of_parts', IntegerType(), True), StructField('tf_cc', StringType(), True), StructField('tf_stereo', StringType(), True), StructField('tf_user_data_1', StringType(), True), StructField('tf_live_tape_delay', StringType(), True), StructField('tf_subtitled', StringType(), True), StructField('tf_premiere_finale', StringType(), True), StructField('tf_joined_in_progress', StringType(), True), StructField('tf_cable_in_the_classroom', StringType(), True), StructField('tf_tv_rating', StringType(), True), StructField('tf_sap', StringType(), True), StructField('tf_user_data', StringType(), True), StructField('tf_sex_rating', StringType(), True), StructField('tf_violence_rating', StringType(), True), StructField('tf_language_rating', StringType(), True), StructField('tf_dialog_rating', StringType(), True), StructField('tf_fv_rating', StringType(), True), StructField('tf_enhanced', StringType(), True), StructField('tf_three_d', StringType(), True), StructField('tf_letterbox', StringType(), True), StructField('tf_hdtv', StringType(), True), StructField('tf_dolby', StringType(), True), StructField('tf_dvs', StringType(), True), StructField('tf_user_data_2', StringType(), True), StructField('tf_new', StringType(), True), StructField('tf_net_syn_source', StringType(), True), StructField('tf_net_syn_type', StringType(), True), StructField('tf_subject_to_blackout', StringType(), True), StructField('tf_time_approximate', StringType(), True), StructField('tf_dubbed', StringType(), True), StructField('tf_dubbed_language', StringType(), True), StructField('tf_ei', StringType(), True), StructField('tf_sap_language', StringType(), True), StructField('tf_subtitled_language', StringType(), True), StructField('tf_left_in_progress', StringType(), True), StructField('vendor_name', StringType(), True), StructField('input_file_name', StringType(), True), StructField('partition_key', DateType(), True), StructField('created_at', TimestampType(), True)])

    raw_sched_df = (spark
                    .read
                    .format("csv")
                    .option("header", "false")
                    .option("delimiter", "|")
                    .option("nullValue", "")
                    .schema(schema_tms_schedule)
                    .load(sched_file_path)
                    )

    return raw_sched_df

# COMMAND ----------

#process dataframe to only records that are needed

def create_processed_sched_df(dt, raw_sched_df, epg_station_df, epg_show_df, vendor_name):
    dt_str = dt.strftime('%Y-%m-%d')
    
    processed_sched_df = (raw_sched_df
                          .select('tf_air_date','tf_air_time','tf_duration','tf_station_num','tf_database_key')
                        #   .withColumn('airdate_dt',F.to_date(F.col('tf_air_date'),'yyyyMMdd'))
                        #   .withColumn('duration',F.substring('tf_duration',0,2)*60*60 + F.substring('tf_duration',3,2)*60)
                        #   .withColumn('airdate',F.to_timestamp(F.concat(F.col('airdate_dt'),F.lit(' '),F.col('tf_air_time')),'yyyy-MM-dd HHmm'))
                        #   .withColumn('airdate_end',F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.col('airdate')) + F.col('duration'))))
                          .withColumn('vendor_name',F.lit(vendor_name))
                          .withColumn('created_at',F.to_timestamp(F.concat(F.lit(dt_str),F.lit(' 00:00')),'yyyy-MM-dd HH:mm'))
                          .withColumn('updated_at',F.to_timestamp(F.concat(F.lit(dt_str),F.lit(' 00:00')),'yyyy-MM-dd HH:mm'))
                        #   .filter(F.col('airdate_dt') == dt_str)
                        #   .join(epg_station_df
                        #         ,how = 'inner'
                        #         ,on = (F.col('tf_station_num') == F.col('station_num'))
                        #         )
                        #   .join(epg_show_df
                        #         ,how = 'inner'
                        #         ,on = (F.col('tf_database_key') == F.col('database_key'))
                        #         )
                        #   .selectExpr('station_id as fk_station_id'
                        #               ,'show_id as fk_show_id'
                        #               ,'duration'
                        #               ,'airdate'
                        #               ,'airdate_end'
                        #               ,'vendor_name'
                        #               ,'created_at'
                        #               ,'updated_at'
                        #               )
                          )
    
    return processed_sched_df

# COMMAND ----------

for dt in dt_list:
    #define s3 file path
    sched_file_path = create_sched_file_path(s3_directory, dt)

    #pull file as a dataframe
    raw_sched_df = create_raw_sched_df(sched_file_path)
    
    #process dataframe to only records that are needed
    processed_sched_df = create_processed_sched_df(dt, raw_sched_df, epg_station_df, epg_show_df, vendor_name)
    
    #load records into redshift table
    write_to_redshift(processed_sched_df, redshift_write_env, redshift_write_table)

    print(f'{dt} complete')

# COMMAND ----------

# dbutils.fs.rm("s3://inscape-databricks/redshifttemp/0e60e3ea-c382-479a-93d8-67a84c0a7ba3/",recurse=True)

# COMMAND ----------

# write_to_redshift(spark.table('unit_tests.epg_schedule_latest'), redshift_write_env, redshift_write_table)

# COMMAND ----------


