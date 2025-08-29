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

vendor_name = 'TMS'

EPG_SHOW = f'detection.epg_show'

min_dt = date(2023,8,24)
max_dt = date(2023,8,29)
# max_dt = date.today()

dt_list = [min_dt + timedelta(days = n) for n in range((max_dt - min_dt).days)]

# COMMAND ----------

#redshift query to pull in epg_show data
query = f'''
SELECT show_id, database_key, title
FROM {EPG_SHOW}
'''

epg_show_df = query_redshift(query, redshift_read_env)
epg_show_df.cache()

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


