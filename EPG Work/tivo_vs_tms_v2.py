# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Alternative EPG -- Comparison of TiVo vs TMS Analysis
# MAGIC
# MAGIC **Context**
# MAGIC
# MAGIC Inscape currently relies on TMS as the sole provider of EPG metadata in the core pipeline. This data is appended to viewing activity & *<need to get more details on exactly how the metadata is utilized in the pipeline>*. Alternative data providers such as TiVo and Redbee could be replacements to reduce the risk to the business.
# MAGIC
# MAGIC **Goal**
# MAGIC - Compare the EPG data from TMS vs TiVo to determine, by call sign, the schedule similarity.
# MAGIC
# MAGIC **Analysis Questions**
# MAGIC
# MAGIC Call Signs:
# MAGIC - What percentage of call_signs do we have an exact match for? What percentage can we match w/ transformations?
# MAGIC - What percentage of non-null sessions/duration do we have a call_sign for? 
# MAGIC
# MAGIC Schedules:
# MAGIC - What percentage of timeslots (call_sign|airdate|duration combination) are perfectly matched across providers?
# MAGIC - What percentage of sessions/duration will be impacted by misaligned schedules?
# MAGIC - When a timeslot is misaligned, what is the distribution of minutes between the timeslots?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup & Global Params

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ./redshift_connect

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# connection to redshift
#----------------------------------------------------------------------------------------------------

redshift = set_redshift_params('warm')

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Tables referenced
#----------------------------------------------------------------------------------------------------

# TMS v1
EPG_SCHEDULE = 'detection.epg_schedule'
EPG_STATION = 'detection.epg_station'
EPG_SHOW = 'detection.epg_show'

# TMS v2
TV_SOURCES_TV2 = 'detection.tv_sources_tv2'
TV_SCHEDULES_TV2_LATEST = 'detection.tv_schedules_tv2_latest'
TV_PROGRAM_TV2_LATEST = 'detection.tv_programs_tv2_latest'

# TMS v2 Experimental
TV_SOURCES_TV2_EXP = 'detection.tv_sources_tv2_experimental_long'
TV_SCHEDULES_TV2_LATEST_EXP = 'detection.tv_schedules_tv2_experimental_long'
TV_PROGRAM_TV2_LATEST_EXP = 'detection.tv_programs_tv2_experimental_long'

# TMS v1 <-> TMS v2 mapping
TMSV1_TMSV2_MAPPING = 'public.tmsv1_tmsv2_callsign_mapping'

# TiVo
SOURCE = 'dev_staging.source'
SCHEDULE = 'dev_staging.schedule'
PROGRAM = 'dev_staging.program'

PUBLIC_TIVO_SOURCE = 'public.tivo_source'
PUBLIC_TIVO_SCHEDULE = 'public.tivo_schedule'


BROADCAST_HISTORY = 'dev_staging.broadcast_history'
CHANNEL_GROUPING = 'dev_staging.channel_grouping'
COMBO_EPISODE_SEQUENCE = 'dev_staging.combo_episode_sequence'
COUNTRY_AVAILABILITY = 'dev_staging.country_availability'
DST = 'dev_staging.dst'
DEVICE_TYPE = 'dev_staging.device_type'
DEVICE_TYPE_TRANSLATION = 'dev_staging.device_type_translation'
EPISODE_SEQUENCE = 'dev_staging.episode_sequence'
EXPIREDIMAGES = 'dev_staging.expiredimages'
FILMING_LOCATION = 'dev_staging.filming_location'
FULL_SEQUENCING = 'dev_staging.full_sequencing'
GROUPID_MASTER = 'dev_staging.groupid_master'
IMAGEFILES = 'dev_staging.imagefiles'
IMAGEFORMATS = 'dev_staging.imageformats'
IMAGETYPES = 'dev_staging.imagetypes'
IMAGEV2 = 'dev_staging.imagev2'
LANGUAGE_COUNTRY_XREF = 'dev_staging.language_country_xref'
MSOLOGOS = 'dev_staging.msologos'
MOVIE_RATING_REASON_TRANSLATION = 'dev_staging.movie_rating_reason_translation'
MSO = 'dev_staging.mso'
NATIONAL_LINEUP = 'dev_staging.national_lineup'
PROGRAM_CREDITS = 'dev_staging.program_credits'
PROGRAM_DESCRIPTION = 'dev_staging.program_description'
PROGRAM_GENRE_ALT = 'dev_staging.program_genre_alt'
PROGRAM_GENRES = 'dev_staging.program_genres'
PROGRAM_MOVIE_RATINGS = 'dev_staging.program_movie_ratings'
PROGRAM_ORIGINAL_AIRDATE_ALLCOUNTRIES = 'dev_staging.program_original_airdate_allcountries'
PROGRAM_ORIGINAL_COUNTRY = 'dev_staging.program_original_country'
PROGRAM_ORIGINAL_LANGUAGE = 'dev_staging.program_original_language'
PROGRAM_QUALITY_RATING = 'dev_staging.program_quality_rating'
PROGRAM_RELEASE_DATE = 'dev_staging.program_release_date'
PROGRAM_TV_RATINGS = 'dev_staging.program_tv_ratings'
PROGRAM_VARIANT = 'dev_staging.program_variant'
PROGRAM_VARIANT_TYPE = 'dev_staging.program_variant_type'
SEASON_HISTORY = 'dev_staging.season_history'
SERIES = 'dev_staging.series'
SERIES_CAST = 'dev_staging.series_cast'
SERIES_MASTER = 'dev_staging.series_master'
SERVICE_TYPE = 'dev_staging.service_type'
SOURCELOGOS = 'dev_staging.sourcelogos'
SOURCE_GENRE = 'dev_staging.source_genre'
SUPER_SERIES = 'dev_staging.super_series'
TIME_ZONE = 'dev_staging.time_zone'
TIME_ZONE_XREF = 'dev_staging.time_zone_xref'
TRANSLATION_REF = 'dev_staging.translation_ref'
HEADEND = 'dev_staging.headend'
LINEUP = 'dev_staging.lineup'
ZIPCODE = 'dev_staging.zipcode'

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# global params
#----------------------------------------------------------------------------------------------------

start_ts = '2023-06-01 00:00:00' 
end_ts = '2023-06-07 23:59:59'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Table Loads

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TiVo call signs
#----------------------------------------------------------------------------------------------------

query = f'''
SELECT
    *
FROM
    {PUBLIC_TIVO_SOURCE}
'''

tivo_source_df = query_redshift(query, redshift)
tivo_source_df.display()

# COMMAND ----------

tivo_source_df.cache()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TiVo schedule
#----------------------------------------------------------------------------------------------------

query = f'''
SELECT
    *
FROM
    {PUBLIC_TIVO_SCHEDULE}
'''

tivo_schedule_df = query_redshift(query, redshift)
tivo_schedule_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v1 call signs
#----------------------------------------------------------------------------------------------------

tms_v1_source_df = spark.table(EPG_STATION)

tms_v1_source_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v1 schedule
#----------------------------------------------------------------------------------------------------

tms_v1_schedule_df = spark.table(EPG_SCHEDULE)

tms_v1_schedule_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v1 show
#----------------------------------------------------------------------------------------------------

tms_v1_show_df = spark.table(EPG_SHOW)

tms_v1_show_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v2 call signs
#----------------------------------------------------------------------------------------------------

tms_v2_source_df = spark.table(TV_SOURCES_TV2)

tms_v2_source_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v2 schedule
#----------------------------------------------------------------------------------------------------

tms_v2_schedule_df = spark.table(TV_SCHEDULES_TV2_LATEST)

tms_v2_schedule_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v2 show
#----------------------------------------------------------------------------------------------------

tms_v2_show_df = spark.table(TV_PROGRAM_TV2_LATEST)

tms_v2_show_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v1 <-> TMS v2 call sign mapping
#----------------------------------------------------------------------------------------------------

query = f'''
SELECT
    *
FROM
    {TMSV1_TMSV2_MAPPING}
'''

tms_v1_tms_v2_mapping_df = query_redshift(query, redshift)
tms_v1_tms_v2_mapping_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v2 Experimental call signs
#----------------------------------------------------------------------------------------------------

query = f'''
SELECT
    *
FROM
    {TV_SOURCES_TV2_EXP}
'''

tms_v2_exp_source_df = query_redshift(query, redshift)
tms_v2_exp_source_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v2 Experimental schedule
#----------------------------------------------------------------------------------------------------

query = f'''
SELECT
    *
FROM
    {TV_SCHEDULES_TV2_LATEST_EXP}
'''

tms_v2_exp_schedule_df = query_redshift(query, redshift)
tms_v2_exp_schedule_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v2 Experimental show
#----------------------------------------------------------------------------------------------------

query = f'''
SELECT
    *
FROM
    {TV_PROGRAM_TV2_LATEST_EXP}
'''

tms_v2_exp_show_df = query_redshift(query, redshift)
tms_v2_exp_show_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### v1 Call Signs to Map

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Call sign active, current in schedule, non-current in schedule
#----------------------------------------------------------------------------------------------------

tms_schedule_agg = (tms_v1_schedule_df
                    .groupBy('fk_station_id')
                    .agg(F.max('airdate').alias('max_airdate'))
                    .withColumn('in_epg_schedule_flag',F.lit(1))
                    .withColumn('in_epg_schedule_L30D_flag',F.when(F.col('max_airdate') >= F.date_sub(F.current_timestamp(),30),1).otherwise(0))
                    )

tms_v1 = (tms_v1_source_df
          .withColumn('lmdb_active_flag',F.when(F.col('lmdb') >= F.current_timestamp(),1).otherwise(0))
          .join(tms_schedule_agg
                ,how = 'left'
                ,on = (tms_v1_source_df.station_id == tms_schedule_agg.fk_station_id)
                )
          )

tms_v1.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Nubmer of active call signs -- lmdb >= current timestamp
#----------------------------------------------------------------------------------------------------

n_tms_v1 = tms_v1.select('station_call_sign').distinct().count()

n_tms_v1_active_lmdb = tms_v1.agg(F.sum('lmdb_active_flag')).first()[0]

n_tms_v1_in_epg_schedule = tms_v1.agg(F.sum('in_epg_schedule_flag')).first()[0]

n_tms_v1_in_epg_schedule_L30D = tms_v1.agg(F.sum('in_epg_schedule_L30D_flag')).first()[0]

# COMMAND ----------

print(f'# of Distinct Call Signs in epg_station; n_tms_v1: {n_tms_v1}')
print(f'# of Distinct Call Signs in epg_station where lmdb >= current timestamp; n_tms_v1_active_lmdb: {n_tms_v1_active_lmdb}')
print(f'# of Distinct Call Signs in epg_station where call_sign in epg_schedule; n_tms_v1_in_epg_schedule: {n_tms_v1_in_epg_schedule}')
print(f'# of Distinct Call Signs in epg_station where call_sign in epg_schedule w/ airdate in L30D; n_tms_v1_in_epg_schedule_L30D: {n_tms_v1_in_epg_schedule_L30D}')

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Status of call signs in epg_station and epg_schedule
#----------------------------------------------------------------------------------------------------

(tms_v1
 .groupBy('lmdb_active_flag'
          ,'in_epg_schedule_flag'
          ,'in_epg_schedule_L30D_flag'
          )
 .agg(F.count('*').alias('n_tms_v1'))
 .orderBy(F.lit(1),F.lit(2),F.lit(3))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TMS v1 <-> TMS v2 Mapping

# COMMAND ----------

# MAGIC %md
# MAGIC #### TMS v2 Exploration

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v2 exploration
#----------------------------------------------------------------------------------------------------

tms_v2_source_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v2 columns, distinct records, etc.
#----------------------------------------------------------------------------------------------------

(tms_v2_source_df
 .agg(F.count('source_id').alias('n_source_id')
      ,F.countDistinct('source_id').alias('nD_source_id')
      ,F.count('prgSvcId').alias('n_prgSvcId')
      ,F.countDistinct('prgSvcId').alias('nD_prgSvcId')
      ,F.count('callSign').alias('n_call_sign')
      ,F.countDistinct('callSign').alias('nD_call_sign')
      )
 ).display()

# COMMAND ----------

# MAGIC  %md
# MAGIC #### Raw TMS v1 vs TMS v2

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v1 <-> TMS v2 call sign mapping (TMS v1 as the base set)
#----------------------------------------------------------------------------------------------------

tms_v1_v2_df = (tms_v1.selectExpr('station_id','station_num','station_name as station_name_v1','station_call_sign as call_sign_v1','lmdb_active_flag','in_epg_schedule_flag','in_epg_schedule_L30D_flag')
                .join(tms_v2_source_df.selectExpr(f'callSign as call_sign_v2','name as station_name_v2','source_id','prgSvcId')
                      ,on = (F.col('call_sign_v1') == F.col('call_sign_v2'))
                      ,how = 'left'
                      )
                .withColumn('tms_v2_match_flag',F.when(F.col('call_sign_v2').isNotNull(),1).otherwise(0))
                .join(tms_v1_tms_v2_mapping_df.selectExpr('tmsv1_station_call_sign as call_sign_v1','tmsv2_callsign as call_sign_v2_PM_table')
                      ,on = ('call_sign_v1')
                      ,how = 'left'
                      )
                .withColumn('tms_v2_match_PM_table_flag',F.when(F.col('call_sign_v2_PM_table').isNotNull(),1).otherwise(0))
                )

tms_v1_v2_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Results

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Status of call signs in epg_station and epg_schedule
#----------------------------------------------------------------------------------------------------

(tms_v1_v2_df
 .groupBy('lmdb_active_flag'
          ,'in_epg_schedule_flag'
          ,'in_epg_schedule_L30D_flag'
          )
 .agg(F.count('*').alias('n_tms_v1')
      ,F.sum('tms_v2_match_flag').alias('n_tms_v2_exact_match')
      ,F.sum('tms_v2_match_PM_table_flag').alias('n_tms_v2_PM_table_match')
      )
 .orderBy(F.lit(1),F.lit(2),F.lit(3))
).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# lmdb active with no tms v2 exact match
#----------------------------------------------------------------------------------------------------

(tms_v1_v2_df
 .filter(F.col('lmdb_active_flag') == 1)
 .filter(F.col('call_sign_v2').isNull())
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TMS v1 <-> TiVo Mapping

# COMMAND ----------

# MAGIC %md
# MAGIC #### TiVo Exploration

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TiVo exploration
#----------------------------------------------------------------------------------------------------

tivo_source_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TiVo columns, distinct records, etc.
#----------------------------------------------------------------------------------------------------

(tivo_source_df
 .agg(F.count('call_letters').alias('n_call_letters')
      ,F.countDistinct('call_letters').alias('nD_call_letters')
      ,F.count('offical_call_sign').alias('n_official_call_sign')
      ,F.countDistinct('offical_call_sign').alias('nD_official_call_sign')
      ,F.count('short_name').alias('n_short_name')
      ,F.countDistinct('short_name').alias('nD_short_name')
      ,F.count('full_name').alias('n_full_name')
      ,F.countDistinct('full_name').alias('nD_full_name')
      ,F.count(F.concat('full_name','short_name','call_letters','short_name')).alias('n_all')
      ,F.countDistinct(F.concat('full_name','short_name','call_letters','short_name')).alias('nD_all')
      )
 ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### TMS v1 <-> TiVo short name

# COMMAND ----------

tms_v1_v2_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TiVo distinct call signs -- based on above exploration, short_name appears to be the most unique, distinct key from the TiVo table
#----------------------------------------------------------------------------------------------------

tivo_distinct_df = tivo_source_df.selectExpr('short_name as call_sign_tivo').distinct()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v1 <-> TMS v2 call sign mapping (TMS v1 as the base set)
#----------------------------------------------------------------------------------------------------

tms_v1_v2_tivo_df = (tms_v1_v2_df
                     .join(tivo_distinct_df
                           ,on = (tms_v1_v2_df.call_sign_v1 == tivo_distinct_df.call_sign_tivo)
                           ,how = 'left'
                           )
                     .withColumn('tivo_exact_match_flag', F.when(F.col('call_sign_tivo').isNotNull(),1).otherwise(0))
                     .join(tivo_distinct_df.selectExpr('call_sign_tivo as call_sign_tivo_clean')
                           ,on = (F.regexp_replace(F.lower(tms_v1_v2_df.call_sign_v1),'-','') == F.regexp_replace(F.lower(F.col('call_sign_tivo_clean')),'-',''))
                           ,how = 'left'
                           )
                     .withColumn('tivo_match_flag', F.when(F.col('call_sign_tivo_clean').isNotNull(),1).otherwise(0))
                     )

tms_v1_v2_tivo_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Status of call signs in epg_station and epg_schedule
#----------------------------------------------------------------------------------------------------

(tms_v1_v2_tivo_df
 .groupBy('lmdb_active_flag'
          ,'in_epg_schedule_flag'
          ,'in_epg_schedule_L30D_flag'
          )
 .agg(F.count('*').alias('n_tms_v1')
      ,F.sum('tms_v2_match_flag').alias('n_tms_v2_exact_match')
      ,F.sum('tivo_exact_match_flag').alias('n_tivo_exact_match')
      ,F.sum('tivo_match_flag').alias('n_tivo_match')
      )
 .orderBy(F.lit(1),F.lit(2),F.lit(3))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TMS v1 <-> TMS v2 -- Exact Matches Schedule Matching

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table Setup

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# lmdb active with TMS v1 schedule & tms v2 match
#----------------------------------------------------------------------------------------------------

tms_v1_active_with_sched = (tms_v1_v2_tivo_df
                            .filter((F.col('lmdb_active_flag') == 1) & (F.col('tms_v2_match_flag') == 1))
                            .join(tms_v1_schedule_df.filter(F.col('airdate').between(start_ts, end_ts))
                                ,how = 'left'
                                ,on = (tms_v1_v2_tivo_df.station_id == tms_v1_schedule_df.fk_station_id)
                                )
                            .join(tms_v1_show_df.select('show_id','database_key','title','epi_title')
                                ,how = 'left'
                                ,on = (F.col('fk_show_id') == F.col('show_id'))
                                )
                            .selectExpr('station_id'
                                    ,'lmdb_active_flag'
                                    ,'call_sign_v1'
                                    ,'airdate'
                                    ,'from_unixtime(unix_timestamp(airdate) + duration) as end_airdate'
                                    ,'duration'
                                    ,'show_id'
                                    ,'database_key'
                                    ,'title'
                                    ,'epi_title'
                                    ,'call_sign_v2'
                                    ,'prgSvcId'
                                    )
                            )

tms_v1_active_with_sched.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# lmdb active with TMS v1 schedule & tms v2 match and schedule
#----------------------------------------------------------------------------------------------------

duration_v2_regexp = '(H|M|PT)'

tms_v1_active_v2_with_sched = (tms_v1_active_with_sched
                               .join(tms_v2_schedule_df.selectExpr('prgSvcId as prgSvcId_key','airdate as airdate_v2','TMSId','duration as duration_raw_v2')
                                     ,how = 'left'
                                     ,on = ((F.col('prgSvcId') == F.col('prgSvcId_key')) & (F.col('airdate') == F.col('airdate_v2')))
                                     )
                               .join(tms_v2_show_df.selectExpr('TMSId','title as title_v2','episode_title as epi_title_v2')
                                     ,how = 'left'
                                     ,on = 'TMSId'
                                     )
                               .withColumn('duration_v2',F.split(F.col('duration_raw_v2'),duration_v2_regexp)[1]*60*60 + F.split(F.col('duration_raw_v2'),duration_v2_regexp)[2]*60)
                               .withColumn('end_airdate_v2',F.from_unixtime(F.unix_timestamp(F.col('airdate_v2')) + F.col('duration_v2')))
                               )

tms_v1_active_v2_with_sched.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# adding flags to identify matches, by airdate, title, etc.
#----------------------------------------------------------------------------------------------------

tms_v1_active_v2_with_sched_detailed = (tms_v1_active_v2_with_sched
                                        .withColumn('v1_show_flag',F.lit(1))
                                        .withColumn('v2_airdate_match_flag',F.when(F.col('airdate_v2').isNotNull(),1).otherwise(0))
                                        .withColumn('v2_airdate_start_and_end_match_flag',F.when((F.col('airdate') == F.col('airdate_v2')) & (F.col('end_airdate') == F.col('end_airdate_v2')),1).otherwise(0))
                                        .withColumn('v2_title_exact_match_flag',F.when((F.col('title') == F.col('title_v2')),1).otherwise(0))
                                        .withColumn('v2_title_match_flag',F.when((F.regexp_replace(F.lower(F.col('title')),'[^a-zA-Z0-9]', '') == (F.regexp_replace(F.lower(F.col('title_v2')),'[^a-zA-Z0-9]', ''))),1).otherwise(0))
                                        )

tms_v1_active_v2_with_sched_detailed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analysis 

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# results overall
#----------------------------------------------------------------------------------------------------

(tms_v1_active_v2_with_sched_detailed
 .agg(F.sum('v1_show_flag').alias('n_v1_show')
      ,F.sum('v2_airdate_match_flag').alias('n_v2_airdate_match')
      ,F.sum('v2_airdate_start_and_end_match_flag').alias('n_v2_airdate_start_and_end_match')
      ,F.sum('v2_title_exact_match_flag').alias('n_v2_title_exact_match')
      ,F.sum('v2_title_match_flag').alias('n_v2_title_match')
      )
).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# results by call sign overall
#----------------------------------------------------------------------------------------------------

matches_by_call_sign_df = (tms_v1_active_v2_with_sched_detailed
                            .groupBy('call_sign_v1','prgSvcId')
                            .agg(F.sum('v1_show_flag').alias('n_v1_show')
                                ,F.sum('v2_airdate_match_flag').alias('n_v2_airdate_match')
                                ,F.sum('v2_airdate_start_and_end_match_flag').alias('n_v2_airdate_start_and_end_match')
                                ,F.sum('v2_title_exact_match_flag').alias('n_v2_title_exact_match')
                                ,F.sum('v2_title_match_flag').alias('n_v2_title_match')
                                )
                            .withColumn('perc_v2_airdate_match',F.col('n_v2_airdate_match')/F.col('n_v1_show'))
                            .withColumn('perc_v2_airdate_start_and_end_match',F.col('n_v2_airdate_start_and_end_match')/F.col('n_v1_show'))
                            .withColumn('perc_v2_title_exact_match',F.col('n_v2_title_exact_match')/F.col('n_v1_show'))
                            .withColumn('perc_v2_title_match',F.col('n_v2_title_match')/F.col('n_v1_show'))
                            )

matches_by_call_sign_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# show percentage of call signs with 0-10, 11-20, etc. airdate matches & airdate start and end matches
#----------------------------------------------------------------------------------------------------

(matches_by_call_sign_df
 .groupBy((F.floor(F.col('perc_v2_airdate_match')/0.1)/10).alias('perc_v2_airdate_match'))
 .agg(F.count('*').alias('n_call_signs'))
 .orderBy(F.lit(1))
).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# show percentage of call signs with 0-10, 11-20, etc. airdate matches & airdate start and end matches
#----------------------------------------------------------------------------------------------------

(matches_by_call_sign_df
 .groupBy((F.floor(F.col('perc_v2_airdate_start_and_end_match')/0.1)/10).alias('perc_v2_airdate_start_and_end_match'))
 .agg(F.count('*').alias('n_call_signs'))
 .orderBy(F.lit(1))
).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# show percentage of call signs with 0-10, 11-20, etc. airdate matches & airdate start and end matches
#----------------------------------------------------------------------------------------------------

(matches_by_call_sign_df
 .groupBy((F.floor(F.col('perc_v2_title_match')/0.1)/10).alias('perc_v2_title_match'))
 .agg(F.count('*').alias('n_call_signs'))
 .orderBy(F.lit(1))
).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# do call signs with no matches not have TMS v2 schedule data?
#----------------------------------------------------------------------------------------------------

(matches_by_call_sign_df
 .filter(F.col('n_v2_airdate_match') == 0)
 .join(tms_v2_schedule_df
       ,how = 'left'
       ,on = 'prgSvcId'
       )
 .groupBy('call_sign_v1')
 .agg(F.sum(F.when(F.col('airdate').isNotNull(),1)).alias('v2_shows')
      ,F.min('airdate').alias('v2_min_airdate')
      ,F.max('airdate').alias('v2_max_airdate')
      )
).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# call signs with more than 90% title match & 90% start and end airdate match
#----------------------------------------------------------------------------------------------------

perc_airdate_start_and_end_match_threshold = 0.9
perc_v2_title_match_threshold = 0.9

confirmed_v1_v2_exact_matches = (matches_by_call_sign_df
                                 .filter(F.col('perc_v2_airdate_start_and_end_match') >= perc_airdate_start_and_end_match_threshold)
                                 .filter(F.col('perc_v2_title_match') >= perc_v2_title_match_threshold)
                                 )

confirmed_v1_v2_exact_matches.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# call signs with more than > 0%, but not in the confirmed matches set
#----------------------------------------------------------------------------------------------------

unconfirmed_v1_v2_exact_matches = (matches_by_call_sign_df
                                   .filter(F.col('perc_v2_airdate_start_and_end_match') > 0)
                                   .filter(F.col('perc_v2_title_match') > 0)
                                    .filter(~F.col('call_sign_v1').isin(confirmed_v1_v2_exact_matches.select('call_sign_v1').rdd.flatMap(lambda x: x).collect()))
                                   )

unconfirmed_v1_v2_exact_matches.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### TMS v1 <-> TMS v2 -- Cartesian Product Schedule Matching for Call Signs with no strong match

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table Setup

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# active call signs w/o an exact match and 90% airdate start and end match and 90% title match
#----------------------------------------------------------------------------------------------------

v1_active_non_confirmed_match_df = (tms_v1_v2_tivo_df
                                    .filter((F.col('lmdb_active_flag') == 1))
                                    .join(confirmed_v1_v2_exact_matches.selectExpr('call_sign_v1 as call_sign_v1_confirmed')
                                          ,how = 'left'
                                          ,on = (tms_v1_v2_tivo_df.call_sign_v1 == F.col('call_sign_v1_confirmed'))
                                          )
                                    .filter(F.col('call_sign_v1_confirmed').isNull())
                                    )

v1_active_non_confirmed_match_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# lmdb active (w/o confirmed match) TMS v1 schedule
#----------------------------------------------------------------------------------------------------

tms_v1_active_with_sched = (v1_active_non_confirmed_match_df
                            .join(tms_v1_schedule_df.filter(F.col('airdate').between(start_ts, end_ts))
                                ,how = 'left'
                                ,on = (v1_active_non_confirmed_match_df.station_id == tms_v1_schedule_df.fk_station_id)
                                )
                            .join(tms_v1_show_df.select('show_id','database_key','title','epi_title')
                                ,how = 'left'
                                ,on = (F.col('fk_show_id') == F.col('show_id'))
                                )
                            .selectExpr('station_id'
                                    ,'lmdb_active_flag'
                                    ,'call_sign_v1'
                                    ,'airdate'
                                    ,'from_unixtime(unix_timestamp(airdate) + duration) as end_airdate'
                                    ,'duration'
                                    ,'show_id'
                                    ,'database_key'
                                    ,'title'
                                    ,'epi_title'
                                    ,'call_sign_v2 as call_sign_v2_exact_match'
                                    ,'prgSvcId as prgSvcId_exact_match'
                                    )
                            )

tms_v1_active_with_sched.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# lmdb active with TMS v1 schedule & tms v2 cartesian product and schedule
#----------------------------------------------------------------------------------------------------

duration_v2_regexp = '(H|M|PT)'

tms_v1_active_with_sched_v2_cart_prod = (tms_v1_active_with_sched
                                        .join(tms_v2_source_df.selectExpr('callSign as call_sign_v2','prgSvcId as prgSvcId_v2')
                                                ,how = 'left'
                                                ,on = (F.lit(1) == F.lit(1))
                                                )
                                        .join(tms_v2_schedule_df.selectExpr('prgSvcId as prgSvcId_key','airdate as airdate_v2','TMSId','duration as duration_raw_v2')
                                                ,how = 'left'
                                                ,on = ((F.col('prgSvcId_v2') == F.col('prgSvcId_key')) & (F.col('airdate') == F.col('airdate_v2')))
                                                )
                                        .join(tms_v2_show_df.selectExpr('TMSId','title as title_v2','episode_title as epi_title_v2')
                                                ,how = 'left'
                                                ,on = 'TMSId'
                                                )
                                        .withColumn('duration_v2',F.split(F.col('duration_raw_v2'),duration_v2_regexp)[1]*60*60 + F.split(F.col('duration_raw_v2'),duration_v2_regexp)[2]*60)
                                        .withColumn('end_airdate_v2',F.from_unixtime(F.unix_timestamp(F.col('airdate_v2')) + F.col('duration_v2')))
                                        )

tms_v1_active_with_sched_v2_cart_prod.limit(10).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# adding flags to identify matches, by airdate, title, etc.
#----------------------------------------------------------------------------------------------------

tms_v1_active_with_sched_v2_cart_prod_detailed = (tms_v1_active_with_sched_v2_cart_prod
                                                  .withColumn('v1_show_flag',F.lit(1))
                                                  .withColumn('v2_airdate_match_flag',F.when(F.col('airdate_v2').isNotNull(),1).otherwise(0))
                                                  .withColumn('v2_airdate_start_and_end_match_flag',F.when((F.col('airdate') == F.col('airdate_v2')) & (F.col('end_airdate') == F.col('end_airdate_v2')),1).otherwise(0))
                                                  .withColumn('v2_title_exact_match_flag',F.when((F.col('title') == F.col('title_v2')),1).otherwise(0))
                                                  .withColumn('v2_title_match_flag',F.when((F.regexp_replace(F.lower(F.col('title')),'[^a-zA-Z0-9]', '') == (F.regexp_replace(F.lower(F.col('title_v2')),'[^a-zA-Z0-9]', ''))),1).otherwise(0))
                                                 )

tms_v1_active_with_sched_v2_cart_prod_detailed.limit(10).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# results by call sign v1 & call sign v2
#----------------------------------------------------------------------------------------------------

cart_prod_matches_by_call_sign_df = (tms_v1_active_with_sched_v2_cart_prod_detailed
                                     .groupBy('call_sign_v1','call_sign_v2_exact_match','call_sign_v2','prgSvcId_v2')
                                     .agg(F.sum('v1_show_flag').alias('n_v1_show')
                                         ,F.sum('v2_airdate_match_flag').alias('n_v2_airdate_match')
                                         ,F.sum('v2_airdate_start_and_end_match_flag').alias('n_v2_airdate_start_and_end_match')
                                         ,F.sum('v2_title_exact_match_flag').alias('n_v2_title_exact_match')
                                         ,F.sum('v2_title_match_flag').alias('n_v2_title_match')
                                         )
                                     .withColumn('perc_v2_airdate_match',F.col('n_v2_airdate_match')/F.col('n_v1_show'))
                                     .withColumn('perc_v2_airdate_start_and_end_match',F.col('n_v2_airdate_start_and_end_match')/F.col('n_v1_show'))
                                     .withColumn('perc_v2_title_exact_match',F.col('n_v2_title_exact_match')/F.col('n_v1_show'))
                                     .withColumn('perc_v2_title_match',F.col('n_v2_title_match')/F.col('n_v1_show'))
                                     .withColumn('perc_v2_airdate_match_rank',F.rank().over(Window.partitionBy('call_sign_v1').orderBy(F.col('perc_v2_airdate_match').desc())))
                                     .withColumn('perc_v2_airdate_start_and_end_match_rank',F.rank().over(Window.partitionBy('call_sign_v1').orderBy(F.col('perc_v2_airdate_start_and_end_match').desc())))
                                     .withColumn('perc_v2_title_exact_match_rank',F.rank().over(Window.partitionBy('call_sign_v1').orderBy(F.col('perc_v2_title_exact_match').desc())))
                                     .withColumn('perc_v2_title_match_rank',F.rank().over(Window.partitionBy('call_sign_v1').orderBy(F.col('perc_v2_title_match').desc())))
                                     .withColumn('v2_exact_match_flag',F.when(F.col('call_sign_v1') == F.col('call_sign_v2'),1).otherwise(0))
                                     .orderBy('call_sign_v1','perc_v2_title_match_rank','perc_v2_title_exact_match_rank','perc_v2_airdate_start_and_end_match_rank','perc_v2_airdate_match_rank',F.col('v2_exact_match_flag').desc())
                                     )

# cart_prod_matches_by_call_sign_df.display()

# COMMAND ----------

cart_prod_matches_by_call_sign_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analysis

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# #1 ranked for all call_sign_v1
#----------------------------------------------------------------------------------------------------

call_sign_v1_n_v2_title_match_rank_1 = (cart_prod_matches_by_call_sign_df
                                        .filter((F.col('perc_v2_title_match_rank') == 1))
                                        .groupBy(F.col('call_sign_v1'))
                                        .agg(F.count('*').alias('n_v2_ranked_1_by_title'))
                                        )

cart_prod_matches_by_call_sign_detailed_df = (cart_prod_matches_by_call_sign_df
                                              .join(call_sign_v1_n_v2_title_match_rank_1
                                                    ,how = 'left'
                                                    ,on = 'call_sign_v1'
                                                    )
                                              )

cart_prod_matches_by_call_sign_detailed_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# #1 ranked for all call_sign_v1
#----------------------------------------------------------------------------------------------------

(cart_prod_matches_by_call_sign_df
 .filter(F.col('perc_v2_title_match_rank') == 1)
 .select('call_sign_v1','call_sign_v2_exact_match','call_sign_v2'
         ,'perc_v2_title_match_rank','perc_v2_title_exact_match_rank','perc_v2_airdate_start_and_end_match_rank','perc_v2_airdate_match_rank'
         ,'perc_v2_title_match','perc_v2_title_exact_match','perc_v2_airdate_start_and_end_match','perc_v2_airdate_match'
         ,'*'
         )
).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# #1 ranked for all call_sign_v1 w/ only 1 v2 #1 ranked title match
#----------------------------------------------------------------------------------------------------

(cart_prod_matches_by_call_sign_detailed_df
 .filter(F.col('perc_v2_title_match_rank') == 1)
 .filter((F.col('n_v2_ranked_1_by_title') == 1) | (F.col('v2_exact_match_flag') == 1))
 .select('call_sign_v1','call_sign_v2_exact_match','call_sign_v2'
         ,'perc_v2_title_match_rank','perc_v2_title_exact_match_rank','perc_v2_airdate_start_and_end_match_rank','perc_v2_airdate_match_rank'
         ,'perc_v2_title_match','perc_v2_title_exact_match','perc_v2_airdate_start_and_end_match','perc_v2_airdate_match'
         ,'*'
         )
).display()

# COMMAND ----------

tms_v1_source_df.filter(F.col('station_call_sign').isin('KAKEDT2')).display()

# COMMAND ----------

tms_v2_source_df.filter(F.col('callSign').isin('WICUDT2')).display()

# COMMAND ----------

tms_v2_schedule_df.filter(F.col('prgSvcId').isin('56032')).display()

# COMMAND ----------

tms_v2_exp_schedule_df.filter(F.col('prgSvcId').isin('56032')).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# #1 ranked for all call_sign_v1
#----------------------------------------------------------------------------------------------------

(cart_prod_matches_by_call_sign_df
 .filter((F.col('perc_v2_title_match_rank') == 1))
 .select('call_sign_v1','call_sign_v2_exact_match','call_sign_v2'
         ,'perc_v2_title_match_rank','perc_v2_title_exact_match_rank','perc_v2_airdate_start_and_end_match_rank','perc_v2_airdate_match_rank'
         ,'perc_v2_title_match','perc_v2_title_exact_match','perc_v2_airdate_start_and_end_match','perc_v2_airdate_match'
         ,'*'
         )
).display()

# COMMAND ----------



# COMMAND ----------


(cart_prod_matches_by_call_sign_df
 .filter((F.col('call_sign_v1') == 'WTLJDT2'))
).display()

# COMMAND ----------

(tms_v1_active_with_sched
 .filter(F.col('call_sign_v1') == 'WTLJDT2')
).display()

# COMMAND ----------

(tms_v2_schedule_df
 .filter(F.col('airdate').between(start_ts, end_ts))
 .filter(F.col('prgSvcId') == 44941)
 .join(tms_v2_show_df
       ,how = 'left'
       ,on = 'TMSId'
       )
).display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### TMS v1 <-> TMS v2 Experimental

# COMMAND ----------

# MAGIC %md
# MAGIC #### TMS v2 Experimental Exploration

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TiVo exploration
#----------------------------------------------------------------------------------------------------

tms_v2_exp_source_df.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# TMS v2 columns, distinct records, etc.
#----------------------------------------------------------------------------------------------------

(tms_v2_exp_source_df
 .agg(F.count('prgsvcid').alias('n_prgsvcid')
      ,F.countDistinct('prgsvcid').alias('nD_prgsvcid')
      ,F.count('callsign').alias('n_call_sign')
      ,F.countDistinct('callsign').alias('nD_call_sign')
      ,F.count('channelvalue').alias('n_channelvalue')
      ,F.countDistinct('channelvalue').alias('nD_channelvalue')
      ,F.count('channelnum').alias('n_channelnum')
      ,F.countDistinct('channelnum').alias('nD_channelnum')
      )
 ).display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Scratch

# COMMAND ----------



# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Redbee sample schedule -- min & max air_datetime
#----------------------------------------------------------------------------------------------------

time_range = (redbee_sample_schedule_df
              .agg(F.min(F.col('air_datetime')).alias('min_air_datetime')
                   ,F.max(F.col('air_datetime')).alias('max_air_datetime')
                   )
              )

min_air_datetime = time_range.select('min_air_datetime').first()[0]
max_air_datetime = time_range.select('max_air_datetime').first()[0]

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Gracenote EPG station data
#----------------------------------------------------------------------------------------------------

epg_station_df = (spark.table(EPG_STATION)
                #   .select('station_id'
                #           ,'station_name'
                #           ,'station_call_sign'
                #           ,'fk_dma_id'
                #           ,'local_or_national'
                #           )
                  )

epg_station_df.limit(5).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Gracenote EPG schedule data
#----------------------------------------------------------------------------------------------------

epg_schedule_df = (spark.table(EPG_SCHEDULE)
                   .filter(F.col('airdate') >= min_air_datetime)
                   .filter(F.col('airdate') <= max_air_datetime)
                   )

epg_schedule_df.limit(5).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Gracenote EPG station schedule complete
#----------------------------------------------------------------------------------------------------

epg_station_schedule_df = (epg_station_df
                           .join(epg_schedule_df
                                 ,on = (epg_station_df.station_id == epg_schedule_df.fk_station_id)
                                 ,how = 'inner'
                                 )
                           )

epg_station_schedule_df.limit(5).display()                                 

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Gracenote EPG station schedule distinct
#----------------------------------------------------------------------------------------------------

distinct_epg_station_schedule_df = (epg_station_schedule_df
                                    .select('station_id','station_num','station_name','station_call_sign')
                                    .distinct()
                                    )

distinct_epg_station_schedule_df.limit(5).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# EPG <-> Redbee call sign mapping (provided by PM)
#----------------------------------------------------------------------------------------------------

query = f'''
SELECT
    *
FROM
    {EPG_REDBEE_STATION_MAPPING}
'''

call_sign_mapping_df = query_redshift(query, redshift)
call_sign_mapping_df.display()

# COMMAND ----------

call_sign_mapping_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Call Sign Comparison

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Distinct Gracenote EPG Call Signs
#----------------------------------------------------------------------------------------------------

n_gracenote_call_signs =  epg_station_df.select('station_call_sign').distinct().count()

print(n_gracenote_call_signs)

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Distinct Gracenote EPG Call Signs w/ a schedule during sample period
#----------------------------------------------------------------------------------------------------

n_gracenote_call_signs_with_sched =  (distinct_epg_station_schedule_df
                                      .select('station_call_sign')
                                      .distinct()
                                      .count()
                                      )

n_gracenote_call_signs_with_sched

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Distinct Gracenote EPG Call Signs from PM provided mapping table
#----------------------------------------------------------------------------------------------------

n_gracenote_call_signs_with_sched_and_map = (call_sign_mapping_df
                                             .join(distinct_epg_station_schedule_df
                                                   ,on = (call_sign_mapping_df.epg_callsign == distinct_epg_station_schedule_df.station_call_sign)
                                                   ,how = 'inner'
                                                   )
                                             .count()
                                             )

n_gracenote_call_signs_with_sched_and_map

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# EPG <-> Redbee call sign mapping (provided by PM) -- reduced to only Gracenote EPGs with schedule
#----------------------------------------------------------------------------------------------------

reduced_call_sign_mapping_df = (call_sign_mapping_df
                                .join(distinct_epg_station_schedule_df
                                      ,on = (call_sign_mapping_df.epg_callsign == distinct_epg_station_schedule_df.station_call_sign)
                                      ,how = 'inner'
                                      )
                                )

reduced_call_sign_mapping_df.limit(5).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Distinct Gracenote EPG Call Signs from PM provided mapping table (for call signs with schedule data)
#----------------------------------------------------------------------------------------------------

n_gracenote_call_signs_with_sched_and_map = (reduced_call_sign_mapping_df
                                             .count()
                                             )

n_gracenote_call_signs_with_sched_and_map

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# EPG <-> Redbee call sign mapping (provided by PM) -- reduced to only Gracenote EPGs with schedule & no redbee call sign
#----------------------------------------------------------------------------------------------------

redbee_call_signs_mapped_to_gracenote_call_signs_with_sched = (reduced_call_sign_mapping_df
                                                               .filter(F.col('redbee_callsign').isNotNull())
                                                               .filter(F.col('redbee_callsign') != '')
                                                               )

redbee_call_signs_mapped_to_gracenote_call_signs_with_sched.limit(5).display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Distinct Redbee EPG call signs from PM provided mapping table (for call signs with EPG schedule data)
#----------------------------------------------------------------------------------------------------

n_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched = redbee_call_signs_mapped_to_gracenote_call_signs_with_sched.count()

n_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Gracenote EPG call sign with schedule data, but no Redbee call sign mapping
#----------------------------------------------------------------------------------------------------

redbee_call_signs_not_mapped_to_gracenote_call_signs_with_sched = (reduced_call_sign_mapping_df
                                                                   .filter(F.col('redbee_callsign').isNull() | (F.col('redbee_callsign') == ''))
                                                                   )

redbee_call_signs_not_mapped_to_gracenote_call_signs_with_sched.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Redbee sample call signs mapped to PM provided mapping table (for call signs with EPG schedule data)
#----------------------------------------------------------------------------------------------------

redbees_sample_mapped_call_signs = (reduced_call_sign_mapping_df
                                    .join(redbee_sample_source_df
                                          ,on = (F.lower(reduced_call_sign_mapping_df.redbee_callsign) == F.lower(F.regexp_replace(redbee_sample_source_df.call_sign,'-','')))
                                          ,how = 'inner'
                                          )
                                    )

redbees_sample_mapped_call_signs.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Count of Redbee sample call signs mapped to PM provided mapping table (for call signs with EPG schedule data)
#----------------------------------------------------------------------------------------------------

n_sample_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched = redbees_sample_mapped_call_signs.count()

n_sample_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Redbee call signs mapped to PM provided mapping table (for call signs with EPG schedule data), but no match in the Redbee provided sample
#----------------------------------------------------------------------------------------------------

redbee_call_signs_mapped_to_gracenote_but_not_present_in_sample = (reduced_call_sign_mapping_df
                                                                   .join(redbee_sample_source_df
                                                                         ,on = (F.lower(reduced_call_sign_mapping_df.redbee_callsign) == F.lower(F.regexp_replace(redbee_sample_source_df.call_sign,'-','')))
                                                                         ,how = 'left'
                                                                         )
                                                                   .filter(F.col('call_sign').isNull())
                                                                   )

redbee_call_signs_mapped_to_gracenote_but_not_present_in_sample.display()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------
# Redbee sample call signs not mapped to PM provided mapping table (for call signs with EPG schedule data)
#----------------------------------------------------------------------------------------------------

redbee_sample_call_signs_not_mapped_to_mapping_df = (reduced_call_sign_mapping_df
                                                     .join(redbee_sample_source_df
                                                           ,on = (F.lower(reduced_call_sign_mapping_df.redbee_callsign) == F.lower(F.regexp_replace(redbee_sample_source_df.call_sign,'-','')))
                                                           ,how = 'right'
                                                           )
                                                     .filter(F.col('epg_callsign').isNull())
                                                     )

redbee_sample_call_signs_not_mapped_to_mapping_df.display()

# COMMAND ----------

print(f'n_gracenote_call_signs: {n_gracenote_call_signs}')
print(f'n_gracenote_call_signs_with_sched: {n_gracenote_call_signs_with_sched}; {round(n_gracenote_call_signs_with_sched/n_gracenote_call_signs,2)*100}%')
print(f'n_gracenote_call_signs_with_sched_and_map: {n_gracenote_call_signs_with_sched_and_map}; {round(n_gracenote_call_signs_with_sched_and_map/n_gracenote_call_signs,2)*100}%')
print(f'n_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched: {n_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched}; {round(n_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched/n_gracenote_call_signs,2)*100}%; redbee_call_signs_not_mapped_to_gracenote_call_signs_with_sched')

print(f'n_sample_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched: {n_sample_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched}; {round(n_sample_redbee_call_signs_mapped_to_gracenote_call_signs_with_sched/n_gracenote_call_signs,2)*100}%; redbee_call_signs_mapped_to_gracenote_but_not_present_in_sample')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


