-- Databricks notebook source
SET ANSI_MODE = false;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.wfp_persona_dataset_wtp;
CREATE TABLE dev.mohit_gangwani.wfp_persona_dataset_wtp AS
WITH wfp_usage AS (
  with by_token_session as (
    select token,
    sctv_session_id,
        CASE WHEN DATE_PART('DOW', local_session_start) = 7 OR DATE_PART('DOW', local_session_start) = 1 THEN 'Weekend'
         WHEN DATE_PART('hour', local_session_start) >= 23 OR DATE_PART('hour', local_session_start) < 6 THEN 'Overnight'
         WHEN DATE_PART('hour', local_session_start) >= 6 AND DATE_PART('hour', local_session_start) < 12 THEN 'Morning'
         WHEN DATE_PART('hour', local_session_start) >= 12 AND DATE_PART('hour', local_session_start) < 18 THEN 'Daytime'
         WHEN DATE_PART('hour', local_session_start) >= 18 AND DATE_PART('hour', local_session_start) < 23 THEN  'Primetime' END AS week_time,
    min(session_start) as session_start, 
    datediff(second, min(session_start),max(session_end)) as app_seconds, 
    sum(session_duration) as viewing_seconds, 
    sum(case when sctv_channel_category[0] not in ('ANTENNA') then session_duration end) as fast_seconds, 
    sum(case when sctv_channel_category[0] in ('ANTENNA')  then session_duration end) as ota_seconds, 
    count(distinct airings_key) as distinct_channels,
    count(distinct case when sctv_channel_category[0] not in ('ANTENNA', 'LOCAL CHANNELS') then airings_key end) as distinct_FAST_channels,
    count(distinct case when sctv_channel_category[0] in ('ANTENNA')  then airings_key end) as distinct_ota_channels,
    count(distinct case when sctv_channel_category[0] in ('LOCAL CHANNELS')  then airings_key end) as distinct_ott_channels,
    count(distinct sctv_channel_category[0]) as distinct_categories
    from datalake_shares.data_lake.agg_wfp_live_channel_sessions_v1
    where (date(session_start) between '2024-12-01' and '2024-12-31')
    and session_duration >= 180
    group by 1,2,3
)
  select token,
  week_time,
  CASE WHEN week_time = 'Overnight' THEN 0
       WHEN week_time = 'Morning' THEN 1
       WHEN week_time = 'Daytime' THEN 2
       WHEN week_time = 'Primetime' THEN 3
       WHEN week_time = 'Weekend' THEN 4 END AS wt_nums,
  count(distinct sctv_session_id) as num_sessions,
  count(distinct case when app_seconds >= 180 then sctv_session_id end) as num_3min_sessions, 
  sum(viewing_seconds)/(60*60)  as viewing_hours,
  sum(fast_seconds)/(60*60)  as fast_hours,
  sum(ota_seconds)/(60*60)  as ota_hours,
  avg(distinct_channels) as avg_distinct_channels_per_session,
  avg(distinct_FAST_channels) as avg_distinct_fast_channels_per_session,
  avg(distinct_ota_channels) as avg_distinct_ota_channels_per_session,
  avg(distinct_categories) as avg_distinct_categories_per_session,
  count(distinct session_start::date) as num_days_active
  from by_token_session
  group by 1, 2, 3
)
, content_preference AS (
  select token,
        CASE WHEN DATE_PART('DOW', local_session_start) = 7 OR DATE_PART('DOW', local_session_start) = 1 THEN 'Weekend'
         WHEN DATE_PART('hour', local_session_start) >= 23 OR DATE_PART('hour', local_session_start) < 6 THEN 'Overnight'
         WHEN DATE_PART('hour', local_session_start) >= 6 AND DATE_PART('hour', local_session_start) < 12 THEN 'Morning'
         WHEN DATE_PART('hour', local_session_start) >= 12 AND DATE_PART('hour', local_session_start) < 18 THEN 'Daytime'
         WHEN DATE_PART('hour', local_session_start) >= 18 AND DATE_PART('hour', local_session_start) < 23 THEN  'Primetime' END AS week_time, 
  count(distinct airings_key) all_unique_channels,  
  count(distinct case when sctv_channel_category[0] != 'ANTENNA' then airings_key end) all_unique_fast_channels,
  count(distinct case when sctv_channel_category[0] = 'ANTENNA' then airings_key end) all_unique_ota_channels, 
  count(distinct sctv_channel_category[0]) all_unique_categories,
  sum(case when sctv_channel_category[0] in ('COMEDY') then session_duration end) as comedy_hours, 
  sum(case when sctv_channel_category[0] in ('CRIME') then session_duration end) as CRIME_hours, 
  sum(case when sctv_channel_category[0] in ('CULTURE + LIFESTYLE', 'INTERESTS + LIFESTYLE') then session_duration end) as lifestyle_hours, 
  sum(case when sctv_channel_category[0] in ('DISCOVER') then session_duration end) as DISCOVER_hours, 
  sum(case when sctv_channel_category[0] in ('ENTERTAINMENT') then session_duration end) as ENTERTAINMENT_hours, 
  sum(case when sctv_channel_category[0] in ('FOOD + TRAVEL','HOME + FOOD') then session_duration end) as food_hours, 
  sum(case when sctv_channel_category[0] in ('FEATURED') then session_duration end) as FEATURED_hours, 
  sum(case when sctv_channel_category[0] in ('GAME SHOWS','GAME SHOWS + REALITY') then session_duration end) as gameshow_hours, 
  sum(case when sctv_channel_category[0] in ('GAMING + ANIME') then session_duration end) as gaming_anime_hours, 
  sum(case when sctv_channel_category[0] in ('HISTORY + DOCUMENTARY', 'HISTORY + DOCS') then session_duration end) as HISTORY_DOCUMENTARY_hours, 
  sum(case when sctv_channel_category[0] in ('HOME','HOME + FOOD') then session_duration end) as home_hours, 
  sum(case when sctv_channel_category[0] in ('INFOMERCIALS') then session_duration end) as INFOMERCIALS_hours,
  sum(case when sctv_channel_category[0] in ('SHOPPING') then session_duration end) as SHOPPING_hours,
  sum(case when sctv_channel_category[0] in ('KIDS + FAMILY') then session_duration end) as KIDS_FAMILY_hours, 
  sum(case when sctv_channel_category[0] LIKE 'LATINO%' OR sctv_channel_category[0] LIKE 'EN ESP%' then session_duration end) as LATINO_hours, 
  sum(case when sctv_channel_category[0] in ('MOOD + AMBIANCE') then session_duration end) as MOOD_AMBIANCE_hours, 
  sum(case when sctv_channel_category[0] in ('MOVIES','MOVIES + TV') then session_duration end) as MOVIES_hours, 
  sum(case when sctv_channel_category[0] in ('MUSIC','MUSIC VIDEOS') then session_duration end) as MUSIC_hours,
  sum(case when sctv_channel_category[0] in ('NATURE + SCIENCE') then session_duration end) as NATURE_SCIENCE_hours, 
  sum(case when sctv_channel_category[0] in ('NEWS + OPINION') then session_duration end) as NEWS_OPINION_hours,
  sum(case when sctv_channel_category[0] in ('REALITY','GAME SHOWS + REALITY') then session_duration end) as REALITY_hours, 
  sum(case when sctv_channel_category[0] in ('TV','MOVIES + TV') then session_duration end) as TV_hours, 
  sum(case when sctv_channel_category[0] in ('SPORTS','SPORTS + OUTDOOR') then session_duration end) as SPORTS_hours, 
  sum(case when sctv_channel_category[0] in ('WESTERNS','WESTERNS + CLASSIC TV', 'WESTERNS + CLASSICS') then session_duration end) as WESTERNS_hours,
  sum(case when sctv_channel_category[0] in ('ANTENNA','LOCAL CHANNELS') then session_duration end) as local_content_hours,
  min(datediff(day, session_start::date, '2025-01-31' )) as last_session, 
  min(case when session_duration >= 180 then datediff(day,  session_start::date, '2024-12-31') end) as last_3m_session
  from datalake_shares.data_lake.agg_wfp_live_channel_sessions_v1
  where (date(session_start) between '2025-01-01' and '2025-01-31') 
    and session_duration is not null
  group by 1, 2
)
SELECT cp.*, wf.num_sessions,
wf.num_3min_sessions,
wf.viewing_hours,
wf.fast_hours,
wf.ota_hours,
wf.avg_distinct_channels_per_session,
wf.avg_distinct_fast_channels_per_session,
wf.avg_distinct_ota_channels_per_session,
wf.avg_distinct_categories_per_session,
wf.num_days_active,
wf.wt_nums
FROM content_preference cp
JOIN wfp_usage wf
  ON wf.token = cp.token
 AND wf.week_time = cp.week_time

-- COMMAND ----------

SELECT * FROM dev.mohit_gangwani.wfp_persona_dataset_wtp
LIMIT 100

-- COMMAND ----------

SET ANSI_MODE = false;
select case when sctv_channel_category[0] in ('COMEDY') then 'Comedy+'
            when sctv_channel_category[0] in ('CRIME') then 'CRIME+'
            when sctv_channel_category[0] in ('CULTURE + LIFESTYLE', 'INTERESTS + LIFESTYLE') then 'lifestyle+'
            when sctv_channel_category[0] in ('DISCOVER') then 'DISCOVER+'
            when sctv_channel_category[0] in ('ENTERTAINMENT') then 'ENTERTAINMENT+'
            when sctv_channel_category[0] in ('FOOD + TRAVEL','HOME + FOOD') then 'food+'
            when sctv_channel_category[0] in ('FEATURED') then 'FEATURED+'
            when sctv_channel_category[0] in ('GAME SHOWS','GAME SHOWS + REALITY') then 'gameshow+'
            when sctv_channel_category[0] in ('GAMING + ANIME') then 'gaming + anime+'
            when sctv_channel_category[0] in ('HISTORY + DOCUMENTARY', 'HISTORY + DOCS') then 'HISTORY + DOCUMENTARY+'
            when sctv_channel_category[0] in ('HOME','HOME + FOOD') then 'home+'
            when sctv_channel_category[0] in ('INFOMERCIALS', 'SHOPPING') then 'INFOMERCIALS+'
            when sctv_channel_category[0] in ('KIDS + FAMILY') then 'KIDS + FAMILY+'
            when sctv_channel_category[0] LIKE 'LATINO%' then 'LATINO+'
            when sctv_channel_category[0] LIKE 'EN ESP%' then 'LATINO+'
            when sctv_channel_category[0] in ('MOOD + AMBIANCE') then 'MOOD + AMBIANCE+'
            when sctv_channel_category[0] in ('MOVIES','MOVIES + TV') then 'MOVIES+'
            when sctv_channel_category[0] in ('MUSIC','MUSIC VIDEOS') then 'MUSIC+'
            when sctv_channel_category[0] in ('NATURE + SCIENCE') then 'NATURE + SCIENCE+'
            when sctv_channel_category[0] in ('NEWS + OPINION') then 'NEWS + OPINION+'
            when sctv_channel_category[0] in ('REALITY','GAME SHOWS + REALITY') then 'REALITY+'
            when sctv_channel_category[0] in ('TV','MOVIES + TV') then 'TV+'
            when sctv_channel_category[0] in ('SPORTS','SPORTS + OUTDOOR') then 'SPORTS+'
            when sctv_channel_category[0] in ('WESTERNS','WESTERNS + CLASSIC TV', 'WESTERNS + CLASSICS') then  'WESTERNS+'
            when sctv_channel_category[0] in ('ANTENNA','LOCAL CHANNELS') then  'OTHER - Local Channels+'
            ELSE sctv_channel_category[0] END AS genre
, COUNT(*)
from datalake_shares.agg.agg_wfp_live_channel_sessions_v1
where session_start between '2024-12-01' and '2024-12-31'
  and session_duration is not null
group by 1

-- COMMAND ----------


SELECT week_time
-- , day_hour
, SUM(app_hours) AS ttl_app_hours
, SUM(viewing_hours) AS total_viewing_hours
, COUNT(DISTINCT token) AS total_tvs
, AVG(app_hours) AS avg_app_hrs_per_tv
, AVG(viewing_hours) AS avg_viewing_hrs_per_tv
FROM (
select token,
    sctv_session_id,
    CASE WHEN DATE_PART('DOW', local_session_start) = 7 OR DATE_PART('DOW', local_session_start) = 1 THEN 'Weekend'
         WHEN DATE_PART('hour', local_session_start) >= 23 OR DATE_PART('hour', local_session_start) < 6 THEN 'Overnight'
         WHEN DATE_PART('hour', local_session_start) >= 6 AND DATE_PART('hour', local_session_start) < 12 THEN 'Morning'
         WHEN DATE_PART('hour', local_session_start) >= 12 AND DATE_PART('hour', local_session_start) < 18 THEN 'Daytime'
         WHEN DATE_PART('hour', local_session_start) >= 18 AND DATE_PART('hour', local_session_start) < 23 THEN  'Primetime' END AS week_time,
    min(session_start) as session_start, 
    datediff(second, min(session_start),max(session_end))/3600.0 as app_hours, 
    sum(session_duration)/3600.0 as viewing_hours
    -- sum(case when sctv_channel_category[0] not in ('ANTENNA') then session_duration end) as fast_seconds, 
    -- sum(case when sctv_channel_category[0] in ('ANTENNA')  then session_duration end) as ota_seconds, 
    -- count(distinct airings_key) as distinct_channels,
    -- count(distinct case when sctv_channel_category[0] not in ('ANTENNA', 'LOCAL CHANNELS') then airings_key end) as distinct_FAST_channels,
    -- count(distinct case when sctv_channel_category[0] in ('ANTENNA')  then airings_key end) as distinct_ota_channels,
    -- count(distinct case when sctv_channel_category[0] in ('LOCAL CHANNELS')  then airings_key end) as distinct_ott_channels,
    -- count(distinct sctv_channel_category[0]) as distinct_categories
    from datalake_shares.agg.agg_wfp_live_channel_sessions_v1
    where (date(session_start) between '2024-12-01' and '2024-12-31')
    and session_duration >= 180
    group by 1,2,3
    )
GROUP BY 1

-- COMMAND ----------

SELECT DATE(local_session_start) AS session_date
, DATE_PART('DOW', local_session_start) AS session_day
, COUNT(*)
from datalake_shares.agg.agg_wfp_live_channel_sessions_v1
where (date(session_start) between '2024-12-01' and '2024-12-08')
  and session_duration >= 180
group by 1

-- COMMAND ----------


