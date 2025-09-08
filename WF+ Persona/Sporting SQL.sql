-- Databricks notebook source
SET ANSI_MODE = false;

-- COMMAND ----------

SELECT sctv_channel_category[0], sctv_channel_name[0], COUNT(*), SUM(session_duration)/3600.0
FROM datalake_shares.data_lake.agg_wfp_live_channel_sessions_v1
WHERE session_start >= '2025-02-01'
  and session_duration >= 180
  AND sctv_channel_category[0] in ('SPORTS','SPORTS + OUTDOOR')
  AND sctv_channel_name[0] NOT IN ('Xtreme Outdoor Presented By History', 'Duck Dynasty', 'Ice Road Truckers','Ax Men','BBC Top Gear','Waypoint TV','Outside tv')
GROUP BY 1, 2

-- COMMAND ----------

-- SELECT ch.sctv_channel_category[0]
SELECT pr.epg_content_title
-- , ch.sctv_channel_name[0]
, COUNT(DISTINCT ch.token) AS device_count
, SUM(pr.program_session_duration)/3600.0 AS total_duration
FROM datalake_shares.data_lake.agg_wfp_live_channel_sessions_v1 ch
JOIN datalake_shares.data_lake.agg_wfp_live_program_sessions_v1 pr
  ON ch.device_id = pr.device_id
 AND ch.token = pr.token
 AND ch.sctv_session_id = pr.sctv_session_id
WHERE ch.session_start >= '2024-08-01'
  AND ch.session_start < '2024-12-01'
  and ch.session_duration >= 180
  AND pr.program_session_duration > 0
  AND pr.program_session_duration IS NOT NULL
  AND pr.epg_content_genre[0] LIKE '%sports%'
  -- AND (pr.epg_content_title ILIKE '%soccer%' OR pr.epg_content_title ILIKE '%futbol%' OR pr.epg_content_title ILIKE ' liga%' OR pr.epg_content_title ILIKE '%liga ' OR pr.epg_content_title = 'Premier League' OR pr.epg_content_title = '%MSL%' OR pr.epg_content_title IN ('Premier League', 'Champions League', 'Serie A', 'Ligue 1'))
GROUP BY 1
ORDER BY 3 DESC
LiMIT 1000

-- COMMAND ----------

-- SELECT ch.sctv_channel_category[0]
SELECT pr.epg_content_title
-- , ch.sctv_channel_name[0]
, COUNT(DISTINCT ch.token) AS device_count
, SUM(pr.program_session_duration)/3600.0 AS total_duration
FROM datalake_shares.data_lake.agg_wfp_live_channel_sessions_v1 ch
JOIN datalake_shares.data_lake.agg_wfp_live_program_sessions_v1 pr
  ON ch.device_id = pr.device_id
 AND ch.token = pr.token
 AND ch.sctv_session_id = pr.sctv_session_id
WHERE ch.session_start >= '2024-08-01'
  AND ch.session_start < '2024-12-01'
  and ch.session_duration >= 180
  AND pr.program_session_duration > 0
  AND pr.program_session_duration IS NOT NULL
  AND pr.epg_content_genre[0] LIKE '%sports%'
  -- AND (pr.epg_content_title ILIKE '%soccer%' OR pr.epg_content_title ILIKE '%futbol%' OR pr.epg_content_title ILIKE ' liga%' OR pr.epg_content_title ILIKE '%liga ' OR pr.epg_content_title = 'Premier League' OR pr.epg_content_title = '%MSL%' OR pr.epg_content_title IN ('Premier League', 'Champions League', 'Serie A', 'Ligue 1'))
GROUP BY 1
ORDER BY 3 DESC
LiMIT 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Steps:
-- MAGIC - Indentify features
-- MAGIC - Create index calculation to identify 

-- COMMAND ----------

SELECT ch.device_id, ch.token, ch.sctv_session_id, ch.airings_key, ch.channel_session_id, ch.watchfreeplus_session_id, ch.sctv_channel_id, ch.sctv_channel_name, ch.sctv_source_id, ch.sctv_channel_category, ch.session_start, ch.session_end, ch.offset_tz, ch.local_session_start, ch.local_session_end, ch.session_duration, ch.clienttype, ch.sctv_category_id, ch.sctv_source_title, ch.app_version, pr.epg_series_id,pr.epg_series_title,pr.epg_content_id,pr.epg_content_title,pr.epg_season_number,pr.epg_episode_number,pr.epg_content_genre,pr.program_schedule_start,pr.program_schedule_end,pr.local_program_schedule_start,pr.local_program_schedule_end,pr.program_watch_start,pr.program_watch_end,pr.local_program_watch_start,pr.local_program_watch_end, pr.program_session_duration
FROM datalake_shares.data_lake.agg_wfp_live_channel_sessions_v1 ch
JOIN datalake_shares.data_lake.agg_wfp_live_program_sessions_v1 pr
  ON ch.device_id = pr.device_id
 AND ch.token = pr.token
 AND ch.sctv_session_id = pr.sctv_session_id
WHERE ch.session_start >= '2025-02-09'
  AND ch.session_start >= '2025-02-12'
  and ch.session_duration >= 180
  AND ch.sctv_channel_category[0] in ('SPORTS','SPORTS + OUTDOOR')
LIMIT 1000

-- COMMAND ----------


SELECT *
FROM datalake_shares.data_lake.agg_wfp_live_channel_sessions_v1
WHERE session_start >= '2025-01-15'
  and session_duration >= 180
  AND sctv_channel_category[0] in ('SPORTS','SPORTS + OUTDOOR')
LIMIT 1000

-- COMMAND ----------

SELECT sctv_channel_category[0], COUNT(*), COUNT(DISTINCT token)
FROM datalake_shares.data_lake.agg_wfp_live_channel_sessions_v1
WHERE session_start >= '2025-01-15'
  and session_duration >= 180
GROUP BY 1

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.sporting_wfp_persona_dataset;
CREATE TABLE dev.mohit_gangwani.sporting_wfp_persona_dataset AS
WITH wfp_usage AS (
  with by_token_session as (
    select token,
    sctv_session_id,
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
    where (date(session_start) between '2025-01-15' and '2025-02-15')
    and session_duration >= 180
    group by 1, 2
)
  select token,
  count(distinct case when app_seconds >= 180 then sctv_session_id end) as num_3min_sessions, 
  sum(viewing_seconds)/(60*60)  as viewing_hours,
  sum(fast_seconds)/(60*60)  as fast_hours,
  sum(ota_seconds)/(60*60)  as ota_hours,
  avg(distinct_channels) as avg_distinct_channels_per_session,
  avg(distinct_FAST_channels) as avg_distinct_fast_channels_per_session,
  avg(distinct_ota_channels) as avg_distinct_ota_channels_per_session,
  avg(distinct_categories) as avg_distinct_categories_per_session,
  AVG(viewing_seconds) AS avg_session_duration,
  count(distinct session_start::date) as num_days_active,
  sum(fast_seconds)/SUM(CASE WHEN fast_seconds > 0 THEN 1 ELSE 0 END) AS avg_fast_session_duration,
  sum(ota_seconds)/SUM(CASE WHEN ota_seconds > 0 THEN 1 ELSE 0 END) AS avg_ota_session_duration
  from by_token_session
  group by 1
)
, content_preference AS (
  select token,
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
  -- sum(case when sctv_channel_category[0] in ('INFOMERCIALS') then session_duration end) as INFOMERCIALS_hours,
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
  min(datediff(day, session_start::date, '2025-02-15' )) as last_session, 
  min(case when session_duration >= 180 then datediff(day,  session_start::date, '2024-12-31') end) as last_3m_session
  from datalake_shares.data_lake.agg_wfp_live_channel_sessions_v1
  where (date(session_start) between '2025-01-15' and '2025-02-15') 
    and session_duration is not null
  group by 1
)
SELECT cp.*,
wf.num_3min_sessions,
wf.viewing_hours,
wf.fast_hours,
wf.ota_hours,
wf.avg_distinct_channels_per_session,
wf.avg_distinct_fast_channels_per_session,
wf.avg_distinct_ota_channels_per_session,
wf.avg_distinct_categories_per_session,
wf.num_days_active,
wf.avg_session_duration,
wf.avg_fast_session_duration,
wf.avg_ota_session_duration
FROM content_preference cp
JOIN wfp_usage wf
  ON wf.token = cp.token

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


