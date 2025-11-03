-- Databricks notebook source

CREATE TABLE dev.mohit_gangwani.metrics_population_report_onn AS
with viewing_sessions as (
	SELECT fk_tvid, fk_content_id, session_start
	FROM prod.detection_onn.viewing_content_firehose
	WHERE fk_tvid IN (SELECT DISTINCT tvid FROM detection.tv_zoo_latest_daily WHERE zoo = 'control-zoo-dtsprod.tvinteractive.tv')
	AND session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
  GROUP BY ALL
),

ispot_commercials as (
	SELECT vc.fk_tvid, session_start
	FROM prod.detection_onn.viewing_commercials_firehose AS vc
	INNER JOIN prod.detection.tv_zoo_latest_daily AS zoo
	ON vc.fk_tvid = zoo.tvid
	LEFT JOIN prod.detection.commercial_id_external_firehose AS cie
	ON vc.fk_commercial_id = cie.fk_commercial_id
	WHERE cie.fk_client_id = 8
	and vc.session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
	and zoo.zoo_id = 17
),

one_year_opted_in as (
  select '1 year active (opted in)' as category,
    current_date as create_timestamp,
    count(distinct a.fk_tvid) as one_year_active
  from prod.detection.tv_populations a
  inner join detection.populations b
    on a.fk_population_id = b.population_id
  inner join detection.tv_activity_latest c
    on a.fk_tvid = c.tvid
  where b.population_name = 'opted_in'
    and c.session_start >= CURRENT_DATE - INTERVAL '365 DAYS'
),

thirty_day_reporting as (
  select
    current_date as create_timestamp,
    count(distinct fk_tvid) as thirty_day_reporting
    from viewing_sessions
),

thirty_day_detecting as (
  select 
    current_date as create_timestamp,
    count(distinct fk_tvid) as thirty_day_detecting
  from viewing_sessions
  where fk_content_id != 3468026 --3468026 is null detection
),

one_day_detecting as (
  select 
    current_date as create_timestamp,
    count(distinct fk_tvid) as one_day_detecting
  from viewing_sessions
  where fk_content_id != 3468026
    and session_start >= CURRENT_DATE - INTERVAL '1 DAY'
  group by 1
),

thirty_day_ispot as (
  select 
    current_date as create_timestamp,
    count(distinct fk_tvid) as thirty_day_ispot_detecting
  from ispot_commercials
  where session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
  group by 1
),

one_day_ispot as (
  select 
    current_date as create_timestamp,
    count(distinct fk_tvid) as one_day_ispot_detecting
  from ispot_commercials
  where session_start >= CURRENT_DATE - INTERVAL '1 DAYS'
  group by 1
)

select a.create_timestamp, 
  a.one_year_active, 
  b.thirty_day_reporting,
  c.thirty_day_detecting,
  d.one_day_detecting,
  e.thirty_day_ispot_detecting,
  f.one_day_ispot_detecting
from one_year_opted_in a
inner join thirty_day_reporting b
  on a.create_timestamp = b.create_timestamp
inner join thirty_day_detecting c
  on a.create_timestamp = c.create_timestamp
inner join one_day_detecting d
  on a.create_timestamp = d.create_timestamp
inner join thirty_day_ispot e
  on a.create_timestamp = e.create_timestamp
inner join one_day_ispot f
  on a.create_timestamp = f.create_timestamp

-- COMMAND ----------


INSERT INTO dev.mohit_gangwani.metrics_population_report
with viewing_sessions as (
	SELECT fk_tvid, fk_content_id, session_start
	FROM detection.viewing_content_firehose
	WHERE fk_tvid IN (SELECT DISTINCT tvid FROM detection.tv_zoo_latest_daily WHERE zoo = 'control-zoo-dtsprod.tvinteractive.tv')
	AND session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
),
ispot_commercials as (
	SELECT vc.fk_tvid, session_start
	FROM detection.viewing_commercials_firehose AS vc
	INNER JOIN detection.tv_zoo_latest_daily AS zoo
	ON vc.fk_tvid = zoo.tvid
	LEFT JOIN detection.commercial_id_external_firehose AS cie
	ON vc.fk_commercial_id = cie.fk_commercial_id
	WHERE cie.fk_client_id = 8
	and vc.session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
	and zoo.zoo_id = 17
),

one_year_opted_in as (
  select '1 year active (opted in)' as category,
    current_date as create_timestamp,
    count(distinct a.fk_tvid) as one_year_active
  from detection.tv_populations a
  inner join detection.populations b
    on a.fk_population_id = b.population_id
  inner join detection.tv_activity_latest c
    on a.fk_tvid = c.tvid
  where b.population_name = 'opted_in'
    and c.session_start >= CURRENT_DATE - INTERVAL '365 DAYS'
),

thirty_day_reporting as (
  select
    current_date as create_timestamp,
    count(distinct fk_tvid) as thirty_day_reporting
    from viewing_sessions
),

thirty_day_detecting as (
  select 
    current_date as create_timestamp,
    count(distinct fk_tvid) as thirty_day_detecting
  from viewing_sessions
  where fk_content_id != 3468026 --3468026 is null detection
),

one_day_detecting as (
  select 
    current_date as create_timestamp,
    count(distinct fk_tvid) as one_day_detecting
  from viewing_sessions
  where fk_content_id != 3468026
    and session_start >= CURRENT_DATE - INTERVAL '1 DAY'
  group by 1
),

thirty_day_ispot as (
  select 
    current_date as create_timestamp,
    count(distinct fk_tvid) as thirty_day_ispot_detecting
  from ispot_commercials
  where session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
  group by 1
),

one_day_ispot as (
  select 
    current_date as create_timestamp,
    count(distinct fk_tvid) as one_day_ispot_detecting
  from ispot_commercials
  where session_start >= CURRENT_DATE - INTERVAL '1 DAYS'
  group by 1
)

select a.create_timestamp, 
  a.one_year_active, 
  b.thirty_day_reporting,
  c.thirty_day_detecting,
  d.one_day_detecting,
  e.thirty_day_ispot_detecting,
  f.one_day_ispot_detecting
from one_year_opted_in a
inner join thirty_day_reporting b
  on a.create_timestamp = b.create_timestamp
inner join thirty_day_detecting c
  on a.create_timestamp = c.create_timestamp
inner join one_day_detecting d
  on a.create_timestamp = d.create_timestamp
inner join thirty_day_ispot e
  on a.create_timestamp = e.create_timestamp
inner join one_day_ispot f
  on a.create_timestamp = f.create_timestamp
