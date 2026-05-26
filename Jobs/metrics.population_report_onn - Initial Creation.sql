-- Databricks notebook source
WITH tvst AS (
    SELECT tv.token
    FROM prod.detection.tv_settings tvst
    JOIN tvs tv
      ON tv.tvid = tvst.fk_tvid
    JOIN prod.detection.settings st
      ON st.settings_id = tvst.fk_settings_id
    WHERE st.disabled = 0
      AND st.points_allowed = 1
      AND st.country_name = 'USA'
      AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    GROUP BY 1
  )
  , tos AS (
    SELECT tv.token
    FROM prod.detection.tv_terms_of_service tos
    JOIN tvs tv
      ON tv.tvid = tos.fk_tvid
    WHERE tos.tos_version >= 514
      AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    GROUP BY 1
  )
  , ta AS (
    SELECT tv.token
    FROM prod.detection.tv_activity ta
    JOIN tvs tv
      ON tv.tvid = ta.fk_tvid
    WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
      AND ta.session_start < CURRENT_DATE
    GROUP BY 1
  )
  select '1 year active (opted in)' as category
  , current_date as create_timestamp
  , count(distinct ta.token) as one_year_active
  FROM ta
  JOIN tvst ON ta.token = tvst.token
  JOIN tos ON ta.token = tos.token
  GROUP BY 1

-- COMMAND ----------

DROP TABLE dev.mohit_gangwani.temp_70_day_viewing_sessions;
CREATE TABLE dev.mohit_gangwani.temp_70_day_viewing_sessions AS
WITH tvs AS (
  SELECT tvid, token
  FROM prod.detection.tv
  WHERE UPPER(tv.oem) = 'ONN'
)
SELECT token, fk_content_id, session_start
FROM prod.detection_onn.viewing_content_firehose vc
JOIN tvs ON tvs.tvid = vc.fk_tvid
WHERE fk_tvid IN (SELECT DISTINCT tvid FROM prod.detection.tv_zoo_latest_daily WHERE zoo = 'control-zoo-dtsprod.tvinteractive.tv')
AND session_start >= CURRENT_DATE - INTERVAL '62 DAYS'
AND session_start < CURRENT_DATE
GROUP BY ALL;

-- COMMAND ----------

DROP TABLE dev.mohit_gangwani.temp_70_day_ispot;
CREATE TABLE dev.mohit_gangwani.temp_70_day_ispot AS
WITH tvs AS (
  SELECT tvid, token
  FROM prod.detection.tv
  WHERE UPPER(tv.oem) = 'ONN'
)
SELECT token, session_start
FROM prod.detection_onn.viewing_commercials_firehose AS vc
JOIN tvs ON tvs.tvid = vc.fk_tvid
INNER JOIN prod.detection.tv_zoo_latest_daily AS zoo
ON vc.fk_tvid = zoo.tvid
LEFT JOIN prod.detection.commercial_id_external_firehose AS cie
ON vc.fk_commercial_id = cie.fk_commercial_id
WHERE cie.fk_client_id = 8
and vc.session_start >= CURRENT_DATE - INTERVAL '62 DAYS'
AND vc.session_start < CURRENT_DATE
and zoo.zoo_id = 17;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_365_day_opted_in_active;
CREATE TABLE dev.mohit_gangwani.temp_365_day_opted_in_active AS
WITH tvs AS (
  SELECT tvid, token
  FROM prod.detection.tv
  WHERE UPPER(tv.oem) = 'ONN'
)
, dates AS (
  SELECT date_start AS day, date_start - INTERVAL '365' DAY AS day_minus_365
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, all_tvs AS (
  SELECT ta.token, DATE(ta.session_start) AS active_day
  FROM dev.mohit_gangwani.temp_tv_activity_for_onn_db_one_year ta
  JOIN dev.mohit_gangwani.temp_tv_settings_for_onn_db_one_year tvst
    ON ta.token = tvst.token
   AND ta.session_start >= tvst.create_timestamp
   AND ta.session_start < tvst.next_create_timestamp
  JOIN dev.mohit_gangwani.temp_tos_for_onn_db_one_year tos
    ON ta.token = tos.token
   AND ta.session_start >= tos.create_timestamp
   AND ta.session_start < tos.next_create_timestamp
)
SELECT td.day AS date_start
, COUNT(DISTINCT ta.token)*1.0 AS tv_count
FROM dates td
JOIN all_tvs ta
  ON ta.active_day >= day_minus_365
  AND ta.active_day < td.day
GROUP BY 1;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_thirty_day_reporting;
CREATE TABLE dev.mohit_gangwani.temp_thirty_day_reporting AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '30' DAY AS day_minus_30
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, agg AS (
  SELECT DATE(session_start) AS active_day, token
  FROM dev.mohit_gangwani.temp_70_day_viewing_sessions
  GROUP BY ALL
)
SELECT td.day AS date_start
, COUNT(DISTINCT a.token)*1.0 AS tv_count
FROM dates td
JOIN agg a
  ON a.active_day >= day_minus_30
  AND a.active_day < td.day
GROUP BY 1;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_thirty_day_detecting;
CREATE TABLE dev.mohit_gangwani.temp_thirty_day_detecting AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '30' DAY AS day_minus_30
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, agg AS (
  SELECT DATE(session_start) AS active_day, token
  FROM dev.mohit_gangwani.temp_70_day_viewing_sessions
  WHERE NOT(fk_content_id <=> 3468026)
  GROUP BY ALL
)
SELECT td.day AS date_start
, COUNT(DISTINCT a.token)*1.0 AS tv_count
FROM dates td
JOIN agg a
  ON a.active_day >= day_minus_30
  AND a.active_day < td.day
GROUP BY 1;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_day_detecting;
CREATE TABLE dev.mohit_gangwani.temp_one_day_detecting AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '365' DAY AS day_minus_365
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
SELECT DATE(session_start) AS create_timestamp, COUNT(DISTINCT token) AS tv_count
FROM dev.mohit_gangwani.temp_70_day_viewing_sessions
WHERE NOT(fk_content_id <=> 3468026)
GROUP BY 1;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_thirty_day_ispot;
CREATE TABLE dev.mohit_gangwani.temp_thirty_day_ispot AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '30' DAY AS day_minus_30
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, agg AS (
  SELECT DATE(session_start) AS active_day, token
  FROM dev.mohit_gangwani.temp_70_day_ispot
  GROUP BY ALL
)
SELECT td.day AS date_start
, COUNT(DISTINCT a.token)*1.0 AS tv_count
FROM dates td
JOIN agg a
  ON a.active_day >= day_minus_30
  AND a.active_day < td.day
GROUP BY 1;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_day_ispot;
CREATE TABLE dev.mohit_gangwani.temp_one_day_ispot AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '365' DAY AS day_minus_365
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
SELECT DATE(session_start) AS create_timestamp, COUNT(DISTINCT token) AS tv_count
FROM dev.mohit_gangwani.temp_70_day_ispot
GROUP BY 1;

-- COMMAND ----------

-- DROP TABLE IF EXISTS dev.mohit_gangwani.metrics_population_report_onn;
-- CREATE TABLE dev.mohit_gangwani.metrics_population_report_onn AS
-- WITH tvs AS (
--   SELECT tvid, token
--   FROM prod.detection.tv
--   WHERE UPPER(tv.oem) = 'ONN'
-- )
-- , viewing_sessions as (
-- 	SELECT fk_tvid, fk_content_id, session_start
-- 	FROM prod.detection_onn.viewing_content_firehose vc
--   JOIN tvs ON tvs.tvid = vc.fk_tvid
-- 	WHERE fk_tvid IN (SELECT DISTINCT tvid FROM detection.tv_zoo_latest_daily WHERE zoo = 'control-zoo-dtsprod.tvinteractive.tv')
-- 	AND session_start >= CURRENT_DATE - INTERVAL '61 DAYS'
--   AND session_start < CURRENT_DATE
--   GROUP BY ALL
-- ),
-- ispot_commercials as (
-- 	SELECT vc.fk_tvid, session_start
-- 	FROM prod.detection_onn.viewing_commercials_firehose AS vc
--   JOIN tvs ON tvs.tvid = vc.fk_tvid
-- 	INNER JOIN prod.detection.tv_zoo_latest_daily AS zoo
-- 	ON vc.fk_tvid = zoo.tvid
-- 	LEFT JOIN prod.detection.commercial_id_external_firehose AS cie
-- 	ON vc.fk_commercial_id = cie.fk_commercial_id
-- 	WHERE cie.fk_client_id = 8
-- 	and vc.session_start >= CURRENT_DATE - INTERVAL '61 DAYS'
--   AND vc.session_start < CURRENT_DATE
-- 	and zoo.zoo_id = 17
-- ),

-- one_year_opted_in as (
--   select '1 year active (opted in)' as category,
--     current_date as create_timestamp,
--     count(distinct a.fk_tvid) as one_year_active
--   from prod.detection.tv_populations a
--   JOIN tvs ON tvs.tvid = a.fk_tvid
--   inner join prod.detection.populations b
--     on a.fk_population_id = b.population_id
--   inner join detection.tv_activity_latest c
--     on a.fk_tvid = c.tvid
--   where b.population_name = 'opted_in'
--     and c.session_start >= CURRENT_DATE - INTERVAL '365 DAYS'
-- ),

-- thirty_day_reporting as (
--   select
--     current_date as create_timestamp,
--     count(distinct fk_tvid) as thirty_day_reporting
--     from viewing_sessions
-- ),

-- thirty_day_detecting as (
--   select 
--     current_date as create_timestamp,
--     count(distinct fk_tvid) as thirty_day_detecting
--   from viewing_sessions
--   where fk_content_id != 3468026 --3468026 is null detection
-- ),

-- one_day_detecting as (
--   select 
--     current_date as create_timestamp,
--     count(distinct fk_tvid) as one_day_detecting
--   from viewing_sessions
--   where fk_content_id != 3468026
--     and session_start >= CURRENT_DATE - INTERVAL '1 DAY'
--   group by 1
-- ),

-- thirty_day_ispot as (
--   select 
--     current_date as create_timestamp,
--     count(distinct fk_tvid) as thirty_day_ispot_detecting
--   from ispot_commercials
--   where session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
--   group by 1
-- ),

-- one_day_ispot as (
--   select 
--     current_date as create_timestamp,
--     count(distinct fk_tvid) as one_day_ispot_detecting
--   from ispot_commercials
--   where session_start >= CURRENT_DATE - INTERVAL '1 DAYS'
--   group by 1
-- )
-- SELECT * FROM dev.mohit_gangwani.metrics_population_report_onn
-- UNION
INSERT INTO dev.mohit_gangwani.metrics_population_report_onn
SELECT a.date_start + INTERVAL '1 DAY' AS create_timestamp, 
  a.tv_count AS one_year_active, 
  b.tv_count AS thirty_day_reporting,
  c.tv_count AS thirty_day_detecting,
  d.tv_count AS one_day_detecting,
  e.tv_count AS thirty_day_ispot_detecting,
  f.tv_count AS one_day_ispot_detecting
FROM dev.mohit_gangwani.temp_365_day_opted_in_active a
join dev.mohit_gangwani.temp_thirty_day_reporting b
  on a.date_start = b.date_start
join dev.mohit_gangwani.temp_thirty_day_detecting c
  on a.date_start = c.date_start
join dev.mohit_gangwani.temp_one_day_detecting d
  on a.date_start = d.create_timestamp
join dev.mohit_gangwani.temp_thirty_day_ispot e
  on a.date_start = e.date_start
join dev.mohit_gangwani.temp_one_day_ispot f
  on a.date_start = f.create_timestamp
ORDER BY 1

-- COMMAND ----------

DELETE FROM dev.mohit_gangwani.metrics_population_report_onn WHERE create_timestamp = '2025-11-05T00:00:00'

-- COMMAND ----------

SELECT * FROM dev.mohit_gangwani.metrics_population_report_onn
ORDER BY 1

-- COMMAND ----------


-- INSERT INTO dev.mohit_gangwani.metrics_population_report_onn
WITH tvs AS (
  SELECT tvid, token
  FROM prod.detection.tv
  WHERE oem = 'ONN'
)
, viewing_sessions as (
	SELECT tvs.token, vc.fk_content_id, DATE(vc.session_start) AS session_start
	FROM prod.detection_onn.viewing_content_firehose vc
  JOIN tvs ON tvid = vc.fk_tvid
	WHERE fk_tvid IN (SELECT DISTINCT tvid FROM prod.detection.tv_zoo_latest_daily WHERE zoo = 'control-zoo-dtsprod.tvinteractive.tv')
	  AND session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
  GROUP BY ALL
),
ispot_commercials as (
	SELECT tvs.token, DATE(session_start) AS session_start
	FROM prod.detection_onn.viewing_commercials_firehose AS vc
  JOIN tvs ON tvid = vc.fk_tvid
	INNER JOIN prod.detection.tv_zoo_latest_daily AS zoo
	  ON vc.fk_tvid = zoo.tvid
	LEFT JOIN prod.detection.commercial_id_external_firehose AS cie
	  ON vc.fk_commercial_id = cie.fk_commercial_id
	WHERE cie.fk_client_id = 8
	  and vc.session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
	  and zoo.zoo_id = 17
  GROUP BY ALL
),
one_year_opted_in as (
  select '1 year active (opted in)' as category
  , current_date as create_timestamp
  , count(distinct tvs.token) as one_year_active
  from prod.detection.tv_populations a
  JOIN tvs ON tvs.tvid = a.fk_tvid
  inner join prod.detection.populations b
    on a.fk_population_id = b.population_id
  inner join prod.detection.tv_activity c
    on a.fk_tvid = c.fk_tvid
  where b.population_name = 'opted_in'
    and c.session_start >= CURRENT_DATE - INTERVAL '365 DAYS'
),
thirty_day_reporting as (
  select
    current_date as create_timestamp,
    count(distinct token) as thirty_day_reporting
    from viewing_sessions
),
thirty_day_detecting as (
  select 
    current_date as create_timestamp,
    count(distinct token) as thirty_day_detecting
  from viewing_sessions
  where fk_content_id != 3468026 --3468026 is null detection
),
one_day_detecting as (
  select 
    current_date as create_timestamp,
    count(distinct token) as one_day_detecting
  from viewing_sessions
  where fk_content_id != 3468026
    and session_start >= CURRENT_DATE - INTERVAL '1 DAY'
  group by 1
),
thirty_day_ispot as (
  select 
    current_date as create_timestamp,
    count(distinct token) as thirty_day_ispot_detecting
  from ispot_commercials
  group by 1
),
one_day_ispot as (
  select 
    current_date as create_timestamp,
    count(distinct token) as one_day_ispot_detecting
  from ispot_commercials
  where session_start >= CURRENT_DATE - INTERVAL '1 DAYS'
  group by 1
)
select a.create_timestamp
, a.one_year_active
, b.thirty_day_reporting
, c.thirty_day_detecting
, d.one_day_detecting
, e.thirty_day_ispot_detecting
, f.one_day_ispot_detecting
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


