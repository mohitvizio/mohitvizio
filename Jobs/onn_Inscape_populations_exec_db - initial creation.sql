-- Databricks notebook source
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_onn_tvs;
CREATE TABLE dev.mohit_gangwani.temp_onn_tvs AS
SELECT tvid, token
FROM prod.detection.tv
WHERE UPPER(oem) = 'ONN';

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_tv_activity_for_onn_db_one_year;
CREATE TABLE dev.mohit_gangwani.temp_tv_activity_for_onn_db_one_year AS
SELECT ta.session_start, tv.token
FROM prod.detection.tv_activity ta
JOIN dev.mohit_gangwani.temp_onn_tvs tv
  ON tv.tvid = ta.fk_tvid
WHERE ta.session_end >= CURRENT_DATE - 400
GROUP BY ALL;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_us_geo_for_onn_db_one_year;
CREATE TABLE dev.mohit_gangwani.temp_us_geo_for_onn_db_one_year AS
SELECT tv.token, geo.create_timestamp, geo.next_create_timestamp
FROM prod.detection.tv_geolocation geo
JOIN dev.mohit_gangwani.temp_onn_tvs tv
  ON tv.tvid = geo.fk_tvid
JOIN prod.detection.location loc
  ON loc.location_id = geo.fk_location_id
WHERE loc.country_code = 'US'
  AND geo.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '400 DAYS'))
GROUP BY ALL;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_tv_settings_for_onn_db_one_year;
CREATE TABLE dev.mohit_gangwani.temp_tv_settings_for_onn_db_one_year AS
SELECT tv.token, tvst.create_timestamp, tvst.next_create_timestamp
FROM prod.detection.tv_settings tvst
JOIN dev.mohit_gangwani.temp_onn_tvs tv
  ON tv.tvid = tvst.fk_tvid
JOIN prod.detection.settings st
  ON st.settings_id = tvst.fk_settings_id
WHERE st.disabled = 0
  AND st.points_allowed = 1
  AND st.country_name = 'USA'
  AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '400 DAYS'))
GROUP BY ALL;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_tos_for_onn_db_one_year;
CREATE TABLE dev.mohit_gangwani.temp_tos_for_onn_db_one_year AS
SELECT tv.token, tos.create_timestamp, tos.next_create_timestamp
FROM prod.detection.tv_terms_of_service tos
JOIN dev.mohit_gangwani.temp_onn_tvs tv
  ON tv.tvid = tos.fk_tvid
WHERE tos.tos_version >= 514
  AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '400 DAYS'))
GROUP BY ALL;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_tvzoo_for_onn_db_one_year;
CREATE TABLE dev.mohit_gangwani.temp_tvzoo_for_onn_db_one_year AS
SELECT tv.token, tz.create_timestamp, tz.next_create_timestamp
FROM prod.detection.tv_zoo tz
JOIN dev.mohit_gangwani.temp_onn_tvs tv
  ON tv.tvid = tz.fk_tvid
WHERE tz.fk_zoo_id = 17
  AND tz.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '400 DAYS'))
GROUP BY ALL;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_all_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_year_all_active_onn as
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '365' DAY AS day_minus_365
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, agg AS (
  SELECT DATE(ta.session_start) AS active_day, ta.token
  FROM dev.mohit_gangwani.temp_tv_activity_for_onn_db_one_year ta
  -- JOIN dev.mohit_gangwani.temp_us_geo_for_onn_db_one_year geo
  --   ON geo.token = ta.token
  --  AND geo.next_create_timestamp >= ta.session_start
  --  AND geo.create_timestamp < ta.session_start
  GROUP BY ALL
)
SELECT td.day AS date_start
, COUNT(DISTINCT a.token)*1.0 AS tv_count
FROM dates td
JOIN agg a
  ON a.active_day >= day_minus_365
  AND a.active_day < td.day
GROUP BY 1

-- COMMAND ----------

SELECT * FROM dev.mohit_gangwani.temp_one_year_all_active_onn;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_optedin_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_month_optedin_active_onn AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '30' DAY AS day_minus_30
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, agg AS (
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
  WHERE ta.session_start >= CURRENT_DATE - INTERVAL '62 DAYS'
    AND ta.session_start < CURRENT_DATE
  GROUP BY ALL
)
SELECT td.day AS date_start
, COUNT(DISTINCT a.token)*1.0 AS tv_count
FROM dates td
JOIN agg a
  ON a.active_day >= day_minus_30
  AND a.active_day < td.day
GROUP BY 1
;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn;
CREATE TABLE dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn AS
SELECT DATE(vc.session_start) AS date_start
, COUNT(DISTINCT tv.token)*1.0 AS tv_count
FROM prod.detection_onn.viewing_content_firehose vc
JOIN prod.detection.location loc
  ON vc.fk_location_id = loc.location_id
JOIN dev.mohit_gangwani.temp_onn_tvs tv
  ON tv.tvid = vc.fk_tvid
WHERE vc.session_start >= '2025-11-04T00:00:00'
  AND vc.session_start < CURRENT_DATE
  AND vc.fk_zoo_id = 17
  AND vc.session_duration > 0
  AND vc.fk_content_id != 3468026
  AND loc.country_code = 'US'
GROUP BY 1
;

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '30' DAY AS day_minus_30
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, agg AS (
  SELECT DATE(vc.session_start) AS active_day
  , tv.token
  FROM prod.detection_onn.viewing_content_firehose vc
  JOIN dev.mohit_gangwani.temp_onn_tvs tv
    ON tv.tvid = vc.fk_tvid
  JOIN prod.detection.location loc
    ON loc.location_id = vc.fk_location_id
  WHERE vc.session_start >= CURRENT_DATE - INTERVAL '62 DAYS'
    AND vc.session_start < CURRENT_DATE
    AND vc.fk_zoo_id = 17
    AND vc.session_duration > 0
    AND vc.fk_content_id != 3468026
    AND loc.country_code = 'US'
  GROUP BY ALL
)
SELECT td.day AS date_start
, COUNT(DISTINCT a.token)*1.0 AS tv_count
FROM dates td
JOIN agg a
  ON a.active_day >= day_minus_30
 AND a.active_day < td.day
GROUP BY 1

-- COMMAND ----------

SELECT * FROM dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_month_production_optedin_active_onn AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '30' DAY AS day_minus_30
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, agg AS (
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
  JOIN dev.mohit_gangwani.temp_tvzoo_for_onn_db_one_year tz
    ON ta.token = tz.token
   AND ta.session_start >= tz.create_timestamp
   AND ta.session_start < tz.next_create_timestamp
  WHERE ta.session_start >= CURRENT_DATE - INTERVAL '62 DAYS'
    AND ta.session_start < CURRENT_DATE
  GROUP BY ALL
)
SELECT td.day AS date_start
, COUNT(DISTINCT a.token)*1.0 AS tv_count
FROM dates td
JOIN agg a
  ON a.active_day >= day_minus_30
 AND a.active_day < td.day
GROUP BY 1
;

-- COMMAND ----------

SELECT * FROM dev.mohit_gangwani.temp_one_month_production_optedin_active_onn

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_optedin_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_year_optedin_active_onn AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '365' DAY AS day_minus_365
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, agg AS (
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
  GROUP BY ALL
)
SELECT td.day AS date_start
, COUNT(DISTINCT a.token)*1.0 AS tv_count
FROM dates td
JOIN agg a
  ON a.active_day >= day_minus_365
 AND a.active_day < td.day
GROUP BY 1
;

-- COMMAND ----------



-- COMMAND ----------

SELECT * FROM dev.mohit_gangwani.exec_dashboard_onn

-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_production_optedin_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_year_production_optedin_active_onn AS
WITH dates AS (
  SELECT date_start AS day, date_start - INTERVAL '365' DAY AS day_minus_365
  FROM prod.detection.time_date
  WHERE date_start >= '2025-11-04T00:00:00'
    AND date_start < CURRENT_DATE
)
, agg AS (
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
  JOIN dev.mohit_gangwani.temp_tvzoo_for_onn_db_one_year tz
    ON ta.token = tz.token
   AND ta.session_start >= tz.create_timestamp
   AND ta.session_start < tz.next_create_timestamp
  GROUP BY ALL
)
SELECT td.day AS date_start
, COUNT(DISTINCT a.token)*1.0 AS tv_count
FROM dates td
JOIN agg a
  ON a.active_day >= day_minus_365
 AND a.active_day < td.day
GROUP BY 1
;

-- COMMAND ----------

-- one year all activities
-- CREATE TABLE dev.mohit_gangwani.temp_onn_tvs AS
-- SELECT tvid, token
-- FROM prod.detection.tv
-- WHERE UPPER(oem) = 'ONN';

-- CREATE TABLE dev.mohit_gangwani.temp_one_year_all_active_onn as
--   WITH geo AS (
--     SELECT token
--     FROM prod.detection.tv_geolocation geo
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = geo.fk_tvid
--     JOIN prod.detection.location loc
--       ON loc.location_id = geo.fk_location_id
--     WHERE loc.country_code = 'US'
--       AND geo.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
--     GROUP BY 1
--   )
--   , ta AS (
--     SELECT tv.token
--     FROM prod.detection.tv_activity ta
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = ta.fk_tvid
--     WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
--       AND ta.session_start < CURRENT_DATE
--     GROUP BY 1
--   )
--   SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
--   , COUNT(DISTINCT ta.token)*1.0 AS tv_count
--   FROM ta
--   JOIN geo ON ta.token = geo.token
--   GROUP BY 1
-- ;

-- CREATE TABLE dev.mohit_gangwani.temp_one_month_optedin_active_onn AS
--   WITH tvst AS (
--     SELECT tv.token
--     FROM prod.detection.tv_settings tvst
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tvst.fk_tvid
--     JOIN prod.detection.settings st
--       ON st.settings_id = tvst.fk_settings_id
--     WHERE st.disabled = 0
--       AND st.points_allowed = 1
--       AND st.country_name = 'USA'
--       AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
--     GROUP BY 1
--   )
--   , tos AS (
--     SELECT tv.token
--     FROM prod.detection.tv_terms_of_service tos
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tos.fk_tvid
--     WHERE tos.tos_version >= 514
--       AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
--     GROUP BY 1
--   )
--   , ta AS (
--     SELECT tv.token
--     FROM prod.detection.tv_activity ta
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = ta.fk_tvid
--     WHERE ta.session_end >= CURRENT_DATE - INTERVAL '31 DAYS'
--       AND ta.session_start < CURRENT_DATE
--     GROUP BY 1
--   )
--   SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
--   , COUNT(DISTINCT ta.token)*1.0 AS tv_count
--   FROM ta
--   JOIN tvst ON ta.token = tvst.token
--   JOIN tos ON ta.token = tos.token
--   GROUP BY 1
-- ;

-- CREATE TABLE dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn AS
--   SELECT DATE_TRUNC('DAY', CURRENT_DATE-1) AS date_start
--   , COUNT(DISTINCT tv.token)*1.0 AS tv_count
--   FROM prod.detection_onn.viewing_content_firehose vc
--   JOIN prod.detection.location loc
--     ON vc.fk_location_id = loc.location_id
--   JOIN dev.mohit_gangwani.temp_onn_tvs tv
--     ON tv.tvid = vc.fk_tvid
--   WHERE vc.session_start >= CURRENT_DATE - 1
--     AND vc.session_start < CURRENT_DATE
--     AND vc.fk_zoo_id = 17
--     AND vc.session_duration > 0
--     AND vc.fk_content_id != 3468026
--     AND loc.country_code = 'US'
--   GROUP BY 1
-- ;

-- CREATE TABLE dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn AS
--   SELECT DATE_TRUNC('DAY', CURRENT_DATE-1) AS date_start
--   , COUNT(DISTINCT tv.token)*1.0 AS tv_count
--   FROM prod.detection_onn.viewing_content_firehose vc
--   JOIN dev.mohit_gangwani.temp_onn_tvs tv
--     ON tv.tvid = vc.fk_tvid
--   JOIN prod.detection.location loc
--     ON loc.location_id = vc.fk_location_id
--   WHERE vc.session_start >= CURRENT_DATE - INTERVAL '31 DAYS'
--     AND vc.session_start < CURRENT_DATE
--     AND vc.fk_zoo_id = 17
--     AND vc.session_duration > 0
--     AND vc.fk_content_id != 3468026
--     AND loc.country_code = 'US'
--   GROUP BY 1
-- ;

-- CREATE TABLE dev.mohit_gangwani.temp_one_month_production_optedin_active_onn AS
--   WITH tvst AS (
--     SELECT tv.token
--     FROM prod.detection.tv_settings tvst
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tvst.fk_tvid
--     JOIN prod.detection.settings st
--       ON st.settings_id = tvst.fk_settings_id
--     WHERE st.disabled = 0
--       AND st.points_allowed = 1
--       AND st.country_name = 'USA'
--       AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
--     GROUP BY 1
--   )
--   , tos AS (
--     SELECT tv.token
--     FROM prod.detection.tv_terms_of_service tos
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tos.fk_tvid
--     WHERE tos.tos_version >= 514
--       AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
--     GROUP BY 1
--   )
--   , tz AS (
--     SELECT tv.token
--     FROM prod.detection.tv_zoo tz
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tz.fk_tvid
--     WHERE tz.fk_zoo_id = 17
--       AND tz.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
--     GROUP BY 1
--   )
--   , ta AS (
--     SELECT tv.token
--     FROM prod.detection.tv_activity ta
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = ta.fk_tvid
--     WHERE ta.session_end >= CURRENT_DATE - INTERVAL '31 DAYS'
--       AND ta.session_start < CURRENT_DATE
--     GROUP BY 1
--   )
--   SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
--   , COUNT(DISTINCT ta.token)*1.0 AS tv_count
--   FROM ta
--   JOIN tvst ON ta.token = tvst.token
--   JOIN tos ON ta.token = tos.token
--   JOIN tz ON ta.token = tz.token
--   GROUP BY 1
-- ;

-- CREATE TABLE dev.mohit_gangwani.temp_one_year_optedin_active_onn AS
--   WITH tvst AS (
--     SELECT tv.token
--     FROM prod.detection.tv_settings tvst
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tvst.fk_tvid
--     JOIN prod.detection.settings st
--       ON st.settings_id = tvst.fk_settings_id
--     WHERE st.disabled = 0
--       AND st.points_allowed = 1
--       AND st.country_name = 'USA'
--       AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
--     GROUP BY 1
--   )
--   , tos AS (
--     SELECT tv.token
--     FROM prod.detection.tv_terms_of_service tos
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tos.fk_tvid
--     WHERE tos.tos_version >= 514
--       AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
--     GROUP BY 1
--   )
--   , ta AS (
--     SELECT tv.token
--     FROM prod.detection.tv_activity ta
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = ta.fk_tvid
--     WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
--       AND ta.session_start < CURRENT_DATE
--     GROUP BY 1
--   )
--   SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
--   , COUNT(DISTINCT ta.token)*1.0 AS tv_count
--   FROM ta
--   JOIN tvst ON ta.token = tvst.token
--   JOIN tos ON ta.token = tos.token
--   GROUP BY 1
-- ;

-- CREATE TABLE dev.mohit_gangwani.temp_one_year_production_optedin_active_onn AS
--   WITH tvst AS (
--     SELECT tv.token
--     FROM prod.detection.tv_settings tvst
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tvst.fk_tvid
--     JOIN prod.detection.settings st
--       ON st.settings_id = tvst.fk_settings_id
--     WHERE st.disabled = 0
--       AND st.points_allowed = 1
--       AND st.country_name = 'USA'
--       AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
--     GROUP BY 1
--   )
--   , tos AS (
--     SELECT tv.token
--     FROM prod.detection.tv_terms_of_service tos
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tos.fk_tvid
--     WHERE tos.tos_version >= 514
--       AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
--     GROUP BY 1
--   )
--   , tz AS (
--     SELECT tv.token
--     FROM prod.detection.tv_zoo tz
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = tz.fk_tvid
--     WHERE tz.fk_zoo_id = 17
--       AND tz.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
--     GROUP BY 1
--   )
--   , ta AS (
--     SELECT tv.token
--     FROM prod.detection.tv_activity ta
--     JOIN dev.mohit_gangwani.temp_onn_tvs tv
--       ON tv.tvid = ta.fk_tvid
--     WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
--       AND ta.session_start < CURRENT_DATE
--     GROUP BY 1
--   )
--   SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
--   , COUNT(DISTINCT ta.token)*1.0 AS tv_count
--   FROM ta
--   JOIN tvst ON ta.token = tvst.token
--   JOIN tos ON ta.token = tos.token
--   JOIN tz ON ta.token = tz.token
--   GROUP BY 1
-- ;

INSERT INTO dev.mohit_gangwani.exec_dashboard_onn
SELECT b1.date_start
    , b1.tv_count as one_year_all_active_tv_count
    , b6.tv_count as one_year_optedin_active_tvcount
    , b7.tv_count as one_year_production_optedin_active_tvcount
    , b3.tv_count as one_month_opredin_active_tvcount
    , b4.tv_count as one_month_production_optedin_active_tvcount
    , b5.tv_count as one_month_production_optedin_detecting_tvcount
    , b2.tv_count as oneday_production_optedin_detecting_tvcount
FROM dev.mohit_gangwani.temp_one_year_all_active_onn AS b1
JOIN dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn as b2
 on b1.date_start = b2.date_start
JOIN dev.mohit_gangwani.temp_one_month_optedin_active_onn as b3
 on b1.date_start = b3.date_start
JOIN dev.mohit_gangwani.temp_one_month_production_optedin_active_onn as b4
 on b1.date_start = b4.date_start
JOIN dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn as b5
 on b1.date_start = b5.date_start
JOIN dev.mohit_gangwani.temp_one_year_optedin_active_onn as b6
 on b1.date_start = b6.date_start
JOIN dev.mohit_gangwani.temp_one_year_production_optedin_active_onn as b7
 on b1.date_start = b7.date_start
;

-- this snipper drops all tables
-- DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_all_active_onn;
-- DROP TABLE IF EXISTS dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn;
-- DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_optedin_active_onn;
-- DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_active_onn;
-- DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn;
-- DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_optedin_active_onn;
-- DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_production_optedin_active_onn;

-- COMMAND ----------

CREATE TABLE dev.mohit_gangwani.exec_dashboard_onn AS
SELECT b1.date_start
    , b1.tv_count as one_year_all_active_tv_count
    , b6.tv_count as one_year_optedin_active_tvcount
    , b7.tv_count as one_year_production_optedin_active_tvcount
    , b3.tv_count as one_month_opredin_active_tvcount
    , b4.tv_count as one_month_production_optedin_active_tvcount
    , b5.tv_count as one_month_production_optedin_detecting_tvcount
    , b2.tv_count as oneday_production_optedin_detecting_tvcount
FROM dev.mohit_gangwani.temp_one_year_all_active_onn AS b1
JOIN dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn as b2
 on b1.date_start = b2.date_start
JOIN dev.mohit_gangwani.temp_one_month_optedin_active_onn as b3
 on b1.date_start = b3.date_start
JOIN dev.mohit_gangwani.temp_one_month_production_optedin_active_onn as b4
 on b1.date_start = b4.date_start
JOIN dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn as b5
 on b1.date_start = b5.date_start
JOIN dev.mohit_gangwani.temp_one_year_optedin_active_onn as b6
 on b1.date_start = b6.date_start
JOIN dev.mohit_gangwani.temp_one_year_production_optedin_active_onn as b7
 on b1.date_start = b7.date_start
ORDER BY 1
;

-- COMMAND ----------

SELECT * FROM dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn
ORDER BY 1

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_all_active_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_optedin_active_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_active_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_optedin_active_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_production_optedin_active_onn;

-- COMMAND ----------

merge into dev.mohit_gangwani.exec_dashboard_onn a using dev.mohit_gangwani.temp_one_year_all_active_onn b
on (a.date_start=b.date_start)
when matched then
update set a.one_year_all_active_tv_count=b.tv_count;

-- COMMAND ----------

SELECT * FROM dev.mohit_gangwani.exec_dashboard_onn
