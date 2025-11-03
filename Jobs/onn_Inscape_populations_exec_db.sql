-- Databricks notebook source

-- one year all activities
CREATE TABLE dev.mohit_gangwani.temp_one_year_all_active as
  WITH geo AS (
    SELECT fk_tvid
    FROM detection.tv_geolocation geo
    JOIN detection.location loc
      ON loc.location_id = geo.fk_location_id
    WHERE loc.country_code = 'US'
      AND geo.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    GROUP BY 1
  )
  , ta AS (
    SELECT ta.fk_tvid
    FROM detection.tv_activity ta
    WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
      AND ta.session_start < CURRENT_DATE
    GROUP BY 1
  )
  , tv AS (
    SELECT tvid, token
    FROM detection.tv
    WHERE tv.oem = 'VIZIO'
    GROUP BY 1, 2
  )
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM ta
  JOIN tv ON ta.fk_tvid = tv.tvid
  JOIN geo ON ta.fk_tvid = geo.fk_tvid
  GROUP BY 1
;

CREATE TABLE dev.mohit_gangwani.temp_one_month_optedin_active AS (
  WITH tvst AS (
    SELECT fk_tvid
    FROM detection.tv_settings tvst
    JOIN detection.settings st
      ON st.settings_id = tvst.fk_settings_id
    WHERE st.disabled = 0
      AND st.points_allowed = 1
      AND st.country_name = 'USA'
      AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
    GROUP BY 1
  )
  , tos AS (
    SELECT fk_tvid
    FROM detection.tv_terms_of_service
    WHERE tos_version >= 514
      AND next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
    GROUP BY 1
  )
  , ta AS (
    SELECT ta.fk_tvid
    FROM detection.tv_activity ta
    WHERE ta.session_end >= CURRENT_DATE - INTERVAL '31 DAYS'
      AND ta.session_start < CURRENT_DATE
    GROUP BY 1
  )
  , tv AS (
    SELECT tvid, token
    FROM detection.tv
    WHERE tv.oem = 'VIZIO'
    GROUP BY 1, 2
  )
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM ta
  JOIN tv ON ta.fk_tvid = tv.tvid
  JOIN tvst ON ta.fk_tvid = tvst.fk_tvid
  JOIN tos ON ta.fk_tvid = tos.fk_tvid
  GROUP BY 1
);

CREATE TABLE dev.mohit_gangwani.temp_1day_production_optedin_detecting AS (
  SELECT DATE_TRUNC('DAY', CURRENT_DATE-1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM detection.viewing_content_firehose vc
  JOIN detection.location loc ON vc.fk_location_id = loc.location_id
  JOIN detection.tv ON tv.tvid = vc.fk_tvid
  WHERE vc.session_start >= CURRENT_DATE - 1
    AND vc.session_start < CURRENT_DATE
    AND vc.fk_zoo_id = 17
    AND vc.session_duration > 0
    AND vc.fk_content_id != 3468026
    AND loc.country_code = 'US'
    AND tv.oem = 'VIZIO'
  GROUP BY 1
);

CREATE TABLE dev.mohit_gangwani.temp_one_month_production_optedin_detecting AS (
  SELECT DATE_TRUNC('DAY', CURRENT_DATE-1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM detection.viewing_content_firehose vc
  JOIN detection.location loc ON loc.location_id = vc.fk_location_id
  JOIN detection.tv ON tv.tvid = vc.fk_tvid
  WHERE vc.session_start >= CURRENT_DATE - INTERVAL '31 DAYS'
    AND vc.session_start < CURRENT_DATE
    AND vc.fk_zoo_id = 17
    AND vc.session_duration > 0
    AND vc.fk_content_id != 3468026
    AND loc.country_code = 'US'
    AND tv.oem = 'VIZIO'
  GROUP BY 1
);

CREATE TABLE dev.mohit_gangwani.temp_one_month_production_optedin_active AS (
  WITH tvst AS (
    SELECT fk_tvid
    FROM detection.tv_settings tvst
    JOIN detection.settings st
      ON st.settings_id = tvst.fk_settings_id
    WHERE st.disabled = 0
      AND st.points_allowed = 1
      AND st.country_name = 'USA'
      AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
    GROUP BY 1
  )
  , tos AS (
    SELECT fk_tvid
    FROM detection.tv_terms_of_service
    WHERE tos_version >= 514
      AND next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
    GROUP BY 1
  )
  , tz AS (
    SELECT fk_tvid
    FROM detection.tv_zoo
    WHERE fk_zoo_id = 17
      AND next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
    GROUP BY 1
  )
  , ta AS (
    SELECT ta.fk_tvid
    FROM detection.tv_activity ta
    WHERE ta.session_end >= CURRENT_DATE - INTERVAL '31 DAYS'
      AND ta.session_start < CURRENT_DATE
    GROUP BY 1
  )
  , tv AS (
    SELECT tvid, token
    FROM detection.tv
    WHERE tv.oem = 'VIZIO'
    GROUP BY 1, 2
  )
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM ta
  JOIN tv ON ta.fk_tvid = tv.tvid
  JOIN tvst ON ta.fk_tvid = tvst.fk_tvid
  JOIN tos ON ta.fk_tvid = tos.fk_tvid
  JOIN tz ON ta.fk_tvid = tz.fk_tvid
  GROUP BY 1
);

CREATE TABLE dev.mohit_gangwani.temp_one_year_optedin_active AS (
  WITH tvst AS (
    SELECT fk_tvid
    FROM detection.tv_settings tvst
    JOIN detection.settings st
      ON st.settings_id = tvst.fk_settings_id
    WHERE st.disabled = 0
      AND st.points_allowed = 1
      AND st.country_name = 'USA'
      AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    GROUP BY 1
  )
  , tos AS (
    SELECT fk_tvid
    FROM detection.tv_terms_of_service
    WHERE tos_version >= 514
      AND next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    GROUP BY 1
  )
  , ta AS (
    SELECT ta.fk_tvid
    FROM detection.tv_activity ta
    WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
      AND ta.session_start < CURRENT_DATE
    GROUP BY 1
  )
  , tv AS (
    SELECT tvid, token
    FROM detection.tv
    WHERE tv.oem = 'VIZIO'
    GROUP BY 1, 2
  )
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM ta
  JOIN tv ON ta.fk_tvid = tv.tvid
  JOIN tvst ON ta.fk_tvid = tvst.fk_tvid
  JOIN tos ON ta.fk_tvid = tos.fk_tvid
  GROUP BY 1
);

CREATE TABLE dev.mohit_gangwani.temp_one_year_production_optedin_active AS (
  WITH tvst AS (
    SELECT fk_tvid
    FROM detection.tv_settings tvst
    JOIN detection.settings st
      ON st.settings_id = tvst.fk_settings_id
    WHERE st.disabled = 0
      AND st.points_allowed = 1
      AND st.country_name = 'USA'
      AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    GROUP BY 1
  )
  , tos AS (
    SELECT fk_tvid
    FROM detection.tv_terms_of_service
    WHERE tos_version >= 514
      AND next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    GROUP BY 1
  )
  , tz AS (
    SELECT fk_tvid
    FROM detection.tv_zoo
    WHERE fk_zoo_id = 17
      AND next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    GROUP BY 1
  )
  , ta AS (
    SELECT ta.fk_tvid
    FROM detection.tv_activity ta
    WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
      AND ta.session_start < CURRENT_DATE
    GROUP BY 1
  )
  , tv AS (
    SELECT tvid, token
    FROM detection.tv
    WHERE tv.oem = 'VIZIO'
    GROUP BY 1, 2
  )
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM ta
  JOIN tv ON ta.fk_tvid = tv.tvid
  JOIN tvst ON ta.fk_tvid = tvst.fk_tvid
  JOIN tos ON ta.fk_tvid = tos.fk_tvid
  JOIN tz ON ta.fk_tvid = tz.fk_tvid
  GROUP BY 1
);

INSERT INTO dev.mohit_gangwani.exec_dashboard
SELECT b1.date_start
    , b1.tv_count as one_year_all_active_tv_count
    , b6.tv_count as one_year_optedin_active_tvcount
    , b7.tv_count as one_year_production_optedin_active_tvcount
    , b3.tv_count as one_month_opredin_active_tvcount
    , b4.tv_count as one_month_production_optedin_active_tvcount
    , b5.tv_count as one_month_production_optedin_detecting_tvcount
    , b2.tv_count as oneday_production_optedin_detecting_tvcount
FROM dev.mohit_gangwani.temp_one_year_all_active AS b1
JOIN dev.mohit_gangwani.temp_1day_production_optedin_detecting as b2
 on b1.date_start = b2.date_start
JOIN dev.mohit_gangwani.temp_one_month_optedin_active as b3
 on b1.date_start = b3.date_start
JOIN dev.mohit_gangwani.temp_one_month_production_optedin_active as b4
 on b1.date_start = b4.date_start
JOIN dev.mohit_gangwani.temp_one_month_production_optedin_detecting as b5
 on b1.date_start = b5.date_start
JOIN dev.mohit_gangwani.temp_one_year_optedin_active as b6
 on b1.date_start = b6.date_start
JOIN dev.mohit_gangwani.temp_one_year_production_optedin_active as b7
 on b1.date_start = b7.date_start
;

-- this snipper drops all tables
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_all_active;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_1day_production_optedin_detecting;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_optedin_active;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_active;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_detecting;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_optedin_active;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_production_optedin_active;
