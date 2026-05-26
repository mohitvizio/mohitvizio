-- Databricks notebook source

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_onn_tvs;
CREATE TABLE dev.mohit_gangwani.temp_onn_tvs AS
SELECT tvid, token
FROM prod.detection.tv
WHERE UPPER(oem) = 'ONN';

-- one year all activities
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_all_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_year_all_active_onn as
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM prod.detection.tv_activity ta
  JOIN dev.mohit_gangwani.temp_onn_tvs tv
    ON tv.tvid = ta.fk_tvid
  WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
   AND ta.session_start < CURRENT_DATE
  GROUP BY 1
;

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_optedin_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_month_optedin_active_onn AS
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM prod.detection.tv_activity ta
  JOIN dev.mohit_gangwani.temp_onn_tvs tv
    ON tv.tvid = ta.fk_tvid
  JOIN prod.detection.tv_terms_of_service tos
    ON tos.fk_tvid = ta.fk_tvid
   AND tos.create_timestamp <= ta.session_start
   AND tos.next_create_timestamp > ta.session_start
  JOIN prod.detection.tv_settings tvst
    ON tvst.fk_tvid = ta.fk_tvid
   AND tvst.create_timestamp <= ta.session_start
   AND tvst.next_create_timestamp > ta.session_start
  JOIN prod.detection.settings st
    ON st.settings_id = tvst.fk_settings_id
  WHERE ta.session_end >= CURRENT_DATE - INTERVAL '31 DAYS'
    AND ta.session_start < CURRENT_DATE
    AND tos.tos_version >= 514
    AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
    AND st.disabled = 0
    AND st.points_allowed = 1
    AND st.country_name = 'USA'
    AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
  GROUP BY 1
;

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn;
CREATE TABLE dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn AS
  SELECT DATE_TRUNC('DAY', CURRENT_DATE-1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM prod.detection_onn.viewing_content_firehose vc
  JOIN prod.detection.location loc
    ON vc.fk_location_id = loc.location_id
  JOIN dev.mohit_gangwani.temp_onn_tvs tv
    ON tv.tvid = vc.fk_tvid
  WHERE vc.session_start >= CURRENT_DATE - 1
    AND vc.session_start < CURRENT_DATE
    AND vc.fk_zoo_id = 17
    AND vc.session_duration > 0
    AND vc.fk_content_id != 3468026
    AND loc.country_code = 'US'
  GROUP BY 1
;

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn AS
  SELECT DATE_TRUNC('DAY', CURRENT_DATE-1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM prod.detection_onn.viewing_content_firehose vc
  JOIN dev.mohit_gangwani.temp_onn_tvs tv
    ON tv.tvid = vc.fk_tvid
  JOIN prod.detection.location loc
    ON loc.location_id = vc.fk_location_id
  WHERE vc.session_start >= CURRENT_DATE - INTERVAL '31 DAYS'
    AND vc.session_start < CURRENT_DATE
    AND vc.fk_zoo_id = 17
    AND vc.session_duration > 0
    AND vc.fk_content_id != 3468026
    AND loc.country_code = 'US'
  GROUP BY 1
;

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_month_production_optedin_active_onn AS
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM prod.detection.tv_activity ta
  JOIN dev.mohit_gangwani.temp_onn_tvs tv
    ON tv.tvid = ta.fk_tvid
  JOIN prod.detection.tv_settings tvst
    ON tvst.fk_tvid = ta.fk_tvid
   AND tvst.create_timestamp <= ta.session_start
   AND tvst.next_create_timestamp > ta.session_start
  JOIN prod.detection.settings st
    ON st.settings_id = tvst.fk_settings_id
  JOIN prod.detection.tv_terms_of_service tos
    ON tos.fk_tvid = ta.fk_tvid
   AND tos.create_timestamp <= ta.session_start
   AND tos.next_create_timestamp > ta.session_start
  JOIN prod.detection.tv_zoo tz
    ON tz.fk_tvid = ta.fk_tvid
   AND tz.create_timestamp <= ta.session_start
   AND tz.next_create_timestamp > ta.session_start
  WHERE ta.session_end >= CURRENT_DATE - INTERVAL '31 DAYS'
    AND ta.session_start < CURRENT_DATE
    AND st.disabled = 0
    AND st.points_allowed = 1
    AND st.country_name = 'USA'
    AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
    AND tz.fk_zoo_id = 17
    AND tz.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
    AND tos.tos_version >= 514
    AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '31 DAYS'))
  GROUP BY 1
;

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_optedin_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_year_optedin_active_onn AS
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM prod.detection.tv_activity ta
  JOIN dev.mohit_gangwani.temp_onn_tvs tv
    ON tv.tvid = ta.fk_tvid
  JOIN prod.detection.tv_settings tvst
    ON tvst.fk_tvid = ta.fk_tvid
   AND tvst.create_timestamp <= ta.session_start
   AND tvst.next_create_timestamp > ta.session_start
  JOIN prod.detection.settings st
    ON st.settings_id = tvst.fk_settings_id
  JOIN prod.detection.tv_terms_of_service tos
    ON tos.fk_tvid = ta.fk_tvid
   AND tos.create_timestamp <= ta.session_start
   AND tos.next_create_timestamp > ta.session_start
  WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
    AND ta.session_start < CURRENT_DATE
    AND tos.tos_version >= 514
    AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    AND st.disabled = 0
    AND st.points_allowed = 1
    AND st.country_name = 'USA'
    AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
  GROUP BY 1
;

DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_production_optedin_active_onn;
CREATE TABLE dev.mohit_gangwani.temp_one_year_production_optedin_active_onn AS
  SELECT DATE_TRUNC('DAY', CURRENT_DATE - 1) AS date_start
  , COUNT(DISTINCT tv.token)*1.0 AS tv_count
  FROM prod.detection.tv_activity ta
  JOIN dev.mohit_gangwani.temp_onn_tvs tv
    ON tv.tvid = ta.fk_tvid
  JOIN prod.detection.tv_settings tvst
    ON tvst.fk_tvid = ta.fk_tvid
   AND tvst.create_timestamp <= ta.session_start
   AND tvst.next_create_timestamp > ta.session_start
  JOIN prod.detection.settings st
    ON st.settings_id = tvst.fk_settings_id
  JOIN prod.detection.tv_terms_of_service tos
    ON tos.fk_tvid = ta.fk_tvid
   AND tos.create_timestamp <= ta.session_start
   AND tos.next_create_timestamp > ta.session_start
  JOIN prod.detection.tv_zoo tz
    ON tz.fk_tvid = ta.fk_tvid
   AND tz.create_timestamp <= ta.session_start
   AND tz.next_create_timestamp > ta.session_start
  WHERE ta.session_end >= CURRENT_DATE - INTERVAL '366 DAYS'
    AND ta.session_start < CURRENT_DATE
    AND tz.fk_zoo_id = 17
    AND tz.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    AND tos.tos_version >= 514
    AND tos.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
    AND st.disabled = 0
    AND st.points_allowed = 1
    AND st.country_name = 'USA'
    AND tvst.next_create_timestamp >=  TIMESTAMPADD(DAY, -7, DATE_TRUNC('WEEK', CURRENT_DATE - INTERVAL '366 DAYS'))
  GROUP BY 1
;

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
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_onn_tvs;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_all_active_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_1day_production_optedin_detecting_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_optedin_active_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_active_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_month_production_optedin_detecting_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_optedin_active_onn;
DROP TABLE IF EXISTS dev.mohit_gangwani.temp_one_year_production_optedin_active_onn;

-- COMMAND ----------


