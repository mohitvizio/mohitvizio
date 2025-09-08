-- Databricks notebook source
DROP TABLE IF EXISTS dev.mohit_gangwani.post_goldencure_viewing_content;
CREATE TABLE dev.mohit_gangwani.post_goldencure_viewing_content AS
SELECT mt.tvid
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor_name}' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
       WHEN mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|{client_name}|%'          THEN NULL
       WHEN mt.client_id_not_null != '||'        AND mt.client_id_not_null NOT LIKE '%|{client_name}|%'    THEN NULL
       WHEN '{client_name}' != 'nielsen'         AND mt.nielsen_exclusive                                   THEN NULL
       WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|{client_name}|%' THEN NULL
       ELSE CASE WHEN '{vendor_name}' = 'TMS' THEN mt.tms_episode_id ELSE mt.tivo_episode_id END
END AS episode_id

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor_name}' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
                                            ELSE COALESCE(mt.tivo_title, mt.tms_title) END
       WHEN mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|{client_name}|%'          THEN NULL
       WHEN mt.client_id_not_null != '||'        AND mt.client_id_not_null NOT LIKE '%|{client_name}|%'    THEN NULL
       WHEN '{client_name}' != 'nielsen'         AND mt.nielsen_exclusive                                  THEN NULL
       WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_title
       ELSE CASE WHEN '{vendor_name}' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title) ELSE COALESCE(mt.tivo_title, mt.tms_title) END
  END AS show_title

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor_name}' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                                   ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
       WHEN mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|{client_name}|%'          THEN NULL
       WHEN mt.client_id_not_null != '||'        AND mt.client_id_not_null NOT LIKE '%|{client_name}|%'    THEN NULL
       WHEN '{client_name}' != 'nielsen'         AND mt.nielsen_exclusive                                  THEN NULL
       WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_airdate
       ELSE CASE WHEN '{vendor_name}' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                 ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
  END AS air_date

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor_name}' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                                             ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
       WHEN mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|{client_name}|%'          THEN NULL
       WHEN mt.client_id_not_null != '||'        AND mt.client_id_not_null NOT LIKE '%|{client_name}|%'    THEN NULL
       WHEN '{client_name}' != 'nielsen'         AND mt.nielsen_exclusive                                  THEN NULL
       WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_channel_callsign
       ELSE CASE WHEN '{vendor_name}' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                 ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
  END AS channel_callsign

, CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
       WHEN mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|{client_name}|%'           THEN NULL
       WHEN mt.client_id_not_null != '||'        AND mt.client_id_not_null NOT LIKE '%|{client_name}|%'     THEN NULL
       WHEN '{client_name}' != 'nielsen'         AND mt.nielsen_exclusive                                   THEN NULL
       WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|{client_name}|%'  THEN NULL
       WHEN '{client_name}' = 'nielsen'          AND mt.tms_airdate IS NULL                                 THEN NULL
       ELSE mt.mt_start
  END AS mt_start

, session_start AS ts_start
, session_end   AS ts_end

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN '{vendor_name}' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                                   ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|{client_name}|%'          THEN NULL
       WHEN mt.client_id_not_null != '||'        AND mt.client_id_not_null NOT LIKE '%|{client_name}|%'    THEN NULL
       WHEN '{client_name}' != 'nielsen'         AND mt.nielsen_exclusive                                  THEN NULL
       WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen' THEN mt.tms_channel_affiliate
       ELSE CASE WHEN '{vendor_name}' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                    ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
  END AS channel_affiliate

, CASE WHEN mt.vizio_epg_not_null THEN 't'
       WHEN mt.acrb_clients != '||' AND mt.acrb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       WHEN mt.client_id_not_null != '||'        AND mt.client_id_not_null NOT LIKE '%|{client_name}|%'    THEN NULL
       WHEN '{client_name}' != 'nielsen'         AND mt.nielsen_exclusive                                  THEN NULL
       WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|{client_name}|%' THEN NULL
       WHEN '{client_name}' = 'nielsen'  AND mt.tms_airdate IS NULL                                        THEN NULL
       WHEN '{client_name}' != 'nielsen' AND COALESCE(mt.tivo_airdate, mt.tms_airdate) IS NULL             THEN NULL
       ELSE mt.is_live
  END AS live

, mt.ip_address AS ip
, mt.input_category
, mt.input_device
, CASE WHEN mt.vizio_epg_not_null = False AND mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|{client_name}|%' THEN NULL
       ELSE mt.app_service END AS app_service

FROM dev.mohit_gangwani.cure_viewing_content_first_iteration AS mt
JOIN prod.detection.tv_settings AS tv_settings
  ON mt.fk_tvid = tv_settings.fk_tvid
 AND mt.session_start >= tv_settings.create_timestamp
 AND mt.session_start < tv_settings.next_create_timestamp
--  AND tv_settings.create_timestamp <= '{end_time}'::timestamp
--  AND tv_settings.next_create_timestamp >= '{start_time}'::timestamp
JOIN prod.detection.settings AS settings
  ON tv_settings.fk_settings_id = settings.settings_id
 AND UPPER(settings.country_name) = 'USA'
JOIN prod.detection.tv_populations AS u
  ON mt.fk_tvid = u.fk_tvid
JOIN prod.detection.populations AS pop
  ON u.fk_population_id = pop.population_id 
 AND pop.population_name = 'opted_in'
 
-- Following conditions are only for Content Only Reports
WHERE mt.content_only_condition = FALSE
  AND CASE WHEN '{client_name}' != 'nielsen'  AND mt.nielsen_exclusive THEN FALSE ELSE TRUE END
  AND CASE WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|{client_name}|%' THEN FALSE ELSE TRUE END
  AND CASE WHEN mt.acrb_clients != '||' AND mt.acrb_clients NOT LIKE '%|{client_name}|%'          THEN FALSE ELSE TRUE END
  AND CASE WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|{client_name}|%' THEN FALSE ELSE TRUE END

-- COMMAND ----------

SELECT CASE WHEN mt.acrb_clients != '||' AND mt.acrb_clients NOT LIKE '%|discovery|%' THEN 1 END AS check_acrb
,CASE WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|discovery|%' THEN 2 END AS check_cli
,CASE WHEN mt.nielsen_exclusive THEN 3 END AS check_nielsen
,CASE WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|discovery|%' THEN 4 END AS check_sttnb
, COUNT(*)
FROM dev.mohit_gangwani.cure_viewing_content_first_iteration mt
GROUP BY 1, 2, 3, 4

-- COMMAND ----------

SELECT CASE WHEN mt.acrb_clients != '||' AND mt.acrb_clients NOT LIKE '%|altice|%' THEN 1
            WHEN mt.client_id_not_null != '||' AND mt.client_id_not_null NOT LIKE '%|altice|%' THEN 2
            WHEN mt.nielsen_exclusive THEN 3
            WHEN mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients LIKE '%|altice|%' THEN 4 END AS check_sttnb
, COUNT(*)
FROM dev.mohit_gangwani.cure_viewing_content_first_iteration mt
GROUP BY 1

-- COMMAND ----------


