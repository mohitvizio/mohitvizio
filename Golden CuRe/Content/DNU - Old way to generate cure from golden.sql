-- Databricks notebook source
-- DBTITLE 1,AdImpact - Content Only
DROP TABLE IF EXISTS dev.mohit_gangwani.content_adimpact_new;
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_adimpact_new (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.content_adimpact_new (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
SELECT mt.tvid
, '' AS hash
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adimpact|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adimpact|%'                                                   THEN NULL
       WHEN 'adimpact' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adimpact|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN mt.tms_episode_id ELSE mt.tivo_episode_id END
  END AS episode_id

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
                                            ELSE COALESCE(mt.tivo_title, mt.tms_title) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adimpact|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adimpact|%'                                                   THEN NULL
       WHEN 'adimpact' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adimpact|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adimpact' = 'nielsen' THEN mt.tms_title
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title) ELSE COALESCE(mt.tivo_title, mt.tms_title) END
  END AS show_title

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                                   ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adimpact|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adimpact|%'                                                   THEN NULL
       WHEN 'adimpact' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adimpact|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adimpact' = 'nielsen' THEN mt.tms_airdate
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                 ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
  END AS air_date

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                                             ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adimpact|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adimpact|%'                                                   THEN NULL
       WHEN 'adimpact' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adimpact|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adimpact' = 'nielsen' THEN mt.tms_channel_callsign
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                 ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
  END AS channel_callsign

, CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adimpact|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adimpact|%'                                                   THEN NULL
       WHEN 'adimpact' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adimpact|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adimpact' = 'nielsen'           AND mt.tms_airdate IS NULL                                                                               THEN NULL
       ELSE mt.mt_start
  END AS mt_start

, session_start AS ts_start
, session_end   AS ts_end

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                                   ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adimpact|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adimpact|%'                                                   THEN NULL
       WHEN 'adimpact' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adimpact|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adimpact' = 'nielsen' THEN mt.tms_channel_affiliate
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                    ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
  END AS channel_affiliate

, CASE WHEN mt.vizio_epg_not_null THEN 't'
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adimpact|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adimpact|%'                                                   THEN NULL
       WHEN 'adimpact' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adimpact|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adimpact' = 'nielsen'  AND mt.tms_airdate IS NULL                                                                                        THEN NULL
       WHEN 'adimpact' != 'nielsen' AND COALESCE(mt.tivo_airdate, mt.tms_airdate) IS NULL                                                             THEN NULL
       ELSE mt.is_live
  END AS live

, mt.ip_address AS ip
, mt.input_category
, mt.input_device
, CASE WHEN mt.vizio_epg_not_null = False AND (mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|adimpact|%') OR mt.appb_clients = '|ALL|' THEN NULL
       ELSE mt.app_service END AS app_service

FROM dev.mohit_gangwani.cure_viewing_content_first_iteration AS mt
JOIN prod.detection.tv_populations AS u
  ON mt.fk_tvid = u.fk_tvid
JOIN prod.detection.populations AS pop
  ON u.fk_population_id = pop.population_id 
 AND pop.population_name = 'opted_in'

-- Following conditions are only for Content Only Reports
WHERE mt.content_only_condition = FALSE
  AND CASE WHEN 'adimpact' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN FALSE ELSE TRUE END
  AND CASE WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adimpact|%'                                                   THEN FALSE ELSE TRUE END
  AND CASE WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adimpact|%')  OR mt.acrb_clients = '|ALL|'                          THEN FALSE ELSE TRUE END
  AND CASE WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adimpact|%') OR mt.station_blacklist_clients = '|ALL|' THEN FALSE ELSE TRUE END

-- COMMAND ----------

-- DBTITLE 1,Content + Null - Adelaide
DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_adelaide_new;
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_with_null_adelaide_new (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.content_with_null_adelaide_new (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
SELECT mt.tvid
, '' AS hash
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adelaide|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adelaide|%'                                                   THEN NULL
       WHEN 'adelaide' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adelaide|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN mt.tms_episode_id ELSE mt.tivo_episode_id END
  END AS episode_id

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
                                            ELSE COALESCE(mt.tivo_title, mt.tms_title) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adelaide|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adelaide|%'                                                   THEN NULL
       WHEN 'adelaide' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adelaide|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adelaide' = 'nielsen' THEN mt.tms_title
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title) ELSE COALESCE(mt.tivo_title, mt.tms_title) END
  END AS show_title

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                                   ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adelaide|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adelaide|%'                                                   THEN NULL
       WHEN 'adelaide' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adelaide|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adelaide' = 'nielsen' THEN mt.tms_airdate
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                 ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
  END AS air_date

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                                             ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adelaide|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adelaide|%'                                                   THEN NULL
       WHEN 'adelaide' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adelaide|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adelaide' = 'nielsen' THEN mt.tms_channel_callsign
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                 ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
  END AS channel_callsign

, CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adelaide|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adelaide|%'                                                   THEN NULL
       WHEN 'adelaide' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adelaide|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adelaide' = 'nielsen'           AND mt.tms_airdate IS NULL                                                                               THEN NULL
       ELSE mt.mt_start
  END AS mt_start

, session_start AS ts_start
, session_end   AS ts_end

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                                   ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adelaide|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adelaide|%'                                                   THEN NULL
       WHEN 'adelaide' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adelaide|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adelaide' = 'nielsen' THEN mt.tms_channel_affiliate
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                    ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
  END AS channel_affiliate

, CASE WHEN mt.vizio_epg_not_null THEN 't'
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|adelaide|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|adelaide|%'                                                   THEN NULL
       WHEN 'adelaide' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|adelaide|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'adelaide' = 'nielsen'  AND mt.tms_airdate IS NULL                                                                                        THEN NULL
       WHEN 'adelaide' != 'nielsen' AND COALESCE(mt.tivo_airdate, mt.tms_airdate) IS NULL                                                             THEN NULL
       ELSE mt.is_live
  END AS live

, mt.ip_address AS ip
, mt.input_category
, mt.input_device
, CASE WHEN mt.vizio_epg_not_null = False AND (mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|adelaide|%') OR mt.appb_clients = '|ALL|' THEN NULL
       ELSE mt.app_service END AS app_service

FROM dev.mohit_gangwani.cure_viewing_content_first_iteration AS mt
JOIN prod.detection.tv_populations AS u
  ON mt.fk_tvid = u.fk_tvid
JOIN prod.detection.populations AS pop
  ON u.fk_population_id = pop.population_id 
 AND pop.population_name = 'opted_in'

-- COMMAND ----------

-- DBTITLE 1,Content + Null - Cognet
DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_cognet_new;
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_with_null_cognet_new (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.content_with_null_cognet_new (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
SELECT mt.tvid
, '' AS hash
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|cognet|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|cognet|%'                                                   THEN NULL
       WHEN 'cognet' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|cognet|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN mt.tms_episode_id ELSE mt.tivo_episode_id END
  END AS episode_id

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
                                            ELSE COALESCE(mt.tivo_title, mt.tms_title) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|cognet|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|cognet|%'                                                   THEN NULL
       WHEN 'cognet' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|cognet|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'cognet' = 'nielsen' THEN mt.tms_title
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title) ELSE COALESCE(mt.tivo_title, mt.tms_title) END
  END AS show_title

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                                   ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|cognet|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|cognet|%'                                                   THEN NULL
       WHEN 'cognet' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|cognet|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'cognet' = 'nielsen' THEN mt.tms_airdate
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                 ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
  END AS air_date

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                                             ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|cognet|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|cognet|%'                                                   THEN NULL
       WHEN 'cognet' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|cognet|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'cognet' = 'nielsen' THEN mt.tms_channel_callsign
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                 ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
  END AS channel_callsign

, CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|cognet|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|cognet|%'                                                   THEN NULL
       WHEN 'cognet' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|cognet|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'cognet' = 'nielsen'           AND mt.tms_airdate IS NULL                                                                               THEN NULL
       ELSE mt.mt_start
  END AS mt_start

, session_start AS ts_start
, session_end   AS ts_end

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                                   ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|cognet|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|cognet|%'                                                   THEN NULL
       WHEN 'cognet' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|cognet|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'cognet' = 'nielsen' THEN mt.tms_channel_affiliate
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                    ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
  END AS channel_affiliate

, CASE WHEN mt.vizio_epg_not_null THEN 't'
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|cognet|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|cognet|%'                                                   THEN NULL
       WHEN 'cognet' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|cognet|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'cognet' = 'nielsen'  AND mt.tms_airdate IS NULL                                                                                        THEN NULL
       WHEN 'cognet' != 'nielsen' AND COALESCE(mt.tivo_airdate, mt.tms_airdate) IS NULL                                                             THEN NULL
       ELSE mt.is_live
  END AS live

, mt.ip_address AS ip
, mt.input_category
, mt.input_device
, CASE WHEN mt.vizio_epg_not_null = False AND (mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|cognet|%') OR mt.appb_clients = '|ALL|' THEN NULL
       ELSE mt.app_service END AS app_service

FROM dev.mohit_gangwani.cure_viewing_content_first_iteration AS mt
JOIN prod.detection.tv_populations AS u
  ON mt.fk_tvid = u.fk_tvid
JOIN prod.detection.populations AS pop
  ON u.fk_population_id = pop.population_id 
 AND pop.population_name = 'opted_in'



-- COMMAND ----------

-- DBTITLE 1,Content + Null - Altice

DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_altice_new;
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_with_null_altice_new (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.content_with_null_altice_new (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
SELECT mt.tvid
, '' AS hash
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|altice|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|altice|%'                                                   THEN NULL
       WHEN 'altice' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|altice|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN mt.tms_episode_id ELSE mt.tivo_episode_id END
  END AS episode_id

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
                                            ELSE COALESCE(mt.tivo_title, mt.tms_title) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|altice|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|altice|%'                                                   THEN NULL
       WHEN 'altice' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|altice|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'altice' = 'nielsen' THEN mt.tms_title
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title) ELSE COALESCE(mt.tivo_title, mt.tms_title) END
  END AS show_title

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                                   ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|altice|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|altice|%'                                                   THEN NULL
       WHEN 'altice' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|altice|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'altice' = 'nielsen' THEN mt.tms_airdate
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                 ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
  END AS air_date

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                                             ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|altice|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|altice|%'                                                   THEN NULL
       WHEN 'altice' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|altice|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'altice' = 'nielsen' THEN mt.tms_channel_callsign
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                 ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
  END AS channel_callsign

, CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|altice|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|altice|%'                                                   THEN NULL
       WHEN 'altice' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|altice|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'altice' = 'nielsen'           AND mt.tms_airdate IS NULL                                                                               THEN NULL
       ELSE mt.mt_start
  END AS mt_start

, session_start AS ts_start
, session_end   AS ts_end

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                                   ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|altice|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|altice|%'                                                   THEN NULL
       WHEN 'altice' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|altice|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'altice' = 'nielsen' THEN mt.tms_channel_affiliate
       ELSE CASE WHEN 'TIVO' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                    ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
  END AS channel_affiliate

, CASE WHEN mt.vizio_epg_not_null THEN 't'
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|altice|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|altice|%'                                                   THEN NULL
       WHEN 'altice' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|altice|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'altice' = 'nielsen'  AND mt.tms_airdate IS NULL                                                                                        THEN NULL
       WHEN 'altice' != 'nielsen' AND COALESCE(mt.tivo_airdate, mt.tms_airdate) IS NULL                                                             THEN NULL
       ELSE mt.is_live
  END AS live

, mt.ip_address AS ip
, mt.input_category
, mt.input_device
, CASE WHEN mt.vizio_epg_not_null = False AND (mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|altice|%') OR mt.appb_clients = '|ALL|' THEN NULL
       ELSE mt.app_service END AS app_service

FROM dev.mohit_gangwani.cure_viewing_content_first_iteration AS mt
JOIN prod.detection.tv_populations AS u
  ON mt.fk_tvid = u.fk_tvid
JOIN prod.detection.populations AS pop
  ON u.fk_population_id = pop.population_id 
 AND pop.population_name = 'opted_in'


-- COMMAND ----------

-- DBTITLE 1,Content Only - Discovery
DROP TABLE IF EXISTS dev.mohit_gangwani.content_discovery_new;
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_discovery_new (
    tvid string, hash string, zipcode string, dma string, episode_id_tms string, show_title_tms string, air_date_tms string, channel_callsign_tms string, mt_start_tms integer, ts_start timestamp, ts_end timestamp, channel_affiliate_tms string, live_tms string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.content_discovery_new (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id_tms, 
    show_title_tms, 
    air_date_tms, 
    channel_callsign_tms, 
    mt_start_tms, 
    ts_start, 
    ts_end, 
    channel_affiliate_tms, 
    live_tms, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
SELECT mt.tvid
, '' AS hash
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|discovery|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|discovery|%'                                                   THEN NULL
       WHEN 'discovery' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|discovery|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       ELSE CASE WHEN 'TMS' = 'TMS' THEN mt.tms_episode_id ELSE mt.tivo_episode_id END
  END AS episode_id

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
                                            ELSE COALESCE(mt.tivo_title, mt.tms_title) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|discovery|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|discovery|%'                                                   THEN NULL
       WHEN 'discovery' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|discovery|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'discovery' = 'nielsen' THEN mt.tms_title
       ELSE CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title) ELSE COALESCE(mt.tivo_title, mt.tms_title) END
  END AS show_title

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                                   ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|discovery|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|discovery|%'                                                   THEN NULL
       WHEN 'discovery' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|discovery|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'discovery' = 'nielsen' THEN mt.tms_airdate
       ELSE CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                 ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
  END AS air_date

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                                             ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|discovery|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|discovery|%'                                                   THEN NULL
       WHEN 'discovery' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|discovery|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'discovery' = 'nielsen' THEN mt.tms_channel_callsign
       ELSE CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                 ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
  END AS channel_callsign

, CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|discovery|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|discovery|%'                                                   THEN NULL
       WHEN 'discovery' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|discovery|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'discovery' = 'nielsen'           AND mt.tms_airdate IS NULL                                                                               THEN NULL
       ELSE mt.mt_start
  END AS mt_start

, session_start AS ts_start
, session_end   AS ts_end

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                                   ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|discovery|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|discovery|%'                                                   THEN NULL
       WHEN 'discovery' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|discovery|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'discovery' = 'nielsen' THEN mt.tms_channel_affiliate
       ELSE CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                    ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
  END AS channel_affiliate

, CASE WHEN mt.vizio_epg_not_null THEN 't'
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|discovery|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|discovery|%'                                                   THEN NULL
       WHEN 'discovery' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|discovery|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'discovery' = 'nielsen'  AND mt.tms_airdate IS NULL                                                                                        THEN NULL
       WHEN 'discovery' != 'nielsen' AND COALESCE(mt.tivo_airdate, mt.tms_airdate) IS NULL                                                             THEN NULL
       ELSE mt.is_live
  END AS live

, mt.ip_address AS ip
, mt.input_category
, mt.input_device
, CASE WHEN mt.vizio_epg_not_null = False AND (mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|discovery|%') OR mt.appb_clients = '|ALL|' THEN NULL
       ELSE mt.app_service END AS app_service

FROM dev.mohit_gangwani.cure_viewing_content_first_iteration AS mt
JOIN prod.detection.tv_populations AS u
  ON mt.fk_tvid = u.fk_tvid
JOIN prod.detection.populations AS pop
  ON u.fk_population_id = pop.population_id 
 AND pop.population_name = 'opted_in'

-- Following conditions are only for Content Only Reports
WHERE mt.content_only_condition = FALSE
  AND CASE WHEN 'discovery' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN FALSE ELSE TRUE END
  AND CASE WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|discovery|%'                                                   THEN FALSE ELSE TRUE END
  AND CASE WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|discovery|%')  OR mt.acrb_clients = '|ALL|'                          THEN FALSE ELSE TRUE END
  AND CASE WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|discovery|%') OR mt.station_blacklist_clients = '|ALL|' THEN FALSE ELSE TRUE END
  

-- COMMAND ----------

-- DBTITLE 1,Content + Null - Nielsen
DROP TABLE IF EXISTS dev.mohit_gangwani.content_with_null_nielsen_new;
CREATE TABLE IF NOT EXISTS dev.mohit_gangwani.content_with_null_nielsen_new (
    tvid string, hash string, zipcode string, dma string, episode_id string, show_title string, air_date string, channel_callsign string, mt_start integer, ts_start timestamp, ts_end timestamp, channel_affiliate string, live string, ip string, input_category string, input_device string, app_service string
    );
INSERT INTO dev.mohit_gangwani.content_with_null_nielsen_new (
    tvid, 
    hash, 
    zipcode, 
    dma, 
    episode_id, 
    show_title, 
    air_date, 
    channel_callsign, 
    mt_start, 
    ts_start, 
    ts_end, 
    channel_affiliate, 
    live, 
    ip, 
    input_category, 
    input_device, 
    app_service
)
SELECT mt.tvid
, '' AS hash
, mt.zipcode
, mt.dma
, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN mt.tms_episode_id ELSE NULL END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|nielsen|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|nielsen|%'                                                   THEN NULL
       WHEN 'nielsen' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|nielsen|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       ELSE CASE WHEN 'TMS' = 'TMS' THEN mt.tms_episode_id ELSE mt.tivo_episode_id END
  END AS episode_id

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title)
                                            ELSE COALESCE(mt.tivo_title, mt.tms_title) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|nielsen|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|nielsen|%'                                                   THEN NULL
       WHEN 'nielsen' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|nielsen|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'nielsen' = 'nielsen' THEN mt.tms_title
       ELSE CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_title, mt.tivo_title) ELSE COALESCE(mt.tivo_title, mt.tms_title) END
  END AS show_title

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                                   ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|nielsen|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|nielsen|%'                                                   THEN NULL
       WHEN 'nielsen' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|nielsen|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'nielsen' = 'nielsen' THEN mt.tms_airdate
       ELSE CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_airdate, mt.tivo_airdate)
                 ELSE COALESCE(mt.tivo_airdate, mt.tms_airdate) END
  END AS air_date

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                                             ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|nielsen|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|nielsen|%'                                                   THEN NULL
       WHEN 'nielsen' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|nielsen|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'nielsen' = 'nielsen' THEN mt.tms_channel_callsign
       ELSE CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_callsign, mt.tivo_channel_callsign)
                 ELSE COALESCE(mt.tivo_channel_callsign, mt.tms_channel_callsign) END
  END AS channel_callsign

, CASE WHEN mt.vizio_epg_not_null THEN mt.mt_start
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|nielsen|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|nielsen|%'                                                   THEN NULL
       WHEN 'nielsen' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|nielsen|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'nielsen' = 'nielsen'           AND mt.tms_airdate IS NULL                                                                               THEN NULL
       ELSE mt.mt_start
  END AS mt_start

, session_start AS ts_start
, session_end   AS ts_end

, CASE WHEN mt.vizio_epg_not_null THEN CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                                   ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|nielsen|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|nielsen|%'                                                   THEN NULL
       WHEN 'nielsen' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|nielsen|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'nielsen' = 'nielsen' THEN mt.tms_channel_affiliate
       ELSE CASE WHEN 'TMS' = 'TMS' THEN COALESCE(mt.tms_channel_affiliate, mt.tivo_channel_affiliate)
                    ELSE COALESCE(mt.tivo_channel_affiliate, mt.tms_channel_affiliate) END
  END AS channel_affiliate

, CASE WHEN mt.vizio_epg_not_null THEN 't'
       WHEN (mt.acrb_clients != '||'              AND mt.acrb_clients NOT LIKE '%|nielsen|%')          OR mt.acrb_clients = '|ALL|'                  THEN NULL
       WHEN mt.client_id_not_null != '||'         AND mt.client_id_not_null NOT LIKE '%|nielsen|%'                                                   THEN NULL
       WHEN 'nielsen' != 'nielsen'          AND mt.nielsen_exclusive                                                                                 THEN NULL
       WHEN (mt.station_blacklist_clients != '||' AND mt.station_blacklist_clients NOT LIKE '%|nielsen|%') OR mt.station_blacklist_clients = '|ALL|' THEN NULL
       WHEN 'nielsen' = 'nielsen'  AND mt.tms_airdate IS NULL                                                                                        THEN NULL
       WHEN 'nielsen' != 'nielsen' AND COALESCE(mt.tivo_airdate, mt.tms_airdate) IS NULL                                                             THEN NULL
       ELSE mt.is_live
  END AS live

, mt.ip_address AS ip
, mt.input_category
, mt.input_device
, CASE WHEN mt.vizio_epg_not_null = False AND (mt.appb_clients != '||' AND mt.appb_clients NOT LIKE '%|nielsen|%') OR mt.appb_clients = '|ALL|' THEN NULL
       ELSE mt.app_service END AS app_service

FROM dev.mohit_gangwani.cure_viewing_content_first_iteration AS mt
JOIN prod.detection.tv_populations AS u
  ON mt.fk_tvid = u.fk_tvid
JOIN prod.detection.populations AS pop
  ON u.fk_population_id = pop.population_id 
 AND pop.population_name = 'opted_in'


-- COMMAND ----------

SELECT COUNT(*) FROM dev.mohit_gangwani.cure_viewing_content_first_iteration

-- COMMAND ----------


