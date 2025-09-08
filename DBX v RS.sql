-- Databricks notebook source
SELECT * FROM customer_reports.hashes_production_cadent
WHERE hash IN ('566d4a5444346973687850426370655a54675159583777697565626e4f7164704648536e587037776672513d',
               '5a7463616467695a49444f4d594a503954726570363777697565626e4f7164704648536e587037776672513d',
               '6e5a552b5073574a35584d6633524b4b69736134653777697565626e4f7164704648536e587037776672513d'
);

-- COMMAND ----------

SELECT * FROM customer_reports.hashes_production_comscore
WHERE hash IN ('467457674c61376c34526c364742506f3346626e427948684252502b53576c45704757484a6b6b363047633d',
               '6848445857334255376145546d797748436f4475784d43707630786d7961384f6b436d38664950706154383d',
               '70344a644c546c2f797a6f39636d3037356f64494b7343707630786d7961384f6b436d38664950706154383d',
               '7544396e58345773476654456750753775703065596343707630786d7961384f6b436d38664950706154383d',
               '7234695632576545697844326f774e464c343572753843707630786d7961384f6b436d38664950706154383d'
);


-- COMMAND ----------


SELECT * FROM customer_reports.hashes_production_discovery
WHERE hash IN ('724e51332b31736150625a4b59574f566a757549797a2b756f6d3554643057484f504f6d516c54663851593d',
               '3548632b4a77325a574e59766f773830684d5a2b4a314c4655374c4650474570504a442b30354d552f73633d',
               '755a2b59704579632b6e7a71375168504959546555734a794f6a6562447165506a4639686e395035546d513d',
               '754370506b62694751346b34374772765a6148584e384a794f6a6562447165506a4639686e395035546d513d',
               '72305247316634334959657a6652685574656d7043642b66677535354e704c346e4f445753617a623642773d',
               '7a5138666a76525339437131626b546c414b514c2b386f53446b50466d482f5a76766f4c666c6b394477673d',
               '79705a49304b567438334a414b625679726b647a68384a794f6a6562447165506a4639686e395035546d513d',
               '776e6868502b45624343777147363837614b536f70534c7575525968554531396e533145394d2b4d4362773d'
);



-- COMMAND ----------

SELECT * FROM customer_reports.hashes_production_geniusdigital
WHERE hash IN ('49316e70477469775043667539413163772b307330544a357647713238385131784d316864442b4968576b3d',
               '68586348344c58737651524f377a55324a316a31516a4a357647713238385131784d316864442b4968576b3d',
               '796150622b4a7a7169623853474d4530535730662f7a4a357647713238385131784d316864442b4968576b3d'
);

-- COMMAND ----------

SELECT DATE_TRUNC('MONTH', joined_date)
, COUNT(DISTINCT token)
FROM (
SELECT tv.token
, tv.joined_date
, MAX(ta.session_start) AS last_activity_date
-- , DATE_ADD('DAYS', 30, last_activity_date) AS lad_plus_30
-- , DATE_ADD('DAYS', 7, last_activity_date) AS lad_plus_7
FROM detection.tv
JOIN detection.tv_activity ta
  ON ta.fk_tvid = tv.tvid
JOIN detection.tv_settings_latest_daily tvst
  ON tvst.tvid = tv.tvid
WHERE tv.oem = 'VIZIO'
  AND tvst.country_name = 'USA'
  AND ta.session_end >= '2022-07-01 00:00:00'
GROUP BY 1, 2)
GROUP BY 1

-- COMMAND ----------


SELECT dateadd(SECOND, 5, to_timestamp('2023-01-26 07:00:00')) AS diff_in_next_start

-- COMMAND ----------

SELECT * FROM customer_reports.hashes_production_4info
WHERE hash = '2b2b30386c4f656b6b7050496876702f787676612f4d2b73586937726734574d3678454a53484e612f68733d';

-- COMMAND ----------

SELECT date_add('SECONDS', 4, to_timestamp('2023-01-25 07:00:00')) AS diff_in_next_start

-- COMMAND ----------

SELECT * FROM detection.tv_ip_address
WHERE fk_tvid = 150404274
;

-- COMMAND ----------

SELECT datediff(SECOND, to_timestamp('2023-01-25 07:00:00'), to_timestamp('2023-01-26 07:00:00')) AS diff_in_next_start

-- COMMAND ----------

select * FROM detection.tv_activity
LIMIT 10;

-- COMMAND ----------

WITH  date_range AS (
    SELECT 
        '2023-01-25T07:00:00'::timestamp AS start_date,
        '2023-01-25T08:00:00'::timestamp AS end_date
),
activity_ip_lookup AS (
  SELECT *
  , DATEDIFF(SECOND, ip_session_end::timestamp, next_start::timestamp) AS diff_in_next_start
    FROM (
    SELECT *
    , LEAD(ip_session_start) OVER (PARTITION BY fk_tvid ORDER BY ip_session_start) AS next_start
    , LAG(ip_session_end) OVER (PARTITION BY fk_tvid ORDER BY ip_session_start) AS prev_end
    FROM (
      SELECT tv.fk_tvid,
          tv.session_end,
          GREATEST(tvip.create_timestamp, dr.start_date) AS ip_session_start,
          LEAST(tvip.next_create_timestamp, dr.end_date) AS ip_session_end,
          tvip.ip_address
      FROM detection.tv_activity tv
      JOIN date_range dr ON 1=1
      JOIN detection.tv_ip_address tvip
        ON tv.fk_tvid=tvip.fk_tvid
       AND tv.session_end > tvip.create_timestamp
       AND tv.session_end < tvip.next_create_timestamp
      WHERE tvip.next_create_timestamp >= dr.start_date
        AND tvip.create_timestamp <= dr.end_date
        AND tv.session_end >= dr.start_date
        AND tv.session_end < dr.end_date
     )
 )
),
activity_ip_lookup_union AS (                      
    (SELECT 
        fk_tvid,
        session_end,
        ip_session_start,
        ip_session_end,
        ip_address,
        next_start,
        prev_end,
        diff_in_next_start
    FROM activity_ip_lookup)
    UNION
    (SELECT 
        fk_tvid,                                  
        session_end,
        DATEADD(SECOND, 0, ip_session_end),
        DATEADD(SECOND, diff_in_next_start - 1, ip_session_end),
        ip_address,
        ip_session_start,
        ip_session_end,
        diff_in_next_start
    FROM activity_ip_lookup
    WHERE diff_in_next_start > 0)
    UNION
    (SELECT 
        fk_tvid,                         
        session_end,
        dr.start_date,
        DATEADD(SECOND, 0, ip_session_start),
        ip_address,
        ip_session_start,
        ip_session_end,
        diff_in_next_start
    FROM activity_ip_lookup
    JOIN date_range dr ON 1=1
    WHERE prev_end IS NULL
      AND ip_session_start > dr.start_date)
    UNION
    (SELECT 
        fk_tvid,                                 
        session_end,
        DATEADD(SECOND, 0, ip_session_end),
        dr.end_date,
        ip_address,
        ip_session_start,
        ip_session_end,
        diff_in_next_start
    FROM activity_ip_lookup
    JOIN date_range dr ON 1=1
    WHERE next_start IS NULL
      AND ip_session_end < dr.end_date)
)
SELECT tvip_joins.tvid_,
    '',
    tvip_joins.ip,
    GREATEST((MIN(MIN(tvip_joins.ip_session_start)) OVER (PARTITION BY tvip_joins.tvid_, tvip_joins.ip)),
    (SELECT start_date FROM date_range)) AS session_start,
    LEAST((MAX(MAX(tvip_joins.ip_session_end)) OVER (PARTITION BY tvip_joins.tvid_, tvip_joins.ip)),
    (SELECT end_date FROM date_range)) AS session_end,
    tvip_joins.city,
    tvip_joins.iso_state,
    tvip_joins.dma,
    tvip_joins.zipcode
FROM ( 
  SELECT COALESCE(tv.long_tvid, tv.vizio_tvid) as tvid_,
      '',
      tvip.ip_address AS ip,
      REPLACE(location.city, ',', '')AS city,
      location.iso_state,
      REPLACE(dma.dma_name, ',', '') AS dma,
      location.zipcode,
      ip_session_start,
      ip_session_end
  FROM activity_ip_lookup_union tvip
  JOIN date_range dr ON 1=1
  JOIN detection.tv as tv
      ON tv.tvid = tvip.fk_tvid
      AND tv.oem = 'VIZIO'
  JOIN detection.tv_zoo AS tvz
      ON tvz.fk_tvid = tvip.fk_tvid
      AND tvz.next_create_timestamp >= dr.start_date
  JOIN detection.zoo AS z
      ON tvz.fk_zoo_id = z.zoo_id
      AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
  JOIN detection.tv_populations AS u
      ON u.fk_tvid = tvip.fk_tvid
  JOIN detection.populations AS pop
      ON u.fk_population_id = pop.population_id
      AND pop.population_name = 'opted_in'
  JOIN detection.tv_geolocation geo
      ON geo.fk_tvid = tvip.fk_tvid
      AND tvip.session_end >= geo.create_timestamp
      AND tvip.session_end < geo.next_create_timestamp
      AND geo.next_create_timestamp >= dr.start_date
  JOIN detection.location location
      ON geo.fk_location_id = location.location_id
      AND location.country_code = 'US'
  LEFT JOIN detection.dma AS dma
      ON location.fk_dma_id = dma.dma_id
  JOIN detection.tv_settings tv_settings
      ON tvip.fk_tvid = tv_settings.fk_tvid
      AND tvip.session_end >= tv_settings.create_timestamp
      AND tvip.session_end < tv_settings.next_create_timestamp
      AND tv_settings.next_create_timestamp >= dr.start_date
  JOIN detection.settings AS settings
      ON tv_settings.fk_settings_id = settings.settings_id
  WHERE tv.chipset != 'MSERIES'
    AND settings.disabled != 1
) as tvip_joins
GROUP BY tvip_joins.tvid_, tvip_joins.ip, tvip_joins.city, tvip_joins.iso_state, tvip_joins.dma, tvip_joins.zipcode
ORDER BY tvip_joins.tvid_
LIMIT 100;

-- COMMAND ----------

SELECT * FROM detection.viewing_commercials_firehose
WHERE fk_tvid = 151714751
AND session_start = '2023-01-18 02:53:16';

-- COMMAND ----------

SELECT * FROM detection.clients;

-- COMMAND ----------

SELECT vc.fk_tvid, vc.session_start, vc.session_end, vc.session_duration
, geo.zipcode
, dma.dma_name
-- , sh.database_key AS prev_dbkey
-- , sh.title AS prev_title
-- , st.station_call_sign AS prev_cs
-- , sh.database_key AS next_dbkey
-- , sh.title AS next_title
FROM detection.viewing_commercials_firehose vc
JOIN detection.commercial_id_external_firehose cief
  ON cief.fk_commercial_id = vc.fk_commercial_id
JOIN detection.clients cl
  ON cief.fk_client_id =cl.client_id
JOIN customer_reports.hashes_production_oath ho
  ON SPLIT_PART(ho.tvid, '_', 1) = vc.fk_tvid
-- LEFT JOIN detection.epg_station st
--   ON st.station_id = vc.prev_station_id
-- LEFT JOIN detection.epg_show sh
--   ON sh.show_id = vc.prev_show_id
LEFT JOIN detection.location geo
  ON geo.location_id = vc.fk_location_id
LEFT JOIN detection.dma
  ON dma.dma_id = vc.fk_dma_id
WHERE vc.session_start = '2023-01-18 06:10:19'
  AND client_name IN ('oath', 'kinetiq')
  AND ho.hash = '2b2b6346376d494d48687631627547667375534a65477630724a774c76387245436b2b504e65674b6a4e733d';

-- COMMAND ----------

SELECT * FROM detection.tv
WHERE tvid = 3317160

-- COMMAND ----------

SELECT * FROM detection.tv_inputsource
WHERE fk_tvid = 3317160
AND create_timestamp >= '2023-01-01';

-- COMMAND ----------


SELECT * FROM detection.epg_station;

-- COMMAND ----------


