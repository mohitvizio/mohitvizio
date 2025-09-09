DROP TABLE IF EXISTS prod.customer_reports.r478_tvadid_experian_2025_08_17_07_production;
CREATE TABLE IF NOT EXISTS prod.customer_reports.r478_tvadid_experian_2025_08_17_07_production (
    tvid string, hash string, tvadid string, tvadid_type string, dai_enabled string
    );
INSERT INTO prod.customer_reports.r478_tvadid_experian_2025_08_17_07_production (
    tvid, 
    hash, 
    tvadid, 
    tvadid_type, 
    dai_enabled
)
WITH dai_distinct_tvs AS (    
    SELECT joined_date,
        COALESCE(tv.long_tvid, tv.vizio_tvid) AS vizio_tvid,
        tvid,
        ROW_NUMBER () OVER (PARTITION BY token ORDER BY joined_date DESC) as rownum  
    FROM prod.detection.tv
    WHERE tv.oem IN ('VIZIO')        -- If only VIZIO
    -- WHERE tv.oem IN ('ONN')          -- If only ONN
    -- WHERE tv.oem IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
    )
    SELECT DISTINCT
        tvs.vizio_tvid, 
        '', 
        dai.tv_ad_id, 
        dai.tv_ad_id_type, 
        CASE
        WHEN dai.dai_disabled = 0 THEN 't'
           ELSE 'f' END
    FROM prod.public.dai_user_history_latest dai
    JOIN dai_distinct_tvs tvs
        ON tvs.tvid = dai.tvid
        AND tvs.rownum=1
    LEFT JOIN prod.detection.tv_geolocation_latest_daily AS geo
        ON geo.tvid = dai.tvid
        AND geo.country_code = 'US'
    JOIN prod.detection.tv_populations AS u
        ON u.fk_tvid = tvs.tvid
    JOIN prod.detection.populations AS pop
        ON u.fk_population_id = pop.population_id
    WHERE 
        pop.population_name = 'opted_in'
        AND dai.tv_ad_id_type = 'vida'
        AND dai.lmt = 0
        AND tvs.rownum=1
        AND dai.tv_ad_id NOT IN ('LMT', '', 'fake_ad_id')
        AND dai.tv_ad_id <> '1'
        AND LOWER(dai.tv_ad_id) NOT IN ('false','true','t','f')
UNION
    SELECT DISTINCT
        tvs.vizio_tvid as tvid,
        '',
        dai.tv_ad_id,
        dai.tv_ad_id_type,
        CASE
            WHEN daid.dai_disabled = 0 THEN 't'
               ELSE 'f' END AS dai_enabled
    FROM prod.detection.tv_ad_id_latest dai
    JOIN dai_distinct_tvs tvs
        ON tvs.tvid = dai.tvid
        AND tvs.rownum=1
    LEFT JOIN prod.detection.tv_geolocation_latest_daily AS geo
        ON geo.tvid = dai.tvid
        AND geo.country_code = 'US'
    JOIN prod.detection.tv_populations AS u
        ON u.fk_tvid = tvs.tvid
    JOIN prod.detection.populations AS pop
        ON u.fk_population_id = pop.population_id
    LEFT JOIN prod.public.dai_user_history_latest daid
    ON dai.tvid = daid.tvid
    WHERE
        pop.population_name = 'opted_in'
        AND dai.tv_ad_id_type = 'vida'
        AND dai.lmt = 0
        AND tvs.rownum=1
        AND dai.tv_ad_id NOT IN ('LMT', '', 'fake_ad_id')
        AND dai.tv_ad_id <> '1'
        AND LOWER(dai.tv_ad_id) NOT IN ('false','true','t','f');