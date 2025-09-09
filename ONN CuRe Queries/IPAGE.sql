DROP TABLE IF EXISTS prod.customer_reports.r333_ipage_4info_2025_08_13_07_production;
CREATE TABLE IF NOT EXISTS prod.customer_reports.r333_ipage_4info_2025_08_13_07_production (
    tvid string, hash string, ip string, ts_start timestamp, ts_end timestamp, city string, iso_state string, dma string, zipcode string
    );
INSERT INTO prod.customer_reports.r333_ipage_4info_2025_08_13_07_production (
    tvid, hash, ip, ts_start, ts_end, city, iso_state, dma, zipcode
)
WITH
date_range AS (
    SELECT
        '2025-08-13T07:00:00'::timestamp AS start_date,
        '2025-08-14T07:00:00'::timestamp AS end_date
),
activity_ip_lookup AS (
    SELECT
        next_start_prev_end.*,
        unix_timestamp(next_start::timestamp) - unix_timestamp(ip_session_end::timestamp) AS diff_in_next_start
        FROM (
            SELECT
                tv_join_tvip.* ,
                LEAD(ip_session_start) OVER (PARTITION BY fk_tvid ORDER BY ip_session_start) AS next_start,
                LAG(ip_session_end) OVER (PARTITION BY fk_tvid ORDER BY ip_session_start) AS prev_end
            FROM (
                SELECT
                    tv.fk_tvid,
                    tv.session_end,
                    GREATEST(tvip.create_timestamp, (SELECT start_date FROM date_range)) AS ip_session_start,
                    LEAST(tvip.next_create_timestamp, (SELECT end_date FROM date_range)) AS ip_session_end,
                    tvip.ip_address
                FROM
                    prod.detection.tv_activity tv
                JOIN
                    prod.detection.tv_ip_address tvip
                    ON tv.fk_tvid=tvip.fk_tvid
                    AND tv.session_end > tvip.create_timestamp
                    AND tv.session_end < tvip.next_create_timestamp
                    AND tv.session_end >= (SELECT start_date FROM date_range)
                    AND tv.session_end < (SELECT end_date FROM date_range)
            )as tv_join_tvip
        )as next_start_prev_end
),
activity_ip_lookup_union AS (
    SELECT
        fk_tvid,
        session_end,
        ip_session_start,
        ip_session_end,
        ip_address,
        next_start,
        prev_end,
        diff_in_next_start
    FROM activity_ip_lookup
        UNION
    SELECT
        fk_tvid,
        session_end,
        timestampadd(SECOND, 0, ip_session_end),
        timestampadd(SECOND, diff_in_next_start - 1, ip_session_end),
        ip_address,
        ip_session_start,
        ip_session_end,
        diff_in_next_start
    FROM activity_ip_lookup
    WHERE diff_in_next_start > 0
        UNION
    SELECT
        fk_tvid,
        session_end,
        (SELECT start_date FROM date_range),
        timestampadd(SECOND, 0, ip_session_start),
        ip_address,
        ip_session_start,
        ip_session_end,
        diff_in_next_start
    FROM activity_ip_lookup
    WHERE prev_end IS NULL
        AND ip_session_start > (SELECT start_date FROM date_range)
        UNION
    SELECT
        fk_tvid,
        session_end,
        timestampadd(SECOND, 0, ip_session_end),
        (SELECT end_date FROM date_range),
        ip_address,
        ip_session_start,
        ip_session_end,
        diff_in_next_start
    FROM activity_ip_lookup
    WHERE next_start IS NULL
    AND ip_session_end < (SELECT end_date FROM date_range)
)
SELECT
    tvip_joins.tvid_,
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
    SELECT
        COALESCE(tv.long_tvid, tv.vizio_tvid) as tvid_,
        '',
        tvip.ip_address AS ip,
        REPLACE(location.city, ',', '')AS city,
        location.iso_state,
        REPLACE(dma.dma_name, ',', '') AS dma,
        location.zipcode,
        ip_session_start,
        ip_session_end
    FROM activity_ip_lookup_union tvip
    JOIN prod.detection.tv as tv
        ON tv.tvid = tvip.fk_tvid
        AND tv.oem in ('VIZIO')           -- If VIZIO only
        -- AND tv.oem in ('ONN')          -- If ONN only
        -- AND tv.oem in ('VIZIO', 'ONN') -- If with ONN and VIZIO
    JOIN prod.detection.tv_zoo AS tvz
        ON tvz.fk_tvid = tvip.fk_tvid
    JOIN prod.detection.zoo AS z
        ON tvz.fk_zoo_id = z.zoo_id
        AND z.zoo = 'control-zoo-dtsprod.tvinteractive.tv'
    JOIN prod.detection.tv_populations AS u
        ON u.fk_tvid = tvip.fk_tvid
    JOIN prod.detection.populations AS pop
        ON u.fk_population_id = pop.population_id
        AND pop.population_name = 'opted_in'
    JOIN prod.detection.tv_geolocation geo
        ON geo.fk_tvid = tvip.fk_tvid
        AND tvip.session_end >= geo.create_timestamp
        AND tvip.session_end < geo.next_create_timestamp
    JOIN prod.detection.location location
        ON geo.fk_location_id = location.location_id
        AND location.country_code = 'US'
    LEFT JOIN prod.detection.dma AS dma
        ON location.fk_dma_id = dma.dma_id
    JOIN prod.detection.tv_settings tv_settings
        ON tvip.fk_tvid = tv_settings.fk_tvid
        AND tv_settings.create_timestamp <= (SELECT end_date FROM date_range)
        AND tv_settings.next_create_timestamp >= (SELECT start_date FROM date_range)
        AND tvip.session_end >= tv_settings.create_timestamp
        AND tvip.session_end < tv_settings.next_create_timestamp
    JOIN prod.detection.settings AS settings
        ON tv_settings.fk_settings_id = settings.settings_id
    WHERE NOT (tv.chipset = 'MSERIES' AND settings.disabled = 1)
) as tvip_joins
GROUP BY tvip_joins.tvid_, tvip_joins.ip, tvip_joins.city, tvip_joins.iso_state, tvip_joins.dma, tvip_joins.zipcode