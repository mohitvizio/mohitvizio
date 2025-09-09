DROP TABLE IF EXISTS prod.customer_reports.r1511_householdinfo_tvsquared_2025_07_01_07_production;
CREATE TABLE IF NOT EXISTS prod.customer_reports.r1511_householdinfo_tvsquared_2025_07_01_07_production (
    house_month date,
    local_market_name string,
    number_tvs integer,
    number_HDTVs integer,
    percent_hdtvs_in_market float,
    num_tv_homes_universe integer,
    percent_TV_Homes_Universe float,
    percent_TV_Homes_Universe_with_Inscape_HDTV float
    );
INSERT INTO prod.customer_reports.r1511_householdinfo_tvsquared_2025_07_01_07_production (
    house_month,
    local_market_name,
    number_tvs,
    number_HDTVs,
    percent_hdtvs_in_market,
    num_tv_homes_universe,
    percent_TV_Homes_Universe,
    percent_TV_Homes_Universe_with_Inscape_HDTV
)
SELECT 
    date(month) AS house_month,
    REPLACE('"' || dma_name || '"', ',', '') AS local_market_name,
    number_tvs,
    number_hd_tvs AS number_HDTVs,
    round((number_hd_tvs / sum(number_hd_tvs) over (partition by month)), 4) as percent_hdtvs_in_market,
    population AS num_tv_homes_universe,
    round((population / sum(population) over (partition by month)), 4) as percent_TV_Homes_Universe,
    percent_TV_Homes_Universe_with_Inscape_HDTV
FROM (
    SELECT 
        date_trunc('month', tv_input_stats.create_timestamp) AS month,
        dma_name,
        1.0 * COUNT(DISTINCT CASE WHEN category = 'HD TV' THEN ip_address ELSE NULL END) / population AS Percent_TV_Homes_Universe_with_Inscape_HDTV,
        COUNT(DISTINCT tv_input_stats.fk_tvid) AS number_tvs,
        COUNT(DISTINCT CASE WHEN category = 'HD TV' THEN tv_input_stats.fk_tvid ELSE NULL END) AS number_hd_tvs,
        population
    FROM (
        SELECT 
            category,
            date_trunc('month', create_timestamp) AS create_timestamp,
            date_add(month, 1, date_trunc('month', create_timestamp)) AS next_create_timestamp,
            fk_tvid,
            SUM(total_duration) AS total_duration,
            CASE
                WHEN SUM(total_duration) > 0 THEN
                    SUM(total_duration * detection_rate) / SUM(total_duration)
                ELSE 0
            END AS detection_rate
        FROM (
            SELECT * FROM prod.detection.tv_input_stats_firehose v
            WHERE 'VIZIO' IN ('VIZIO')           -- If only VIZIO
            -- WHERE 'VIZIO' IN ('ONN')          -- If only ONN
            -- WHERE 'VIZIO' IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
              AND v.create_timestamp >= date_add(month, -1, date_trunc('month', current_date()))
              AND v.total_duration > 0
            UNION
            SELECT * FROM prod.detection_onn.tv_input_stats_firehose o
            WHERE 'ONN' IN ('VIZIO')           -- If only VIZIO
            -- WHERE 'ONN' IN ('ONN')          -- If only ONN
            -- WHERE 'ONN' IN ('VIZIO', 'ONN') -- If both ONN and VIZIO
              AND o.create_timestamp >= date_add(month, -1, date_trunc('month', current_date()))
              AND o.total_duration > 0
        )
        GROUP BY 1, 2, 3, 4
    ) tv_input_stats
    INNER JOIN prod.detection.tv 
        ON tv.tvid = tv_input_stats.fk_tvid
        AND tv.oem in ('VIZIO', 'TVIS', 'NOVATEK')           -- If VIZIO only
        -- AND tv.oem in ('ONN')                             -- If ONN only
        -- AND tv.oem in ('VIZIO', 'TVIS', 'NOVATEK', 'ONN') -- If with ONN and VIZIO
    INNER JOIN prod.detection.tv_geolocation
        ON tv_input_stats.fk_tvid = tv_geolocation.fk_tvid
        AND tv_input_stats.create_timestamp BETWEEN tv_geolocation.create_timestamp AND tv_geolocation.next_create_timestamp
    INNER JOIN prod.detection.location
        ON fk_location_id = location_id
    INNER JOIN prod.detection.dma
        ON fk_dma_id = dma_id
    INNER JOIN prod.detection.tv_ip_address
        ON tv_ip_address.fk_tvid = tv_input_stats.fk_tvid
        AND tv_input_stats.create_timestamp BETWEEN tv_ip_address.create_timestamp AND tv_ip_address.next_create_timestamp
        AND tv_input_stats.create_timestamp >= date_add(month, -1, date_trunc('month', current_date()))
    GROUP BY 1, 2, population
)
ORDER BY month DESC, number_hd_tvs DESC;