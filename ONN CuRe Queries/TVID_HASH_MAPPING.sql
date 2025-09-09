DROP TABLE IF EXISTS prod.customer_reports.r418_tvid_hash_mapping_experian_2025_07_01_07_production;
CREATE TABLE IF NOT EXISTS prod.customer_reports.r418_tvid_hash_mapping_experian_2025_07_01_07_production (
    tvid string, hash_experian string, hash_geniusdigital string
    );
INSERT INTO prod.customer_reports.r418_tvid_hash_mapping_experian_2025_07_01_07_production (
    tvid, hash_experian, hash_geniusdigital
)
WITH tvs AS (
    SELECT
        DISTINCT COALESCE(tv.long_tvid, CAST(u.fk_tvid AS CHAR(50))) as tvid
    FROM prod.detection.tv_populations u
    LEFT JOIN prod.detection.tv tv on u.fk_tvid=tv.tvid
    WHERE u.fk_population_id = 304
    AND tv.oem in ('VIZIO', 'TVIS', 'NOVATEK')           -- If VIZIO only
    -- AND tv.oem in ('ONN')                             -- If ONN only
    -- AND tv.oem in ('VIZIO', 'TVIS', 'NOVATEK', 'ONN') -- If with ONN and VIZIO

)
SELECT DISTINCT
    tv.tvid, hashes_production_experian.hash AS hash_experian, hashes_production_geniusdigital.hash AS hash_geniusdigital 
    FROM tvs tv
    JOIN prod.customer_reports.hashes_production_experian
    ON tv.tvid=customer_reports.hashes_production_experian.tvid
    JOIN prod.customer_reports.hashes_production_geniusdigital
    ON tv.tvid=customer_reports.hashes_production_geniusdigital.tvid