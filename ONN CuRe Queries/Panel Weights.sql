DROP TABLE IF EXISTS prod.customer_reports.r565_panelweights_discovery_2025_08_19_07_production;
CREATE TABLE IF NOT EXISTS prod.customer_reports.r565_panelweights_discovery_2025_08_19_07_production (
    tvid string, hash string, nrp_start_time string, nrp_weight double;
INSERT INTO prod.customer_reports.r565_panelweights_discovery_2025_08_19_07_production (
    tvid, 
    hash, 
    nrp_start_time, 
    nrp_weight
)
SELECT DISTINCT
    COALESCE(tv.long_tvid, tv.vizio_tvid) AS TVID, 
    '', 
    split(CAST(nrp.start_date AS STRING), '[.]')[0] as start_date, 
    nrp.full_projected_weight
    FROM prod.detection.panel_weights nrp -- Not sure about this. Need to check with Alina if there will be another schema for ONN.
    JOIN prod.detection.tv tv
    	ON nrp.fk_tvid = tv.tvid 
        AND tv.oem in ('VIZIO', 'TVIS', 'NOVATEK')           -- If VIZIO only
        -- AND tv.oem in ('ONN')                             -- If ONN only
        -- AND tv.oem in ('VIZIO', 'TVIS', 'NOVATEK', 'ONN') -- If with ONN and VIZIO
    WHERE nrp.start_date>= '2023-08-27T07:00:00'::timestamp 
    and nrp.start_date<= '2025-08-26T07:00:00'::timestamp;