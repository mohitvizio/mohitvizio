DROP TABLE IF EXISTS prod.customer_reports.r1086_optout_disney_2025_08_25_07_production;
CREATE TABLE IF NOT EXISTS prod.customer_reports.r1086_optout_disney_2025_08_25_07_production (
    tvid string,
    hash string,
    ts string
    );
INSERT INTO prod.customer_reports.r1086_optout_disney_2025_08_25_07_production (tvid, hash, ts)
    SELECT
        COALESCE(tv.long_tvid, CAST(o.tvid AS CHAR(50))) as tvid,
        '',
        split(CAST(to_timestamp(o.ts_created , 'YYYY-MM-DD HH24:MI:SS')::timestamp AS STRING), '[.]')[0] AS ts
    FROM prod.public.optout_request_production o
    LEFT OUTER JOIN prod.detection.tv tv ON o.tvid = tv.tvid
    WHERE
        o.request_source = 1
    AND o.ts_created BETWEEN '2025-08-25T07:00:00'::timestamp AND '2025-08-26T07:00:00'::timestamp
    AND tv.oem in ('VIZIO', 'TVIS', 'NOVATEK')           -- If VIZIO only
    -- AND tv.oem in ('ONN')                             -- If ONN only
    -- AND tv.oem in ('VIZIO', 'TVIS', 'NOVATEK', 'ONN') -- If with ONN and VIZIO
;