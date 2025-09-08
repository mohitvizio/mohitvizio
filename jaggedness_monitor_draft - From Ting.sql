-- Databricks notebook source
-- Prod
-- viewing_content_firehose
WITH dt AS (
  SELECT '2024-11-04')
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
SELECT tm.minute_start
, chipset
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
, COUNT(DISTINCT vc.fk_tvid)/MEDIAN(COUNT(DISTINCT vc.fk_tvid)) OVER (PARTITION BY chipset) AS normalized_tvs
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 HOUR
  AND chipset in ('5583', '5691', '5597', '5581p')
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

-- Stage
-- viewing_content_firehose
WITH dt AS (
  SELECT '2024-11-04')
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
SELECT tm.minute_start
, chipset
, COUNT(DISTINCT vc.fk_tvid) AS total_tvs
, COUNT(DISTINCT vc.fk_tvid)/MEDIAN(COUNT(DISTINCT vc.fk_tvid)) OVER (PARTITION BY chipset) AS normalized_tvs
FROM tm 
LEFT JOIN stage.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN stage.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 HOUR
  AND chipset in ('5583', '5691', '5597', '5581p')
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

-- Prod
-- vizio_content_firehose
WITH dt AS (
  SELECT '2024-11-04')
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
SELECT tm.minute_start
, chipset
, COUNT(DISTINCT tv.token) AS total_tvs
, COUNT(DISTINCT tv.token)/MEDIAN(COUNT(DISTINCT tv.token)) OVER (PARTITION BY chipset) AS normalized_tvs
FROM tm 
LEFT JOIN prod.staging.vizio_content_firehose vc
  ON GREATEST(vc.ts_start, tm.minute_start) < LEAST(tm.minute_stop, vc.ts_end)
LEFT JOIN prod.detection.tv
ON vc.tvid = tv.tvid
WHERE vc.ts_start >= (SELECT * FROM dt)
  AND vc.ts_end < (SELECT * FROM dt) + interval 4 HOUR
  AND chipset in ('5583', '5691', '5597', '5581p')
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

-- Stage
-- vizio_content_firehose
WITH dt AS (
  SELECT '2024-11-04')
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
SELECT tm.minute_start
, chipset
, COUNT(DISTINCT tv.token) AS total_tvs
, COUNT(DISTINCT tv.token)/MEDIAN(COUNT(DISTINCT tv.token)) OVER (PARTITION BY chipset) AS normalized_tvs
FROM tm 
LEFT JOIN stage.staging.vizio_content_firehose vc
  ON GREATEST(vc.ts_start, tm.minute_start) < LEAST(tm.minute_stop, vc.ts_end)
LEFT JOIN stage.detection.tv
ON vc.tvid = tv.tvid
WHERE vc.ts_start >= (SELECT * FROM dt)
  AND vc.ts_end < (SELECT * FROM dt) + interval 4 HOUR
  AND chipset in ('5583', '5691', '5597', '5581p')
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

WITH dt AS (
  SELECT '2024-11-04')
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
  SELECT tm.minute_start
        , chipset
        , COUNT(DISTINCT tv.token) AS total_tvs
        , COUNT(DISTINCT tv.token)/MEDIAN(COUNT(DISTINCT tv.token)) OVER (PARTITION BY chipset) AS normalized_tvs
FROM tm 
LEFT JOIN prod.staging.vizio_content_firehose vc
  ON GREATEST(vc.ts_start, tm.minute_start) < LEAST(tm.minute_stop, vc.ts_end)
LEFT JOIN prod.detection.tv
ON vc.tvid = tv.tvid
LEFT JOIN prod.detection.tv_settings_latest_daily ts
ON vc.tvid = ts.tvid
WHERE vc.ts_start >= (SELECT * FROM dt)
  AND vc.ts_end < (SELECT * FROM dt) + interval 4 HOUR
GROUP BY 1, 2
HAVING total_tvs > 1000
ORDER BY 1, 2

-- COMMAND ----------

WITH dt AS (
  SELECT '2024-11-04')
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
,
data as (
  SELECT tm.minute_start
          , tv.token
          , case when chipset in ('5581p', '5597') then concat('2019_', chipset) 
                  when chipset in ('5583', '5691') then concat('2020_', chipset) 
                  else 'Other'
            end as year_chipset
  FROM tm 
  LEFT JOIN prod.staging.vizio_content_firehose vc
    ON GREATEST(vc.ts_start, tm.minute_start) < LEAST(tm.minute_stop, vc.ts_end)
  LEFT JOIN prod.detection.tv
  ON vc.tvid = tv.tvid
  WHERE vc.ts_start >= (SELECT * FROM dt)
    AND vc.ts_end < (SELECT * FROM dt) + interval 4 HOUR
    AND chipset in ('5583', '5691', '5597', '5581p')
)
select minute_start
        , year_chipset
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY year_chipset) AS normalized_tvs
from data
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

WITH dt AS (
  SELECT '2024-11-04')
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
,
data as (
  SELECT tm.minute_start
          , tv.token
          , case when xml.tvid is not null then concat(tv.chipset, '_platform_XML') 
                  else concat(tv.chipset, '_client_XML') 
            end as chipset_xml
  FROM tm 
  LEFT JOIN prod.staging.vizio_content_firehose vc
    ON GREATEST(vc.ts_start, tm.minute_start) < LEAST(tm.minute_stop, vc.ts_end)
  LEFT JOIN prod.detection.tv
  ON vc.tvid = tv.tvid
  LEFT JOIN dev.ltao.xml_rollout_tv_30a_latest xml
  ON vc.tvid = xml.tvid
  AND rand > 0
  AND rand < 0.05
  AND client_version_string >= '3.5.1053'
  WHERE vc.ts_start >= (SELECT * FROM dt)
    AND vc.ts_end < (SELECT * FROM dt) + interval 4 HOUR
    AND tv.chipset in ('5583', '5691')
)
select minute_start
        , chipset_xml
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY chipset_xml) AS normalized_tvs
from data
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

select tvs.always_fp,
       chipset,
       count(distinct tv.tvid) as total_tvs
from prod.detection.viewing_content_firehose vc
join prod.detection.tv
on vc.fk_tvid = tv.tvid
join prod.detection.tv_settings_latest_daily tvs
on tv.tvid = tvs.tvid
and tvs.disabled = 0
where tv.chipset in ('5695', '5695S')
and vc.session_start >= curdate()
group by 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-04'
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 1 DAY
)
SELECT tm.minute_start
      , COUNT(DISTINCT CASE WHEN vc.fk_content_id != 3468026 THEN tv.token ELSE NULL END) AS detected_tvs
      , COUNT(DISTINCT tv.token) AS total_tvs
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
LEFT JOIN prod.detection.tv_settings_latest_daily ts
ON vc.fk_tvid = ts.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 1 DAY
  and chipset in ('5583', '5691', '5695', '5695S')
  and vc.fk_zoo_id = 17
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-04'
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 1 DAY
)
, new_log as (
SELECT tm.minute_start
      , COUNT(DISTINCT CASE WHEN vc.fk_content_id != 3468026 THEN tv.token ELSE NULL END) AS new_detected_tvs
      , COUNT(DISTINCT tv.token) AS new_total_tvs
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
JOIN prod.detection.tv
on vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 hours
  and vc.fk_zoo_id = 17
GROUP BY 1
)
, old_log as (
SELECT tm.minute_start
      , COUNT(DISTINCT CASE WHEN vc.fk_content_id != 3468026 THEN tv.token ELSE NULL END) AS old_detected_tvs
      , COUNT(DISTINCT tv.token) AS old_total_tvs
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON session_start <= minute_start and minute_start <= session_end
JOIN prod.detection.tv
on vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 hours
  and vc.fk_zoo_id = 17
GROUP BY 1
)
select *
from new_log join old_log using(minute_start)

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-04'
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 1 DAY
)
SELECT tm.minute_start
      , COUNT(DISTINCT CASE WHEN vc.fk_content_id != 3468026 THEN tv.token ELSE NULL END) AS detected_tvs
      , COUNT(DISTINCT tv.token) AS total_tvs
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
LEFT JOIN prod.detection.tv_settings_latest_daily ts
ON vc.fk_tvid = ts.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 1 DAY
  and chipset not in ('5583', '5691', '5695', '5695S')
  and vc.fk_zoo_id = 17
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-04'
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 1 DAY
)
, raw as (
SELECT tm.minute_start
      , CASE WHEN chipset in ('5583', '5691', '5695', '5695S') THEN 'Niffler' ELSE 'Non Niffler' END AS niffler
      , tv.token
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
LEFT JOIN prod.detection.tv_settings_latest_daily ts
ON vc.fk_tvid = ts.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 1 DAY
  and vc.fk_zoo_id = 17
)
select minute_start
       , niffler
      , COUNT(DISTINCT token) AS total_tvs
from raw
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-04'
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
, raw as (
SELECT tm.minute_start
        , tv.token
        , ins.input_source as input_category
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
LEFT JOIN prod.detection.input_source ins
ON vc.fk_input_source_id = ins.input_source_id
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 HOUR
  -- AND chipset not in ('SIGMA_SX6', 'SIGMA_SX7C', '5583', '5691', '5695S', '5695')
  AND chipset in ('5583', '5691', '5695S', '5695')
)
select minute_start
        , input_category
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY input_category) AS normalized_tvs
from raw
GROUP BY 1, 2
HAVING total_tvs >= 1000
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-04'
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
, raw as (
SELECT tm.minute_start
        , tv.token
        , case when vc.fk_input_source_id in (47, 32, 34425, 44, 48) then 'APP' else 'NON APP' end as input_category
        , case when chipset in ('5691', '5695', '5695S', '5583') then concat('Niffler_', chipset) 
               else concat('Non_Niffler_', chipset)
          end as niffler_chipset
        , concat(niffler_chipset, '_', input_category) as category
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 HOUR
)
select minute_start
        , category
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY category) AS normalized_tvs
from raw
GROUP BY 1, 2
HAVING total_tvs >= 1000
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-04'
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
, raw as (
SELECT tm.minute_start
        , tv.token
        , case when chipset in ('5691', '5695', '5695S', '5583', 
                                'SIGMA_SX6', 'SIGMA_SX7C') 
                    and vc.fk_input_source_id not in (47, 32, 34425, 44, 48)
                    then 'Jagged'
               else 'Smooth'
          end as  category
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 HOUR
)
select minute_start
        , category
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY category) AS normalized_tvs
from raw
GROUP BY 1, 2
HAVING total_tvs >= 1000
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-04'
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
, raw as (
SELECT tm.minute_start
        , tv.token
        , case when chipset in ('5691', '5695', '5695S', '5583', 
                                'SIGMA_SX6', 'SIGMA_SX7C') 
                    and vc.fk_input_source_id not in (47, 32, 34425, 44, 48)
                    then 'Jagged'
               else 'Smooth'
          end as  category
FROM tm 
LEFT JOIN stage.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN stage.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 HOUR
)
select minute_start
        , category
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY category) AS normalized_tvs
from raw
GROUP BY 1, 2
HAVING total_tvs >= 1000
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-04'
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT * FROM dt)
    AND minute_start <= (SELECT * FROM dt) + interval 4 HOUR
)
, raw_stage as (
SELECT tm.minute_start
        , tv.token
        , case when metadata_acr_origin = 2 then 'Client XML'
               else 'Platform XML'
          end as  category
FROM stage.detection.viewing_content_firehose vc
LEFT JOIN stage.detection.tv_settings ts
ON vc.fk_tvid = ts.fk_tvid
and vc.session_start >= ts.create_timestamp
and vc.session_start < ts.next_create_timestamp
LEFT JOIN stage.detection.settings 
on ts.fk_settings_id = settings.settings_id
JOIN tm
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN stage.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 HOUR
  and chipset in ('5691', '5695', '5695S', '5583')
)
, raw_prod as (
SELECT tm.minute_start
        , tv.token
        , case when metadata_acr_origin = 2 then 'Client XML'
               else 'Platform XML'
          end as  category
FROM prod.detection.viewing_content_firehose vc
LEFT JOIN prod.detection.tv_settings ts
ON vc.fk_tvid = ts.fk_tvid
and vc.session_start >= ts.create_timestamp
and vc.session_start < ts.next_create_timestamp
LEFT JOIN prod.detection.settings 
on ts.fk_settings_id = settings.settings_id
JOIN tm
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT * FROM dt)
  AND vc.session_end < (SELECT * FROM dt) + interval 4 HOUR
  and chipset in ('5691', '5695', '5695S', '5583')
),
comb as (
select minute_start
        , category
        , 'Stage' as zoo
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY category) AS normalized_tvs
from raw_stage
GROUP BY 1, 2, 3
HAVING total_tvs >= 1000
UNION
select minute_start
        , category
        , 'Prod' as zoo
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY category) AS normalized_tvs
from raw_prod
GROUP BY 1, 2, 3
HAVING total_tvs >= 1000
)
select *
       , concat(zoo, '_', category) as zoo_category
       , case when zoo_category = 'Stage_Client XML' then 0
              when zoo_category = 'Prod_Client XML' then 1
              when zoo_category = 'Prod_Platform XML' then 2
              else 3
         end as zoo_category_order
       , normalized_tvs + zoo_category_order*0.02 as normalized_tvs_adjusted
from comb

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-10-29' as date_start
         , '2024-11-05' as date_end
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT date_start FROM dt)
    AND minute_start < (SELECT date_end FROM dt)
    AND DATE_PART('hour', minute_start) < 4
)
, raw as (
SELECT date_trunc('day', minute_start) as ts_date
       , datediff(minute, ts_date, minute_start) as ts_minute
       , tv.token
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT date_start FROM dt)
  AND vc.session_end < (SELECT date_end FROM dt)
  AND DATE_PART('hour', vc.session_start) < 4
  AND chipset in ('5691', '5695', '5695S', '5583', 'SIGMA_SX6', 'SIGMA_SX7C') 
  AND vc.fk_input_source_id not in (47, 32, 34425, 44, 48)
)
select  ts_minute
        , ts_date
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY ts_date) AS normalized_tvs
from raw
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-02' as date_start
         , '2024-11-07' as date_end
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT date_start FROM dt)
    AND minute_start < (SELECT date_end FROM dt)
    AND DATE_PART('hour', minute_start) < 4
)
, raw as (
SELECT date_trunc('day', minute_start) as ts_date
       , datediff(minute, ts_date, minute_start) as ts_minute
       , tv.token
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT date_start FROM dt)
  AND vc.session_end < (SELECT date_end FROM dt)
  AND DATE_PART('hour', vc.session_start) < 4
  -- AND chipset in ('5691', '5695', '5695S', '5583', 'SIGMA_SX6', 'SIGMA_SX7C') 
  -- AND vc.fk_input_source_id not in (47, 32, 34425, 44, 48)
)
select  ts_minute
        , ts_date
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY ts_date) AS normalized_tvs
from raw
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-10-29' as date_start
         , '2024-11-05' as date_end
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT date_start FROM dt)
    AND minute_start < (SELECT date_end FROM dt)
    -- AND DATE_PART('hour', minute_start) < 4
)
, raw as (
SELECT date_trunc('day', minute_start) as ts_date
       , datediff(minute, ts_date, minute_start) as ts_minute
       , tv.token
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT date_start FROM dt)
  AND vc.session_end < (SELECT date_end FROM dt)
  -- AND DATE_PART('hour', vc.session_start) < 4
  AND chipset in ('5691', '5695', '5695S', '5583', 'SIGMA_SX6', 'SIGMA_SX7C') 
  AND vc.fk_input_source_id not in (47, 32, 34425, 44, 48)
)
select  ts_minute
        , ts_date
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY ts_date) AS normalized_tvs
from raw
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-11-05' as date_start
         , '2024-11-07' as date_end
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT date_start FROM dt)
    AND minute_start < (SELECT date_end FROM dt)
    -- AND DATE_PART('hour', minute_start) < 4
)
, raw as (
SELECT date_trunc('day', minute_start) as ts_date
       , datediff(minute, ts_date, minute_start) as ts_minute
       , tv.token
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT date_start FROM dt)
  AND vc.session_end < (SELECT date_end FROM dt)
  -- AND DATE_PART('hour', vc.session_start) < 4
  -- AND chipset in ('5691', '5695', '5695S', '5583', 'SIGMA_SX6', 'SIGMA_SX7C') 
  -- AND vc.fk_input_source_id not in (47, 32, 34425, 44, 48)
)
select  ts_minute
        , ts_date
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY ts_date) AS normalized_tvs
from raw
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-10-29' as date_start
         , '2024-11-05' as date_end
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT date_start FROM dt)
    AND minute_start < (SELECT date_end FROM dt)
    -- AND DATE_PART('hour', minute_start) < 4
)
, raw as (
SELECT date_trunc('day', minute_start) as ts_date
       , datediff(minute, ts_date, minute_start) as ts_minute
       , tv.token
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT date_start FROM dt)
  AND vc.session_end < (SELECT date_end FROM dt)
  -- AND DATE_PART('hour', vc.session_start) < 4
  AND chipset in ('5691', '5695', '5695S', '5583', 'SIGMA_SX6', 'SIGMA_SX7C') 
  AND vc.fk_input_source_id not in (47, 32, 34425, 44, 48)
)
select  ts_minute
        , ts_date
        , COUNT(DISTINCT token) AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY ts_date) AS normalized_tvs
from raw
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------

WITH 
dt AS (
  SELECT '2024-10-07' as date_start
         , '2024-11-05' as date_end
  )
,
tm AS (
  SELECT *
  FROM detection.time_minute
  WHERE minute_start >= (SELECT date_start FROM dt)
    AND minute_start < (SELECT date_end FROM dt)
    AND DATE_PART('hour', minute_start) < 4
    AND dayofweek(minute_start) = 2
)
, raw as (
SELECT date_trunc('day', minute_start) as ts_date
       , datediff(minute, ts_date, minute_start) as ts_minute
       , tv.token
FROM tm 
LEFT JOIN prod.detection.viewing_content_firehose vc
  ON GREATEST(vc.session_start, tm.minute_start) < LEAST(tm.minute_stop, vc.session_end)
LEFT JOIN prod.detection.tv
ON vc.fk_tvid = tv.tvid
WHERE vc.session_start >= (SELECT date_start FROM dt)
  AND vc.session_end < (SELECT date_end FROM dt)
  AND DATE_PART('hour', vc.session_start) < 4
  AND dayofweek(vc.session_start) = 2
  AND chipset in ('5691', '5695', '5695S', '5583', 'SIGMA_SX6', 'SIGMA_SX7C') 
  AND vc.fk_input_source_id not in (47, 32, 34425, 44, 48)
)
select  ts_minute
        , ts_date
        , COUNT(DISTINCT token) + datediff(week, ts_date, (SELECT date_end FROM dt))*100000 AS total_tvs
        , COUNT(DISTINCT token)/MEDIAN(COUNT(DISTINCT token)) OVER (PARTITION BY ts_date) AS normalized_tvs
from raw
GROUP BY 1, 2
ORDER BY 1, 2

-- COMMAND ----------


