-- Databricks notebook source
UPDATE dev.mohit_gangwani.metrics_population_by_dma
SET next_create_timestamp = current_date
WHERE next_create_timestamp = '2100-01-01 00:00:00';


INSERT INTO dev.mohit_gangwani.metrics_population_by_dma
with opted_in_dma as (
  select distinct a.fk_tvid, d.dma_name, d.fk_dma_id, e.population
  from detection.tv_populations a
  inner join detection.populations b
    on a.fk_population_id = b.population_id
  left join detection.tv_geolocation_latest_daily d --some records in tv_populations may not have a record in tv_geolocation_latest_daily
    on a.fk_tvid = d.tvid
  left join detection.dma e
    on d.fk_dma_id = e.dma_id
  where b.population_name = 'opted_in'
),

one_year_active as (
  select dma_name,
    fk_dma_id,
    population, 
    count(distinct fk_tvid) as total, 
    count(distinct fk_tvid)::real / (population * 1.2) as p
  from opted_in_dma a
  inner join detection.tv_activity_latest b
    on a.fk_tvid = b.tvid
  where b.session_start >= CURRENT_DATE - INTERVAL '365 DAYS'
  group by 1,2,3
),

thirty_day_active as (
  select dma_name,
    fk_dma_id,
    count(distinct fk_tvid) as total
  from opted_in_dma a
  inner join detection.tv_activity_latest b
    on a.fk_tvid = b.tvid
  where b.session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
  group by 1,2
),

thirty_day_detecting as (
  select dma_name,
    fk_dma_id,
    count(distinct a.fk_tvid) as total
  from opted_in_dma a
  inner join detection.tv_zoo_latest_daily b
    on a.fk_tvid = b.tvid
  inner join (
    select fk_tvid
    from detection.viewing_content_firehose
    where fk_content_id != 3468026
      and session_start >= CURRENT_DATE - INTERVAL '30 DAYS'
    ) c on a.fk_tvid = c.fk_tvid  
  where zoo = 'control-zoo-dtsprod.tvinteractive.tv'
  group by 1,2
)

select a.dma_name,
  a.population as US_tv_households, 
  a.total as one_year_active_opted_in, 
  b.total as thirty_day_active_opted_in, 
  c.total as thirty_day_detecting_opted_in,
  a.p as percent_penetration,
  current_date as create_timestamp,
  '2100-01-01 00:00:00' as next_create_timestamp
from one_year_active a
inner join thirty_day_active b
  on a.fk_dma_id = b.fk_dma_id
    or (a.fk_dma_id is null and b.fk_dma_id is null)
inner join thirty_day_detecting c
  on a.fk_dma_id = c.fk_dma_id
    or (a.fk_dma_id is null and c.fk_dma_id is null)
