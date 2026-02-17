{{ config(materialized='table', schema='dq', alias='daily') }}

with src as (
  select
    event_date as dt,
    count(*) as rows_total,

    count(*) filter (
      where event_name not in ('view','click','add_to_cart','purchase','signup')
    ) as invalid_event_name_cnt,

    count(*) filter (
      where event_name = 'purchase' and price is null
    ) as purchase_without_price_cnt,

    count(*) filter (
      where price is not null and price < 0
    ) as negative_price_cnt,

    count(*) filter (
      where platform is null or platform = 'unknown'
    ) as unknown_platform_cnt,

    count(*) filter (
      where region is null or region = 'unknown'
    ) as unknown_region_cnt,

    count(*) filter (
      where source is null or source = 'unknown'
    ) as unknown_source_cnt

  from {{ ref('stg_events') }}
  where event_date >= ((now() at time zone 'utc')::date - 30)
  group by 1
)

select
  dt,
  rows_total,
  invalid_event_name_cnt,
  purchase_without_price_cnt,
  negative_price_cnt,
  unknown_platform_cnt,
  unknown_region_cnt,
  unknown_source_cnt,

  case
    when invalid_event_name_cnt > 0 then false
    when negative_price_cnt > 0 then false
    else true
  end as day_pass,

  now() as updated_at
from src
order by dt
