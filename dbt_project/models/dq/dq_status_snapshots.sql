{{ config(
    materialized='incremental',
    schema='dq',
    alias='status_snapshots',
    unique_key='check_ts'
) }}

with checks as (
  select
    now() as check_ts,

    -- freshness: события за последние 10 минут
    (select count(*) from {{ ref('stg_events') }}
      where event_time >= now() - interval '10 minutes') as events_last_10m,

    -- invalid event_name
    (select count(*) from {{ ref('stg_events') }}
      where event_name not in ('view','click','add_to_cart','purchase','signup')) as invalid_event_name_cnt,

    -- purchase without price
    (select count(*) from {{ ref('stg_events') }}
      where event_name='purchase' and price is null) as purchase_without_price_cnt,

    -- negative price
    (select count(*) from {{ ref('stg_events') }}
      where price is not null and price < 0) as negative_price_cnt
),
final as (
  select
    check_ts,

    events_last_10m,
    invalid_event_name_cnt,
    purchase_without_price_cnt,
    negative_price_cnt,

    -- “светофор”
    case
      when events_last_10m = 0 then false
      when invalid_event_name_cnt > 0 then false
      when negative_price_cnt > 0 then false
      else true
    end as overall_pass
  from checks
)

select * from final

{% if is_incremental() %}
  -- чтобы не вставлять слишком часто одинаковые метки, можно оставить как есть
{% endif %}
