{{ config(materialized='view', schema='stg') }}

with base as (
  select
    event_time,
    (event_time at time zone 'utc')::date as event_date,

    user_id,

    nullif(trim(session_id), '') as session_id,
    lower(trim(event_name)) as event_name,

    item_id,

    case
      when price is not null and price >= 0 then price
      else null
    end as price,

    coalesce(nullif(lower(trim(platform)), ''), 'unknown') as platform,
    coalesce(nullif(lower(trim(region)), ''), 'unknown') as region,
    coalesce(nullif(lower(trim(source)), ''), 'unknown') as source,

    ingested_at
  from {{ source('raw', 'events') }}
  where event_time is not null
    and user_id is not null
    and event_name is not null
),
dedup as (
  select
    *,
    row_number() over (
      partition by
        user_id,
        coalesce(session_id, 'no_session'),
        event_time,
        event_name,
        coalesce(item_id, -1)
      order by ingested_at asc
    ) as rn
  from base
)
select
  event_time,
  event_date,
  user_id,
  session_id,
  event_name,
  item_id,
  price,
  platform,
  region,
  source,
  ingested_at
from dedup
where rn = 1
