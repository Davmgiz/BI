{{ config(materialized='table', schema='mart') }}

with params as (
  select (now() at time zone 'utc')::date as today, 30::int as days_back
),
src as (
  select
    e.event_date as dt,
    count(distinct e.user_id) as dau,
    count(distinct e.session_id) filter (where e.session_id is not null) as sessions,
    count(*) filter (where e.event_name = 'purchase') as orders,
    coalesce(sum(e.price) filter (where e.event_name = 'purchase' and e.price is not null), 0)::numeric(14,2) as revenue
  from {{ ref('stg_events') }} e
  join params p on e.event_date >= (p.today - (p.days_back || ' days')::interval)::date
  group by 1
)
select
  dt,
  dau,
  sessions,
  orders,
  revenue,
  case
    when sessions > 0 then (orders::numeric / sessions::numeric)
    else 0
  end::numeric(10,6) as conversion_rate,
  now() as updated_at
from src
