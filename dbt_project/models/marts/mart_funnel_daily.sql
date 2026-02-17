{{ config(materialized='table', schema='mart') }}

with params as (
  select (now() at time zone 'utc')::date as today, 30::int as days_back
),
agg as (
  select
    e.event_date as dt,
    count(distinct e.user_id) filter (where e.event_name = 'view') as users_view,
    count(distinct e.user_id) filter (where e.event_name = 'click') as users_click,
    count(distinct e.user_id) filter (where e.event_name = 'add_to_cart') as users_cart,
    count(distinct e.user_id) filter (where e.event_name = 'purchase') as users_purchase
  from {{ ref('stg_events') }} e
  join params p on e.event_date >= (p.today - (p.days_back || ' days')::interval)::date
  group by 1
)
select
  dt,
  users_view,
  users_click,
  users_cart,
  users_purchase,
  case when users_view > 0 then users_click::numeric / users_view::numeric else 0 end::numeric(10,6) as view_to_click,
  case when users_click > 0 then users_cart::numeric / users_click::numeric else 0 end::numeric(10,6) as click_to_cart,
  case when users_cart > 0 then users_purchase::numeric / users_cart::numeric else 0 end::numeric(10,6) as cart_to_purchase,
  now() as updated_at
from agg
