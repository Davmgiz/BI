create schema if not exists raw;

create table if not exists raw.events (
  event_id       bigserial primary key,
  event_time     timestamptz not null,
  user_id        bigint,
  session_id     text,
  event_name     text,                 -- view, click, add_to_cart, purchase, signup
  item_id        bigint,
  price          numeric(12,2),
  currency       text default 'RUB',
  platform       text,                 -- web, ios, android
  region         text,                 -- msk, spb, kzn, nsk, ekb
  source         text,                 -- organic, ads, referral
  experiment_id  text,                 -- optional
  payload        jsonb,                -- optional: любые доп поля
  ingested_at    timestamptz not null default now()
);

create index if not exists ix_raw_events_time on raw.events(event_time);
create index if not exists ix_raw_events_user_time on raw.events(user_id, event_time);
create index if not exists ix_raw_events_name_time on raw.events(event_name, event_time);
