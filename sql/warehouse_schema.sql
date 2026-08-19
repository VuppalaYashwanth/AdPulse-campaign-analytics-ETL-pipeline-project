-- ============================================================
-- AdPulse: warehouse schema (star schema)
-- Grain of fact_campaign_performance: one row per
-- (campaign, channel, date, geography)
-- ============================================================

create schema if not exists warehouse;

-- ---------------------------
-- Dimensions
-- ---------------------------

drop table if exists warehouse.dim_date cascade;
create table warehouse.dim_date (
    date_key      integer primary key,       -- YYYYMMDD
    full_date     date not null unique,
    day_of_week   text not null,
    day_num       integer not null,
    month         integer not null,
    month_name    text not null,
    quarter       integer not null,
    year          integer not null,
    is_weekend    boolean not null
);

drop table if exists warehouse.dim_campaign cascade;
create table warehouse.dim_campaign (
    campaign_key   bigserial primary key,
    campaign_id    text not null unique,
    campaign_name  text not null,
    objective      text,
    start_date     date,
    end_date       date,
    updated_at     timestamp default now()
);

drop table if exists warehouse.dim_channel cascade;
create table warehouse.dim_channel (
    channel_key    bigserial primary key,
    channel_name   text not null unique,
    channel_type   text   -- e.g. paid_search, paid_social, display, email
);

drop table if exists warehouse.dim_geography cascade;
create table warehouse.dim_geography (
    geo_key        bigserial primary key,
    country        text not null,
    region         text,
    city           text,
    unique (country, region, city)
);

-- ---------------------------
-- Fact table
-- ---------------------------

drop table if exists warehouse.fact_campaign_performance cascade;
create table warehouse.fact_campaign_performance (
    fact_key       bigserial primary key,
    date_key       integer not null references warehouse.dim_date(date_key),
    campaign_key   bigint  not null references warehouse.dim_campaign(campaign_key),
    channel_key    bigint  not null references warehouse.dim_channel(channel_key),
    geo_key        bigint  not null references warehouse.dim_geography(geo_key),

    impressions    bigint  not null default 0,
    clicks         bigint  not null default 0,
    spend          numeric(12,2) not null default 0,
    conversions    integer not null default 0,
    revenue        numeric(12,2) not null default 0,

    -- derived metrics, computed once at load time
    ctr            numeric(8,4),   -- clicks / impressions
    cpc            numeric(10,4),  -- spend / clicks
    cpa            numeric(10,4),  -- spend / conversions
    roas           numeric(10,4),  -- revenue / spend

    load_ts        timestamp default now(),

    unique (date_key, campaign_key, channel_key, geo_key)
);

create index if not exists idx_fact_date on warehouse.fact_campaign_performance(date_key);
create index if not exists idx_fact_campaign on warehouse.fact_campaign_performance(campaign_key);
create index if not exists idx_fact_channel on warehouse.fact_campaign_performance(channel_key);
create index if not exists idx_fact_geo on warehouse.fact_campaign_performance(geo_key);
