-- ============================================================
-- AdPulse: staging schema (raw landing zone)
-- No cleaning/typing enforcement here on purpose — this layer
-- mirrors the source data as-is so bad rows are visible and
-- auditable before transform.py touches them.
-- ============================================================

create schema if not exists staging;

-- Source 1: "CSV export" style feed (e.g. a search/social ads export)
drop table if exists staging.campaign_performance_csv;
create table staging.campaign_performance_csv (
    row_id            bigserial primary key,
    campaign_id       text,
    campaign_name     text,
    channel           text,
    report_date       text,        -- kept as text on purpose: raw layer
    country           text,
    region            text,
    city              text,
    impressions       text,
    clicks            text,
    spend             text,
    conversions       text,
    revenue           text,
    source_file       text,
    loaded_at         timestamp default now()
);

-- Source 2: "JSON API" style feed (e.g. a display/email platform feed)
drop table if exists staging.campaign_performance_json;
create table staging.campaign_performance_json (
    row_id            bigserial primary key,
    raw_payload       jsonb not null,
    source_file       text,
    loaded_at         timestamp default now()
);

-- Simple ingestion log so we can see what was loaded, when, and how many rows
drop table if exists staging.load_log;
create table staging.load_log (
    log_id            bigserial primary key,
    source_name       text not null,
    file_name         text,
    rows_loaded       integer,
    loaded_at         timestamp default now(),
    status            text,
    notes             text
);
