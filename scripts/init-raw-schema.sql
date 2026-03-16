-- Create raw schema and tables for the fintech data pipeline

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.users (
    user_id             TEXT PRIMARY KEY,
    created_at          TIMESTAMPTZ,
    country             TEXT,
    acquisition_channel TEXT,
    kyc_status          TEXT,
    device_type         TEXT,
    source_file         TEXT,
    ingested_at         TIMESTAMPTZ,
    batch_id            TEXT
);

CREATE TABLE IF NOT EXISTS raw.merchants (
    merchant_id   TEXT PRIMARY KEY,
    merchant_name TEXT,
    category      TEXT,
    country       TEXT,
    created_at    TIMESTAMPTZ,
    source_file   TEXT,
    ingested_at   TIMESTAMPTZ,
    batch_id      TEXT
);

CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id     TEXT PRIMARY KEY,
    user_id        TEXT,
    merchant_id    TEXT,
    amount         NUMERIC,
    currency       TEXT,
    payment_method TEXT,
    gateway        TEXT,
    status         TEXT,
    created_at     TIMESTAMPTZ,
    source_file    TEXT,
    ingested_at    TIMESTAMPTZ,
    batch_id       TEXT
);

CREATE TABLE IF NOT EXISTS raw.product_events (
    event_id        TEXT PRIMARY KEY,
    event_name      TEXT,
    event_timestamp TIMESTAMPTZ,
    user_id         TEXT,
    payment_id      TEXT,
    session_id      TEXT,
    platform        TEXT,
    country         TEXT,
    source_file     TEXT,
    ingested_at     TIMESTAMPTZ,
    batch_id        TEXT
);
