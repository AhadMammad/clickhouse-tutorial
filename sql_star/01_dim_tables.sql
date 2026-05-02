-- =============================================================================
-- ClickHouse Star Schema — Dimension Tables
-- Mobile App Interaction Analytics
-- =============================================================================
--
-- Design decisions:
--   - ReplacingMergeTree(ver): SCD Type 1 (overwrite) via version column
--   - ver UInt32: set to toUnixTimestamp(now()) on each ETL load
--   - Surrogate keys mirror PostgreSQL sequence IDs (no hash collision risk)
--   - date_key: YYYYMMDD integer format (e.g., 20260429) — sortable, human-readable
--   - dim_session: prevents repeating session attrs for every event row in fact
-- =============================================================================

-- User tier dimension (mirrors PostgreSQL user_tiers lookup)
CREATE TABLE IF NOT EXISTS dim_user_tier (
    tier_key     UInt32,
    tier_name    LowCardinality(String),
    max_monthly_events Int32,  -- -1 = unlimited
    description  String,
    ver          UInt32
) ENGINE = ReplacingMergeTree(ver)
ORDER BY (tier_key);

-- User dimension
CREATE TABLE IF NOT EXISTS dim_user (
    user_key         UInt64,
    external_user_id String,
    username         LowCardinality(String),
    tier_key         UInt32,
    registered_at    DateTime,
    ver              UInt32
) ENGINE = ReplacingMergeTree(ver)
ORDER BY (user_key);

-- Device dimension
CREATE TABLE IF NOT EXISTS dim_device (
    device_key        UInt64,
    device_fingerprint String,
    device_type        LowCardinality(String),  -- mobile, tablet
    os_name            LowCardinality(String),  -- iOS, Android
    os_version         LowCardinality(String),
    device_model       LowCardinality(String),
    screen_resolution  LowCardinality(String),
    ver                UInt32
) ENGINE = ReplacingMergeTree(ver)
ORDER BY (device_key);

-- Screen dimension
CREATE TABLE IF NOT EXISTS dim_screen (
    screen_key      UInt32,
    screen_name     LowCardinality(String),
    screen_category LowCardinality(String),  -- navigation, commerce, account, support, auth
    ver             UInt32
) ENGINE = ReplacingMergeTree(ver)
ORDER BY (screen_key);

-- Event type dimension
CREATE TABLE IF NOT EXISTS dim_event_type (
    event_type_key  UInt32,
    event_name      LowCardinality(String),
    event_category  LowCardinality(String),  -- navigation, interaction, system, commerce, account
    ver             UInt32
) ENGINE = ReplacingMergeTree(ver)
ORDER BY (event_type_key);

-- App version dimension
CREATE TABLE IF NOT EXISTS dim_app_version (
    version_key     UInt32,
    version_code    LowCardinality(String),
    platform        LowCardinality(String),  -- ios, android
    release_date    Date,
    is_force_update UInt8,
    ver             UInt32
) ENGINE = ReplacingMergeTree(ver)
ORDER BY (version_key);

-- Session dimension: captures session-level attributes once, not per-event
CREATE TABLE IF NOT EXISTS dim_session (
    session_key      UInt64,  -- MD5(session_id UUID) % 2^63, deterministic
    session_id       String,
    user_key         UInt64,
    device_key       UInt64,
    version_key      UInt32,
    session_start    DateTime,
    session_end      Nullable(DateTime),
    duration_seconds Nullable(UInt32),  -- EXTRACT(EPOCH FROM session_end - session_start)
    country_code     LowCardinality(String),
    ver              UInt32
) ENGINE = ReplacingMergeTree(ver)
ORDER BY (session_key);

-- Date dimension: date_key = YYYYMMDD (e.g., 20260429)
-- Populated by ETL for the full date range of loaded data
CREATE TABLE IF NOT EXISTS dim_date (
    date_key     UInt32,  -- YYYYMMDD format: 20260429
    full_date    Date,
    year         UInt16,
    quarter      UInt8,
    month        UInt8,
    week_of_year UInt8,
    day_of_month UInt8,
    day_of_week  UInt8,  -- 1=Mon, 7=Sun (ISO 8601)
    is_weekend   UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY (date_key);
