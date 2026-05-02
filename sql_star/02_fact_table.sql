-- =============================================================================
-- ClickHouse Star Schema — Fact Table
-- Mobile App Interaction Analytics
-- =============================================================================
--
-- Grain: one row per event (finest granularity)
-- session_key → dim_session (avoids repeating session attrs per event)
-- platform removed from fact — query via dim_app_version JOIN
-- country_code kept as degenerate dimension (frequent filter, avoids JOIN overhead)
-- date_key = YYYYMMDD integer (self-documenting, JOIN to dim_date)
-- =============================================================================

CREATE TABLE IF NOT EXISTS fact_app_interactions (
    event_id        UInt64,
    session_key     UInt64,
    user_key        UInt64,
    device_key      UInt64,
    screen_key      UInt32,
    event_type_key  UInt32,
    version_key     UInt32,
    date_key        UInt32,         -- YYYYMMDD format: 20260429
    event_timestamp DateTime,
    sequence_number UInt32,
    duration_ms     UInt32,
    country_code    LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (user_key, event_timestamp, event_id)
PRIMARY KEY (user_key, event_timestamp);
