-- =============================================================================
-- 07_ttl_and_compression.sql
-- ClickHouse Advanced - TTL Rules and Compression Codecs
-- =============================================================================
-- TTL (Time To Live) automates data lifecycle: tiering, aggregation, deletion.
-- Compression codecs optimize storage and query performance.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TTL (TIME TO LIVE) OVERVIEW
-- -----------------------------------------------------------------------------
-- TTL rules define what happens to data over time:
--   - DELETE: Remove old data automatically
--   - TO DISK/VOLUME: Move to slower/cheaper storage
--   - GROUP BY: Roll up old data into aggregates
--   - RECOMPRESS: Change compression for old data
--
-- TTL is evaluated during background merges, not immediately!
-- Use OPTIMIZE TABLE to force TTL evaluation if needed.
-- -----------------------------------------------------------------------------


-- Example table with comprehensive TTL rules
DROP TABLE IF EXISTS transactions_with_ttl;

CREATE TABLE transactions_with_ttl
(
    transaction_id UUID DEFAULT generateUUIDv4(),
    user_id UInt64,
    merchant_id UInt64,
    amount Decimal(18, 2),
    currency LowCardinality(String) DEFAULT 'USD',
    status Enum8(
        'pending' = 1,
        'processing' = 2,
        'completed' = 3,
        'failed' = 4,
        'refunded' = 5,
        'cancelled' = 6
    ),
    category LowCardinality(String),
    created_at DateTime DEFAULT now(),
    processed_at Nullable(DateTime)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (user_id, created_at, transaction_id)
-- Table-level TTL rules
TTL
    -- Delete data older than 2 years
    created_at + INTERVAL 2 YEAR DELETE
    -- Move to 'cold' disk after 90 days (requires storage config)
    -- created_at + INTERVAL 90 DAY TO DISK 'cold',
    -- Move to 'archive' volume after 1 year
    -- created_at + INTERVAL 1 YEAR TO VOLUME 'archive'
;


-- -----------------------------------------------------------------------------
-- COLUMN-LEVEL TTL
-- -----------------------------------------------------------------------------
-- You can set TTL on individual columns to null them out over time
-- Useful for PII data retention policies

DROP TABLE IF EXISTS users_with_column_ttl;

CREATE TABLE users_with_column_ttl
(
    user_id UInt64,
    email String TTL created_at + INTERVAL 1 YEAR,  -- Delete email after 1 year
    phone String TTL created_at + INTERVAL 6 MONTH, -- Delete phone after 6 months
    username String,  -- Keep forever
    country LowCardinality(String),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY user_id;


-- -----------------------------------------------------------------------------
-- TTL WITH GROUP BY (Data Rollup)
-- -----------------------------------------------------------------------------
-- Aggregate old detailed data into summaries automatically!
-- Extremely powerful for time-series data.

DROP TABLE IF EXISTS metrics_with_rollup;

CREATE TABLE metrics_with_rollup
(
    metric_name LowCardinality(String),
    timestamp DateTime,
    value Float64,
    count UInt64 DEFAULT 1
)
ENGINE = SummingMergeTree()
ORDER BY (metric_name, timestamp)
TTL
    -- After 1 year, delete (simpler TTL without GROUP BY aggregation)
    timestamp + INTERVAL 1 YEAR DELETE;


-- -----------------------------------------------------------------------------
-- COMPRESSION CODECS
-- -----------------------------------------------------------------------------
-- ClickHouse supports multiple compression algorithms.
-- Choose based on data characteristics and read/write patterns.
--
-- Available codecs:
--   NONE            - No compression
--   LZ4             - Fast compression/decompression (default)
--   LZ4HC           - Higher compression than LZ4, slower
--   ZSTD            - Best compression ratio, moderate speed
--   ZSTD(level)     - ZSTD with compression level 1-22
--   Delta           - Delta encoding for sorted integers
--   DoubleDelta     - For monotonic sequences (timestamps)
--   Gorilla         - For floating point time-series
--   T64             - For integers with narrow range
--
-- Codecs can be chained! Delta + ZSTD is common for time-series.
-- -----------------------------------------------------------------------------


-- Example table with column-level compression
DROP TABLE IF EXISTS transactions_compressed;

CREATE TABLE transactions_compressed
(
    -- UUID: random, no special encoding helps
    transaction_id UUID CODEC(LZ4),

    -- User ID: somewhat sequential in batches, Delta helps slightly
    user_id UInt64 CODEC(Delta, LZ4),

    -- Merchant ID: often repeated, ZSTD compresses well
    merchant_id UInt64 CODEC(ZSTD(3)),

    -- Amount: use ZSTD for decimal values (Gorilla is for Float types only)
    amount Decimal(18, 2) CODEC(ZSTD(3)),

    -- Low cardinality strings: LZ4 is fine, dictionary encoding handles most
    currency LowCardinality(String) CODEC(LZ4),
    status Enum8('pending'=1, 'processing'=2, 'completed'=3,
                 'failed'=4, 'refunded'=5, 'cancelled'=6) CODEC(LZ4),
    category LowCardinality(String) CODEC(LZ4),

    -- Timestamps: DoubleDelta is perfect for monotonic sequences
    created_at DateTime CODEC(DoubleDelta, ZSTD(3)),

    -- Nullable timestamp: Delta works, ZSTD for extra compression
    processed_at Nullable(DateTime) CODEC(Delta, ZSTD(1)),

    -- Large text: maximize compression with ZSTD
    description String CODEC(ZSTD(9))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (user_id, created_at, transaction_id);


-- -----------------------------------------------------------------------------
-- CODEC SELECTION GUIDE
-- -----------------------------------------------------------------------------
--
-- ┌─────────────────────────────┬──────────────────────────────────────────┐
-- │ Data Type                   │ Recommended Codec                        │
-- ├─────────────────────────────┼──────────────────────────────────────────┤
-- │ Monotonic timestamps        │ DoubleDelta + ZSTD                       │
-- │ Sequential integers         │ Delta + LZ4 or Delta + ZSTD              │
-- │ Random integers/UUIDs       │ LZ4 or ZSTD                              │
-- │ Float time-series           │ Gorilla + LZ4                            │
-- │ Money/Decimal               │ ZSTD (Gorilla only for Float types)      │
-- │ Repeated strings            │ LZ4 (use LowCardinality first!)          │
-- │ Large text/JSON             │ ZSTD(9) for max compression              │
-- │ Boolean/Small enums         │ T64 or LZ4                               │
-- │ Arrays                      │ LZ4 or ZSTD                              │
-- └─────────────────────────────┴──────────────────────────────────────────┘
--
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- ANALYZING COMPRESSION
-- -----------------------------------------------------------------------------

-- See compression stats for a table
-- SELECT
--     column,
--     type,
--     formatReadableSize(data_compressed_bytes) AS compressed,
--     formatReadableSize(data_uncompressed_bytes) AS uncompressed,
--     round(data_uncompressed_bytes / data_compressed_bytes, 2) AS ratio,
--     compression_codec
-- FROM system.columns
-- WHERE table = 'transactions_compressed'
-- ORDER BY data_compressed_bytes DESC;


-- Overall table compression
-- SELECT
--     table,
--     formatReadableSize(sum(bytes_on_disk)) AS disk_size,
--     formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
--     round(sum(data_uncompressed_bytes) / sum(bytes_on_disk), 2) AS ratio
-- FROM system.columns
-- WHERE table = 'transactions_compressed'
-- GROUP BY table;


-- -----------------------------------------------------------------------------
-- RECOMPRESS TTL
-- -----------------------------------------------------------------------------
-- Change compression for older data (often to higher compression)

DROP TABLE IF EXISTS logs_with_recompress;

CREATE TABLE logs_with_recompress
(
    timestamp DateTime,
    level LowCardinality(String),
    message String CODEC(LZ4)  -- Fast compression for recent data
)
ENGINE = MergeTree()
ORDER BY timestamp
TTL
    -- After 30 days, recompress with higher compression
    timestamp + INTERVAL 30 DAY RECOMPRESS CODEC(ZSTD(9)),
    -- Delete after 1 year
    timestamp + INTERVAL 1 YEAR DELETE;


-- -----------------------------------------------------------------------------
-- STORAGE TIERING (Requires Storage Configuration)
-- -----------------------------------------------------------------------------
-- ClickHouse can move data between storage tiers based on TTL.
--
-- Example storage.xml config (not SQL, for reference):
--
-- <storage_configuration>
--     <disks>
--         <hot>
--             <type>local</type>
--             <path>/var/lib/clickhouse/hot/</path>
--         </hot>
--         <cold>
--             <type>local</type>
--             <path>/mnt/cold-storage/</path>
--         </cold>
--         <s3>
--             <type>s3</type>
--             <endpoint>https://s3.amazonaws.com/mybucket/</endpoint>
--             <access_key_id>KEY</access_key_id>
--             <secret_access_key>SECRET</secret_access_key>
--         </s3>
--     </disks>
--     <policies>
--         <tiered>
--             <volumes>
--                 <hot><disk>hot</disk></hot>
--                 <cold><disk>cold</disk></cold>
--                 <archive><disk>s3</disk></archive>
--             </volumes>
--         </tiered>
--     </policies>
-- </storage_configuration>
--
-- Then in CREATE TABLE:
-- TTL created_at + INTERVAL 7 DAY TO VOLUME 'cold',
--     created_at + INTERVAL 90 DAY TO VOLUME 'archive'
-- SETTINGS storage_policy = 'tiered';


-- -----------------------------------------------------------------------------
-- MONITORING TTL
-- -----------------------------------------------------------------------------

-- See TTL status for tables
-- SELECT
--     database,
--     table,
--     result_part_name,
--     result_part_path,
--     delete_ttl_info_min,
--     delete_ttl_info_max
-- FROM system.parts
-- WHERE table = 'transactions_with_ttl' AND active;

-- Force TTL evaluation
-- OPTIMIZE TABLE transactions_with_ttl FINAL;

-- See pending TTL merges
-- SELECT * FROM system.merges WHERE table = 'transactions_with_ttl';


-- -----------------------------------------------------------------------------
-- BEST PRACTICES
-- -----------------------------------------------------------------------------

-- 1. TTL is eventual, not immediate
--    Data stays until next merge (could be hours/days)
--    Use OPTIMIZE TABLE to force if needed (sparingly)

-- 2. Test compression on real data
--    Different data has different compression characteristics
--    Test with representative samples before production

-- 3. Don't over-compress hot data
--    High compression = slower reads
--    Use LZ4 for hot data, ZSTD for cold

-- 4. Use column-level TTL for GDPR/PII
--    Remove personal data while keeping analytics

-- 5. Plan storage tiering for cost optimization
--    Hot (NVMe) → Warm (SSD) → Cold (HDD) → Archive (S3)
