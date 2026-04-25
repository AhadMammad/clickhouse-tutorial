-- =============================================================================
-- 02_partitioning_and_keys.sql
-- ClickHouse Fundamentals - Partitioning and Primary Keys Deep Dive
-- =============================================================================
-- This file demonstrates the critical differences between ORDER BY, PRIMARY KEY,
-- and PARTITION BY - the most misunderstood concepts in ClickHouse.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- KEY CONCEPT: ORDER BY vs PRIMARY KEY
-- -----------------------------------------------------------------------------
-- In traditional databases (PostgreSQL, MySQL):
--   PRIMARY KEY = uniqueness constraint + clustered index
--
-- In ClickHouse:
--   PRIMARY KEY = what goes in the sparse index (for lookups)
--   ORDER BY    = physical sort order on disk
--
-- They can be DIFFERENT! PRIMARY KEY must be a prefix of ORDER BY.
--
-- Example:
--   ORDER BY (a, b, c, d)
--   PRIMARY KEY (a, b)     -- Valid: prefix of ORDER BY
--
-- Why would you want them different?
--   - ORDER BY has more columns for uniqueness/sorting
--   - PRIMARY KEY is shorter for a smaller index file
--   - Smaller index = faster to load, better cache utilization
-- -----------------------------------------------------------------------------

-- Example: Creating a table with different ORDER BY and PRIMARY KEY
DROP TABLE IF EXISTS events_example;

CREATE TABLE events_example
(
    event_date Date,
    user_id UInt64,
    event_type LowCardinality(String),
    event_id UUID DEFAULT generateUUIDv4(),
    properties String
)
ENGINE = MergeTree()
-- PARTITION BY: Coarse data organization, enables partition pruning
PARTITION BY toYYYYMM(event_date)
-- ORDER BY: Full sort key for data on disk
ORDER BY (event_date, user_id, event_type, event_id)
-- PRIMARY KEY: Prefix that goes in sparse index (smaller = more efficient)
PRIMARY KEY (event_date, user_id);

-- The sparse index will contain (event_date, user_id) tuples
-- But data is sorted by all four columns


-- -----------------------------------------------------------------------------
-- PARTITION BY - Data Organization
-- -----------------------------------------------------------------------------
-- Partitions are the highest level of data organization in MergeTree.
--
-- How partitions work:
--   1. Data is written to parts (immutable data chunks)
--   2. Each part belongs to exactly one partition
--   3. Merges only happen WITHIN a partition
--   4. Queries can skip entire partitions (partition pruning)
--
-- Common partitioning strategies:
--
--   PARTITION BY toYYYYMM(date_col)
--   → Monthly partitions, good for most time-series
--
--   PARTITION BY toYYYYMMDD(date_col)
--   → Daily partitions, for high-volume data with daily retention
--
--   PARTITION BY (toYYYYMM(date_col), region)
--   → Multi-column partition, use sparingly
--
-- WARNING: Too many partitions hurts performance!
--   - Each partition has overhead
--   - Aim for 100-1000 partitions, not 100,000
--   - Daily partitions for 10 years = 3,650 partitions (borderline)
-- -----------------------------------------------------------------------------

-- Let's inspect partitions using system tables
-- (Run these after inserting data)

-- See all partitions for a table
-- SELECT
--     partition,
--     name,
--     rows,
--     bytes_on_disk,
--     modification_time
-- FROM system.parts
-- WHERE table = 'transactions'
--   AND active = 1
-- ORDER BY partition;


-- -----------------------------------------------------------------------------
-- SPARSE INDEX (Primary Index) Deep Dive
-- -----------------------------------------------------------------------------
-- ClickHouse does NOT index every row. It uses a sparse index.
--
-- How it works:
--   1. Data is divided into granules (default 8192 rows each)
--   2. Index stores the PRIMARY KEY value of the FIRST row in each granule
--   3. Binary search on index finds the granule(s) to scan
--   4. Granules are read and filtered
--
-- ASCII Diagram:
--
--   Granule 0      Granule 1      Granule 2      Granule 3
--   [8192 rows]    [8192 rows]    [8192 rows]    [8192 rows]
--       ↓              ↓              ↓              ↓
--   Index: (A,1)   Index: (B,500) Index: (C,1)   Index: (D,1)
--
--   Query: WHERE user_id = 'B' AND created_at > 400
--   → Binary search finds: Granule 1 might have matches
--   → Read only Granule 1, skip others
--
-- Key insight: ORDER BY column order matters!
--   - First column: Can filter efficiently (binary search)
--   - Second column: Efficient only if first column is fixed
--   - Later columns: Less useful for filtering
--
-- Example:
--   ORDER BY (country, city, user_id)
--
--   WHERE country = 'US'                    → Very fast (uses index)
--   WHERE country = 'US' AND city = 'NYC'   → Very fast (uses index)
--   WHERE city = 'NYC'                      → SLOW! Can't use index efficiently
--   WHERE user_id = 123                     → SLOW! Can't use index efficiently
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- GRANULARITY SETTINGS
-- -----------------------------------------------------------------------------

-- You can customize granularity per table
DROP TABLE IF EXISTS high_granularity_example;

CREATE TABLE high_granularity_example
(
    id UInt64,
    value Float64
)
ENGINE = MergeTree()
ORDER BY id
-- Smaller granules = larger index, better for point queries
-- Larger granules = smaller index, better for range scans
SETTINGS index_granularity = 1024;  -- 1024 rows per granule instead of 8192


-- Adaptive granularity (modern default)
DROP TABLE IF EXISTS adaptive_granularity_example;

CREATE TABLE adaptive_granularity_example
(
    id UInt64,
    data String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS
    index_granularity = 8192,
    index_granularity_bytes = 10485760,  -- 10 MB max per granule
    enable_mixed_granularity_parts = 1;


-- -----------------------------------------------------------------------------
-- INSPECTING THE INDEX AND GRANULES
-- -----------------------------------------------------------------------------

-- See column statistics including marks (granules)
-- SELECT
--     column,
--     type,
--     marks,                    -- Number of granules
--     compressed_size,
--     uncompressed_size,
--     compression_ratio
-- FROM system.columns
-- WHERE table = 'transactions'
-- ORDER BY column;

-- See primary key columns
-- SELECT
--     name,
--     primary_key,
--     sorting_key,
--     partition_key
-- FROM system.tables
-- WHERE name = 'transactions';


-- -----------------------------------------------------------------------------
-- PARTITIONING BEST PRACTICES EXAMPLE
-- -----------------------------------------------------------------------------

-- Good: Monthly partitions for multi-year data
DROP TABLE IF EXISTS logs_monthly;
CREATE TABLE logs_monthly
(
    timestamp DateTime,
    level LowCardinality(String),
    message String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (level, timestamp);


-- Good: Daily partitions with short retention
DROP TABLE IF EXISTS logs_daily;
CREATE TABLE logs_daily
(
    timestamp DateTime,
    level LowCardinality(String),
    message String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (level, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE;  -- Auto-delete after 30 days


-- Bad: Hourly partitions (too many!)
-- This would create 8,760 partitions per year - avoid!
-- DROP TABLE IF EXISTS logs_hourly;
-- CREATE TABLE logs_hourly (...) PARTITION BY toYYYYMMDDhh(timestamp);


-- Bad: High-cardinality partition key
-- This creates one partition per user - terrible!
-- CREATE TABLE bad_example (...) PARTITION BY user_id;


-- -----------------------------------------------------------------------------
-- PARTITION MANAGEMENT OPERATIONS
-- -----------------------------------------------------------------------------

-- These are useful operational commands (run manually as needed):

-- Detach a partition (remove from active data, keep files)
-- ALTER TABLE transactions DETACH PARTITION 202301;

-- Drop a partition (delete data permanently)
-- ALTER TABLE transactions DROP PARTITION 202301;

-- Attach a partition back
-- ALTER TABLE transactions ATTACH PARTITION 202301;

-- Move partition between tables (must have same structure)
-- ALTER TABLE transactions_archive ATTACH PARTITION 202301 FROM transactions;

-- See partition operations in progress
-- SELECT * FROM system.mutations WHERE table = 'transactions';
