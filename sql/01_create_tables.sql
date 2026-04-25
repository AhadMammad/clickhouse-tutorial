-- =============================================================================
-- 01_create_tables.sql
-- ClickHouse Fundamentals - Core Table Definitions
-- =============================================================================
-- This file creates the foundational tables for our payment analytics system.
-- We demonstrate MergeTree engine, proper data types, and ClickHouse best practices.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DATA TYPES REFERENCE (ClickHouse supports a rich type system)
-- -----------------------------------------------------------------------------
-- Integer Types:
--   UInt8   (0 to 255)           - flags, small enums, boolean-like
--   UInt16  (0 to 65535)         - port numbers, small counts
--   UInt32  (0 to 4 billion)     - user IDs, most counters
--   UInt64  (0 to 18 quintillion)- large IDs, timestamps as integers
--   Int8/16/32/64                - signed versions when negatives needed
--
-- Floating Point:
--   Float32 (single precision)   - fast but ~7 digits precision
--   Float64 (double precision)   - ~15 digits precision
--   Decimal(P, S)                - exact decimal, P=precision, S=scale
--                                  Use for money! Decimal(18,2) is common
--
-- String Types:
--   String                       - variable length, UTF-8
--   FixedString(N)               - fixed N bytes, padded with nulls
--   LowCardinality(String)       - dictionary-encoded, HUGE savings for
--                                  repeated values like country codes, status
--
-- Date/Time Types:
--   Date                         - day precision (2 bytes, 1970-2149)
--   Date32                       - extended range (4 bytes, 1900-2299)
--   DateTime                     - second precision (4 bytes)
--   DateTime64(precision)        - sub-second, precision=3 for ms, 6 for μs
--
-- Special Types:
--   UUID                         - 128-bit UUID, stored efficiently
--   Enum8('a'=1, 'b'=2)          - 8-bit enum, validates on insert
--   Enum16                       - 16-bit enum for more values
--   Array(T)                     - array of type T
--   Tuple(T1, T2, ...)           - fixed heterogeneous tuple
--   Map(K, V)                    - key-value map
--   Nullable(T)                  - allows NULL (adds storage overhead!)
--   IPv4, IPv6                   - network address types
-- -----------------------------------------------------------------------------

-- Drop existing tables for idempotency (safe re-runs)
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS merchants;
DROP TABLE IF EXISTS users;

-- -----------------------------------------------------------------------------
-- TRANSACTIONS TABLE (Main fact table)
-- -----------------------------------------------------------------------------
-- This is our core fact table storing payment transactions.
--
-- ENGINE = MergeTree() is the foundation of all ClickHouse analytics.
-- It provides:
--   - Columnar storage (each column stored separately)
--   - Background merges (async consolidation of parts)
--   - Sparse primary index (efficient range queries)
--   - Excellent compression (similar values in columns)
--
-- Design decisions explained:
--   1. PARTITION BY toYYYYMM(created_at)
--      - Creates monthly partitions
--      - Enables partition pruning (skip entire months in queries)
--      - Allows efficient TTL and partition management
--
--   2. ORDER BY (user_id, created_at, transaction_id)
--      - Determines physical sort order on disk
--      - First column(s) = most efficient for filtering
--      - user_id first: optimizes "show me user X's transactions"
--      - created_at second: efficient time-range within a user
--      - transaction_id: ensures uniqueness in sort key
--
--   3. PRIMARY KEY (user_id, created_at)
--      - DIFFERENT from ORDER BY in ClickHouse!
--      - Defines what goes in the sparse index
--      - Can be a prefix of ORDER BY (and usually is)
--      - Smaller primary key = smaller index = faster loads
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transactions
(
    -- Unique identifier for the transaction
    -- UUID is efficient in ClickHouse (stored as 2x UInt64)
    transaction_id UUID DEFAULT generateUUIDv4(),

    -- Foreign keys to dimension tables
    -- UInt64 for large ID spaces, UInt32 if <4 billion entities
    user_id UInt64,
    merchant_id UInt64,

    -- Financial amount with exact decimal precision
    -- Decimal(18,2) supports up to 9,999,999,999,999,999.99
    -- NEVER use Float for money!
    amount Decimal(18, 2),

    -- Currency code - LowCardinality saves massive space
    -- ~200 currencies → dictionary encoding is perfect
    -- Can reduce storage by 10-100x vs plain String
    currency LowCardinality(String) DEFAULT 'USD',

    -- Transaction status as Enum8 (1 byte, validated)
    -- Enum enforces valid values at insert time
    -- Much better than storing arbitrary strings
    status Enum8(
        'pending' = 1,
        'processing' = 2,
        'completed' = 3,
        'failed' = 4,
        'refunded' = 5,
        'cancelled' = 6
    ) DEFAULT 'pending',

    -- Payment category - another great LowCardinality candidate
    category LowCardinality(String),

    -- Payment method
    payment_method LowCardinality(String) DEFAULT 'card',

    -- Timestamps
    created_at DateTime DEFAULT now(),
    processed_at Nullable(DateTime),  -- NULL until processed

    -- Metadata as flexible Map type
    -- Useful for variable attributes without schema changes
    metadata Map(String, String) DEFAULT map()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (user_id, created_at, transaction_id)
PRIMARY KEY (user_id, created_at)
SETTINGS index_granularity = 8192;  -- Default, shown for clarity

-- Add a comment explaining the table
-- (ClickHouse supports table and column comments)


-- -----------------------------------------------------------------------------
-- MERCHANTS TABLE (Dimension table)
-- -----------------------------------------------------------------------------
-- Dimension tables in ClickHouse are typically smaller and change less.
-- We still use MergeTree for consistency and merge capabilities.
--
-- For small dimension tables, consider:
--   - Dictionary: For pure lookups (JOIN optimization)
--   - Memory engine: If data fits in RAM and is read-only
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS merchants
(
    merchant_id UInt64,

    -- Business information
    name String,
    category LowCardinality(String),

    -- Location data
    country_code LowCardinality(String),  -- ISO 3166-1 alpha-2
    city String,

    -- MCC (Merchant Category Code) - standard 4-digit code
    mcc UInt16,

    -- Business metrics
    is_active UInt8 DEFAULT 1,  -- Boolean as UInt8 (0/1)
    monthly_volume Decimal(18, 2) DEFAULT 0,

    -- Timestamps
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (merchant_id)
PRIMARY KEY (merchant_id);


-- -----------------------------------------------------------------------------
-- USERS TABLE (Dimension table)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS users
(
    user_id UInt64,

    -- User information
    email String,
    username String,

    -- Profile data
    country_code LowCardinality(String),
    tier LowCardinality(String) DEFAULT 'standard',  -- standard, premium, enterprise

    -- Account status
    is_verified UInt8 DEFAULT 0,
    is_active UInt8 DEFAULT 1,

    -- Timestamps
    created_at DateTime DEFAULT now(),
    last_login_at Nullable(DateTime)
)
ENGINE = MergeTree()
ORDER BY (user_id)
PRIMARY KEY (user_id);


-- -----------------------------------------------------------------------------
-- VERIFICATION QUERIES
-- -----------------------------------------------------------------------------
-- Run these to verify tables were created correctly:

-- Show all tables in current database
-- SELECT name, engine FROM system.tables WHERE database = currentDatabase();

-- Show columns and types for transactions table
-- DESCRIBE TABLE transactions;

-- Show table creation statement
-- SHOW CREATE TABLE transactions;
