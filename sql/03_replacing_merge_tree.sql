-- =============================================================================
-- 03_replacing_merge_tree.sql
-- ClickHouse Advanced - ReplacingMergeTree for Deduplication
-- =============================================================================
-- ReplacingMergeTree handles the common case where you insert updates as
-- new rows and want only the latest version of each record.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- PROBLEM: ClickHouse MergeTree Doesn't Deduplicate
-- -----------------------------------------------------------------------------
-- In MergeTree, if you insert the same row twice, you get two rows.
-- There's no UPDATE statement like in PostgreSQL.
--
-- Common scenarios needing deduplication:
--   1. CDC (Change Data Capture) streams that replay events
--   2. At-least-once message delivery from Kafka
--   3. Application retries on network errors
--   4. Updating records by inserting new versions
--
-- ReplacingMergeTree solves this by:
--   - Keeping only the latest row for each unique ORDER BY key
--   - Deduplication happens DURING MERGES (not immediately!)
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- ReplacingMergeTree SYNTAX
-- -----------------------------------------------------------------------------
-- ENGINE = ReplacingMergeTree([ver])
--
-- Parameters:
--   ver (optional) - Column name for version. Higher value = newer row.
--                    If omitted, the row inserted later wins.
--
-- The ORDER BY clause defines what makes a row "unique" for replacement.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS transactions_dedup;

CREATE TABLE transactions_dedup
(
    -- The transaction_id is our logical primary key
    transaction_id UUID,

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
    created_at DateTime,

    -- Version column: higher value = newer record
    -- Use DateTime or UInt64 timestamp
    updated_at DateTime DEFAULT now()
)
-- ReplacingMergeTree with version column
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
-- ORDER BY defines uniqueness for replacement
-- Rows with same (transaction_id) will be deduplicated
ORDER BY (transaction_id)
PRIMARY KEY (transaction_id);


-- -----------------------------------------------------------------------------
-- HOW DEDUPLICATION WORKS
-- -----------------------------------------------------------------------------
-- CRITICAL: Deduplication is EVENTUAL, not immediate!
--
-- Timeline:
--   T0: INSERT row with id=1, version=1   → Stored in part A
--   T1: INSERT row with id=1, version=2   → Stored in part B
--   T2: SELECT * WHERE id=1               → Returns BOTH rows!
--   T3: Background merge runs              → Parts A+B merge to C
--   T4: SELECT * WHERE id=1               → Returns only version=2
--
-- You cannot rely on deduplication being instant!
-- -----------------------------------------------------------------------------


-- Let's demonstrate with an example
-- First, insert an initial transaction
INSERT INTO transactions_dedup
    (transaction_id, user_id, merchant_id, amount, status, category, created_at, updated_at)
VALUES
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1001, 5001, 99.99, 'pending', 'retail', now(), now());

-- Simulate an update by inserting a new version
INSERT INTO transactions_dedup
    (transaction_id, user_id, merchant_id, amount, status, category, created_at, updated_at)
VALUES
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1001, 5001, 99.99, 'completed', 'retail', now(), now() + INTERVAL 1 SECOND);


-- -----------------------------------------------------------------------------
-- QUERYING DEDUPLICATED DATA
-- -----------------------------------------------------------------------------

-- Method 1: SELECT ... FINAL
-- Forces deduplication at query time. Simple but can be slow on large tables.
-- SELECT * FROM transactions_dedup FINAL WHERE transaction_id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';

-- Method 2: GROUP BY with argMax
-- More flexible, can be faster for complex queries
-- SELECT
--     transaction_id,
--     argMax(status, updated_at) as status,
--     argMax(amount, updated_at) as amount,
--     max(updated_at) as updated_at
-- FROM transactions_dedup
-- WHERE transaction_id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
-- GROUP BY transaction_id;

-- Method 3: OPTIMIZE TABLE (force merge)
-- Do NOT run this in production frequently! It's resource-intensive.
-- OPTIMIZE TABLE transactions_dedup FINAL;


-- -----------------------------------------------------------------------------
-- COMMON PATTERNS WITH ReplacingMergeTree
-- -----------------------------------------------------------------------------

-- Pattern 1: Upsert simulation
-- Insert new records and updates the same way
DROP TABLE IF EXISTS products;
CREATE TABLE products
(
    product_id UInt64,
    name String,
    price Decimal(10, 2),
    stock_quantity UInt32,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (product_id);

-- Insert initial product
INSERT INTO products (product_id, name, price, stock_quantity) VALUES (1, 'Widget', 29.99, 100);

-- "Update" by inserting new version
INSERT INTO products (product_id, name, price, stock_quantity) VALUES (1, 'Widget Pro', 39.99, 150);

-- Query with FINAL to get latest
-- SELECT * FROM products FINAL;


-- Pattern 2: CDC Integration
-- When receiving updates from Debezium/Kafka Connect
DROP TABLE IF EXISTS customers_cdc;
CREATE TABLE customers_cdc
(
    customer_id UInt64,
    email String,
    name String,
    is_deleted UInt8 DEFAULT 0,  -- Soft delete flag
    _version UInt64,              -- Kafka offset or DB sequence number
    _ts DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (customer_id);


-- -----------------------------------------------------------------------------
-- GOTCHAS AND BEST PRACTICES
-- -----------------------------------------------------------------------------

-- GOTCHA 1: FINAL can be slow on large tables
-- Use it for small result sets or filtered queries
-- Bad:  SELECT * FROM big_table FINAL;
-- Good: SELECT * FROM big_table FINAL WHERE user_id = 123;

-- GOTCHA 2: Deduplication only within same partition
-- If version column changes which partition a row belongs to,
-- you'll have duplicates across partitions!

-- GOTCHA 3: ORDER BY defines uniqueness
-- All columns in ORDER BY must match for deduplication
-- ORDER BY (user_id, event_type) means (user_id=1, event_type='click')
-- and (user_id=1, event_type='view') are DIFFERENT and both kept

-- GOTCHA 4: No version column = insertion order matters
-- Without version column, the "last inserted" row wins during merge
-- But "last" is not guaranteed with parallel inserts!


-- -----------------------------------------------------------------------------
-- WHEN TO USE ReplacingMergeTree
-- -----------------------------------------------------------------------------
-- ✅ Good use cases:
--   - CDC pipelines where you receive full row updates
--   - At-least-once delivery with possible duplicates
--   - Slowly changing dimensions (SCD Type 1)
--   - Any "upsert" pattern
--
-- ❌ Not ideal for:
--   - Immediate consistency requirements (use PostgreSQL)
--   - Very high update frequency (consider different design)
--   - Complex update logic (consider materialized views)
--
-- Alternative approaches:
--   - CollapsingMergeTree: For insertions + deletions
--   - VersionedCollapsingMergeTree: For full CDC with deletes
--   - AggregatingMergeTree: For pre-aggregation
-- -----------------------------------------------------------------------------


-- Clean up example data
-- TRUNCATE TABLE transactions_dedup;
