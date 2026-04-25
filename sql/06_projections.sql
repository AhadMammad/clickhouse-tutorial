-- =============================================================================
-- 06_projections.sql
-- ClickHouse Advanced - Projections for Query Optimization
-- =============================================================================
-- Projections are hidden materialized copies of data sorted differently.
-- ClickHouse automatically uses them when they speed up queries.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- WHAT ARE PROJECTIONS?
-- -----------------------------------------------------------------------------
-- A projection is a secondary representation of table data with:
--   - Different ORDER BY (optimizes different query patterns)
--   - Optional GROUP BY (pre-aggregation)
--   - Automatic maintenance (updates with main table)
--
-- Think of it as: "Store the same data sorted another way"
--
-- Example:
--   Main table: ORDER BY (user_id, created_at)  -- Fast for user queries
--   Projection:  ORDER BY (merchant_id, created_at)  -- Fast for merchant queries
--
-- The optimizer chooses the best representation automatically!
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- PROJECTIONS vs MATERIALIZED VIEWS
-- -----------------------------------------------------------------------------
--
-- ┌────────────────────┬─────────────────────┬─────────────────────────┐
-- │ Aspect             │ Projection          │ Materialized View       │
-- ├────────────────────┼─────────────────────┼─────────────────────────┤
-- │ Storage            │ Inside same table   │ Separate table          │
-- │ Automatic selection│ Yes (optimizer)     │ No (explicit query)     │
-- │ Flexibility        │ Limited transforms  │ Full SQL transforms     │
-- │ Management         │ Automatic           │ Manual (target table)   │
-- │ Use case           │ Index-like speedup  │ Complex aggregations    │
-- └────────────────────┴─────────────────────┴─────────────────────────┘
--
-- Use projections when:
--   - You need the same data sorted differently
--   - Simple pre-aggregations
--   - Automatic optimizer selection is desired
--
-- Use materialized views when:
--   - Complex transformations
--   - Joins with other tables
--   - Multi-level aggregation
-- -----------------------------------------------------------------------------


-- Create a transactions table with projections
DROP TABLE IF EXISTS transactions_with_projections;

CREATE TABLE transactions_with_projections
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
    ) DEFAULT 'completed',
    category LowCardinality(String),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
-- Main order: optimized for user-centric queries
ORDER BY (user_id, created_at, transaction_id);


-- -----------------------------------------------------------------------------
-- ADDING PROJECTIONS
-- -----------------------------------------------------------------------------

-- Projection 1: Merchant-centric queries
-- Speeds up: WHERE merchant_id = X, GROUP BY merchant_id
ALTER TABLE transactions_with_projections
ADD PROJECTION proj_by_merchant
(
    SELECT
        transaction_id,
        user_id,
        merchant_id,
        amount,
        currency,
        status,
        category,
        created_at
    ORDER BY (merchant_id, created_at, transaction_id)
);


-- Projection 2: Category-based analysis
-- Speeds up: WHERE category = X, GROUP BY category
ALTER TABLE transactions_with_projections
ADD PROJECTION proj_by_category
(
    SELECT
        transaction_id,
        user_id,
        merchant_id,
        amount,
        currency,
        status,
        category,
        created_at
    ORDER BY (category, created_at)
);


-- Projection 3: Pre-aggregated daily stats per merchant
-- This is a powerful pattern: stores GROUP BY results!
ALTER TABLE transactions_with_projections
ADD PROJECTION proj_daily_merchant_stats
(
    SELECT
        merchant_id,
        toDate(created_at) AS txn_date,
        count() AS txn_count,
        sum(amount) AS total_amount,
        avg(amount) AS avg_amount
    GROUP BY
        merchant_id,
        toDate(created_at)
);


-- Projection 4: Pre-aggregated stats by status
ALTER TABLE transactions_with_projections
ADD PROJECTION proj_status_stats
(
    SELECT
        status,
        toStartOfHour(created_at) AS hour,
        count() AS txn_count,
        sum(amount) AS total_amount
    GROUP BY
        status,
        toStartOfHour(created_at)
);


-- -----------------------------------------------------------------------------
-- MATERIALIZING PROJECTIONS
-- -----------------------------------------------------------------------------
-- Projections must be materialized to be used!
-- New data is automatically added, but existing data needs explicit command.

ALTER TABLE transactions_with_projections
MATERIALIZE PROJECTION proj_by_merchant;

ALTER TABLE transactions_with_projections
MATERIALIZE PROJECTION proj_by_category;

ALTER TABLE transactions_with_projections
MATERIALIZE PROJECTION proj_daily_merchant_stats;

ALTER TABLE transactions_with_projections
MATERIALIZE PROJECTION proj_status_stats;


-- -----------------------------------------------------------------------------
-- USING PROJECTIONS (Automatic Selection)
-- -----------------------------------------------------------------------------

-- This query will automatically use proj_by_merchant
-- SELECT merchant_id, count(), sum(amount)
-- FROM transactions_with_projections
-- WHERE merchant_id = 5001
-- GROUP BY merchant_id;

-- This query will use proj_daily_merchant_stats (pre-aggregated!)
-- SELECT merchant_id, toDate(created_at), count(), sum(amount)
-- FROM transactions_with_projections
-- GROUP BY merchant_id, toDate(created_at);


-- -----------------------------------------------------------------------------
-- VERIFYING PROJECTION USAGE WITH EXPLAIN
-- -----------------------------------------------------------------------------

-- Use EXPLAIN to see if a projection is being used:
--
-- EXPLAIN indexes = 1
-- SELECT merchant_id, count(), sum(amount)
-- FROM transactions_with_projections
-- WHERE merchant_id = 5001
-- GROUP BY merchant_id;
--
-- Look for "Projection: proj_by_merchant" in output


-- EXPLAIN with actions shows more detail:
--
-- EXPLAIN actions = 1
-- SELECT merchant_id, toDate(created_at), sum(amount)
-- FROM transactions_with_projections
-- GROUP BY merchant_id, toDate(created_at);


-- EXPLAIN PLAN shows the full query plan:
--
-- EXPLAIN PLAN
-- SELECT *
-- FROM transactions_with_projections
-- WHERE merchant_id = 5001
-- ORDER BY created_at DESC
-- LIMIT 100;


-- -----------------------------------------------------------------------------
-- PROJECTION MANAGEMENT
-- -----------------------------------------------------------------------------

-- List all projections on a table
-- SELECT name, partition, rows, bytes_on_disk
-- FROM system.projection_parts
-- WHERE table = 'transactions_with_projections' AND active;

-- Check projection definitions
-- SELECT name, type, expr
-- FROM system.projections
-- WHERE table = 'transactions_with_projections';

-- Drop a projection
-- ALTER TABLE transactions_with_projections
-- DROP PROJECTION proj_by_category;


-- -----------------------------------------------------------------------------
-- BEST PRACTICES
-- -----------------------------------------------------------------------------

-- 1. Start with the most common query patterns
--    Analyze your query log to find slow queries that could benefit

-- 2. Don't over-project
--    Each projection multiplies storage and write overhead
--    Start with 2-3, add more based on performance data

-- 3. Use aggregating projections wisely
--    Pre-aggregation is powerful but adds complexity
--    Ensure the GROUP BY matches your common queries exactly

-- 4. Monitor projection usage
--    SELECT query, projections
--    FROM system.query_log
--    WHERE projections != []
--    ORDER BY event_time DESC;

-- 5. Consider storage trade-offs
--    SELECT
--        table,
--        sum(bytes_on_disk) AS total_bytes,
--        formatReadableSize(sum(bytes_on_disk)) AS readable
--    FROM system.projection_parts
--    WHERE active
--    GROUP BY table;


-- -----------------------------------------------------------------------------
-- GOTCHAS
-- -----------------------------------------------------------------------------

-- GOTCHA 1: Projection must be materialized
-- New projections are empty until MATERIALIZE PROJECTION runs

-- GOTCHA 2: FINAL doesn't use projections
-- SELECT ... FINAL won't benefit from projections in most cases

-- GOTCHA 3: Projection columns must exist in base table
-- Can't add computed columns that aren't in the source

-- GOTCHA 4: Aggregating projections have limitations
-- GROUP BY must match exactly for the projection to be used
-- Different GROUP BY columns = projection not used

-- GOTCHA 5: Write amplification
-- Every INSERT writes to base table + all projections
-- More projections = slower inserts
