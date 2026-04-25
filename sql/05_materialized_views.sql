-- =============================================================================
-- 05_materialized_views.sql
-- ClickHouse Advanced - Materialized Views for Real-Time Aggregation
-- =============================================================================
-- Materialized views in ClickHouse are NOT like PostgreSQL materialized views!
-- They act as INSERT TRIGGERS that transform data as it arrives.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- WHAT ARE MATERIALIZED VIEWS IN CLICKHOUSE?
-- -----------------------------------------------------------------------------
-- In PostgreSQL:
--   - Materialized view is a snapshot of a query
--   - Must be manually refreshed (REFRESH MATERIALIZED VIEW)
--   - Full recomputation on refresh
--
-- In ClickHouse:
--   - Materialized view is an INSERT TRIGGER
--   - Automatically processes new data as it's inserted
--   - Incremental: only processes new rows, not historical data
--   - Can transform, filter, and aggregate on the fly
--
-- Think of it as:
--   INSERT INTO source_table → triggers → INSERT INTO mv_target_table
-- -----------------------------------------------------------------------------


-- First, ensure we have our source table
-- (Already created in 01_create_tables.sql, but included for completeness)


-- -----------------------------------------------------------------------------
-- MATERIALIZED VIEW PATTERN 1: Simple Transformation
-- -----------------------------------------------------------------------------
-- Target table stores transformed data

DROP TABLE IF EXISTS mv_transactions_enriched_target;
DROP VIEW IF EXISTS mv_transactions_enriched;

CREATE TABLE mv_transactions_enriched_target
(
    transaction_id UUID,
    user_id UInt64,
    merchant_id UInt64,
    amount Decimal(18, 2),
    amount_usd Decimal(18, 2),  -- Converted to USD
    status String,              -- Denormalized from Enum
    category LowCardinality(String),
    hour_of_day UInt8,          -- Extracted for time analysis
    day_of_week UInt8,
    is_weekend UInt8,
    created_at DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (user_id, created_at);

-- The materialized view triggers on INSERT to transactions
CREATE MATERIALIZED VIEW mv_transactions_enriched
TO mv_transactions_enriched_target
AS SELECT
    transaction_id,
    user_id,
    merchant_id,
    amount,
    -- Simple currency conversion (in practice, use a lookup table)
    -- Cast float literals to Decimal to match amount's Decimal(18,2) type
    multiIf(
        currency = 'USD', amount,
        currency = 'EUR', toDecimal64(amount * 1.08, 2),
        currency = 'GBP', toDecimal64(amount * 1.27, 2),
        amount
    ) AS amount_usd,
    toString(status) AS status,
    category,
    toHour(created_at) AS hour_of_day,
    toDayOfWeek(created_at) AS day_of_week,
    if(toDayOfWeek(created_at) IN (6, 7), 1, 0) AS is_weekend,
    created_at
FROM transactions;


-- -----------------------------------------------------------------------------
-- MATERIALIZED VIEW PATTERN 2: Pre-Aggregation with AggregatingMergeTree
-- -----------------------------------------------------------------------------
-- This is the most powerful pattern: real-time aggregation!

-- Target table for hourly revenue by merchant
DROP TABLE IF EXISTS mv_hourly_revenue_target;
DROP VIEW IF EXISTS mv_hourly_revenue;

CREATE TABLE mv_hourly_revenue_target
(
    merchant_id UInt64,
    category LowCardinality(String),
    hour DateTime,  -- Truncated to hour

    -- Aggregate states for incremental merging
    total_amount AggregateFunction(sum, Decimal(18, 2)),
    transaction_count AggregateFunction(count),
    unique_users AggregateFunction(uniq, UInt64),
    avg_amount AggregateFunction(avg, Decimal(18, 2))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (merchant_id, category, hour);

-- Materialized view that aggregates on insert
CREATE MATERIALIZED VIEW mv_hourly_revenue
TO mv_hourly_revenue_target
AS SELECT
    merchant_id,
    category,
    toStartOfHour(created_at) AS hour,

    sumState(amount) AS total_amount,
    countState() AS transaction_count,
    uniqState(user_id) AS unique_users,
    avgState(amount) AS avg_amount
FROM transactions
GROUP BY
    merchant_id,
    category,
    toStartOfHour(created_at);


-- Query the aggregated data (remember to use -Merge functions!)
-- SELECT
--     merchant_id,
--     category,
--     hour,
--     sumMerge(total_amount) AS revenue,
--     countMerge(transaction_count) AS txn_count,
--     uniqMerge(unique_users) AS unique_customers,
--     avgMerge(avg_amount) AS avg_transaction
-- FROM mv_hourly_revenue_target
-- WHERE hour >= now() - INTERVAL 7 DAY
-- GROUP BY merchant_id, category, hour
-- ORDER BY hour DESC;


-- -----------------------------------------------------------------------------
-- MATERIALIZED VIEW PATTERN 3: User Spending Summary
-- -----------------------------------------------------------------------------
-- Track running totals per user per day using SummingMergeTree

DROP TABLE IF EXISTS mv_user_spending_target;
DROP VIEW IF EXISTS mv_user_spending;

CREATE TABLE mv_user_spending_target
(
    user_id UInt64,
    spending_date Date,
    currency LowCardinality(String),

    -- These will be automatically summed by SummingMergeTree
    total_spent Decimal(18, 2),
    transaction_count UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(spending_date)
ORDER BY (user_id, spending_date, currency);

CREATE MATERIALIZED VIEW mv_user_spending
TO mv_user_spending_target
AS SELECT
    user_id,
    toDate(created_at) AS spending_date,
    currency,
    sum(amount) AS total_spent,
    count() AS transaction_count
FROM transactions
WHERE status IN ('completed', 'processing')  -- Only count successful transactions
GROUP BY
    user_id,
    toDate(created_at),
    currency;


-- Query user spending
-- SELECT
--     user_id,
--     spending_date,
--     sum(total_spent) AS daily_total,
--     sum(transaction_count) AS daily_txns
-- FROM mv_user_spending_target FINAL
-- WHERE user_id = 1001
-- GROUP BY user_id, spending_date
-- ORDER BY spending_date DESC;


-- -----------------------------------------------------------------------------
-- MATERIALIZED VIEW PATTERN 4: Category Statistics
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS mv_category_stats_target;
DROP VIEW IF EXISTS mv_category_stats;

CREATE TABLE mv_category_stats_target
(
    category LowCardinality(String),
    status String,
    stat_date Date,

    -- Aggregate states
    total_amount AggregateFunction(sum, Decimal(18, 2)),
    transaction_count AggregateFunction(count),
    unique_merchants AggregateFunction(uniq, UInt64),
    unique_users AggregateFunction(uniq, UInt64),
    amount_p50 AggregateFunction(quantile(0.5), Decimal(18, 2)),
    amount_p95 AggregateFunction(quantile(0.95), Decimal(18, 2))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(stat_date)
ORDER BY (category, status, stat_date);

CREATE MATERIALIZED VIEW mv_category_stats
TO mv_category_stats_target
AS SELECT
    category,
    toString(status) AS status,
    toDate(created_at) AS stat_date,

    sumState(amount) AS total_amount,
    countState() AS transaction_count,
    uniqState(merchant_id) AS unique_merchants,
    uniqState(user_id) AS unique_users,
    quantileState(0.5)(amount) AS amount_p50,
    quantileState(0.95)(amount) AS amount_p95
FROM transactions
GROUP BY
    category,
    status,     -- reference SELECT alias to avoid ambiguity with source Enum column
    stat_date;


-- -----------------------------------------------------------------------------
-- POPULATING EXISTING DATA
-- -----------------------------------------------------------------------------
-- Materialized views only process NEW inserts!
-- To backfill historical data, insert manually:

-- INSERT INTO mv_hourly_revenue_target
-- SELECT
--     merchant_id,
--     category,
--     toStartOfHour(created_at) AS hour,
--     sumState(amount) AS total_amount,
--     countState() AS transaction_count,
--     uniqState(user_id) AS unique_users,
--     avgState(amount) AS avg_amount
-- FROM transactions
-- WHERE created_at < now()  -- Historical data
-- GROUP BY merchant_id, category, toStartOfHour(created_at);


-- -----------------------------------------------------------------------------
-- MATERIALIZED VIEW CHAINING (Fan-Out Pattern)
-- -----------------------------------------------------------------------------
-- You can chain MVs: Table A → MV 1 → Table B → MV 2 → Table C
-- Useful for multi-level aggregation pipelines
--
-- Example flow:
--   raw_events → mv_hourly_stats → hourly_stats_table
--                                        ↓
--                              mv_daily_stats → daily_stats_table
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- GOTCHAS AND BEST PRACTICES
-- -----------------------------------------------------------------------------

-- GOTCHA 1: MVs don't process historical data
-- Solution: Manual INSERT ... SELECT for backfill

-- GOTCHA 2: MV errors can block inserts
-- If the MV query fails, the original insert may fail too!
-- Always test MV logic thoroughly

-- GOTCHA 3: MV runs in insert context
-- No access to other tables during MV execution (use JOINs carefully)

-- GOTCHA 4: Dropped MV doesn't drop target table
-- DROP VIEW mv_name; -- Only drops the trigger
-- DROP TABLE mv_target_table; -- Separate command for data

-- GOTCHA 5: MV with TO clause vs implicit storage
--   CREATE MATERIALIZED VIEW mv TO target_table AS SELECT...
--   → Uses existing target_table (recommended)
--
--   CREATE MATERIALIZED VIEW mv ENGINE=... AS SELECT...
--   → Creates hidden .inner.mv table (harder to manage)

-- BEST PRACTICE: Always use "TO table_name" syntax
-- BEST PRACTICE: Name target tables clearly (mv_*_target)
-- BEST PRACTICE: Test INSERT → SELECT flow before creating MV
-- BEST PRACTICE: Monitor MV lag with system.mutations


-- -----------------------------------------------------------------------------
-- DEBUGGING MATERIALIZED VIEWS
-- -----------------------------------------------------------------------------

-- See all materialized views
-- SELECT name, engine, create_table_query
-- FROM system.tables
-- WHERE engine LIKE '%MaterializedView%';

-- Check for MV-related errors in query log
-- SELECT query, exception
-- FROM system.query_log
-- WHERE query LIKE '%mv_%' AND exception != ''
-- ORDER BY event_time DESC
-- LIMIT 10;

-- Monitor part count (too many = MV not merging fast enough)
-- SELECT table, count() as parts
-- FROM system.parts
-- WHERE active AND table LIKE 'mv_%'
-- GROUP BY table;
