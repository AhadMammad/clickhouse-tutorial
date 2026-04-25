-- =============================================================================
-- 04_aggregating_merge_tree.sql
-- ClickHouse Advanced - AggregatingMergeTree for Pre-Aggregation
-- =============================================================================
-- AggregatingMergeTree is the most powerful engine for analytics.
-- It stores intermediate aggregation states that merge incrementally.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- THE AGGREGATION PROBLEM
-- -----------------------------------------------------------------------------
-- Imagine you need to compute daily revenue per merchant.
--
-- Naive approach: Query raw transactions every time
--   SELECT merchant_id, toDate(created_at), sum(amount)
--   FROM transactions
--   GROUP BY merchant_id, toDate(created_at)
--
-- Problem: Scans millions/billions of rows every query!
--
-- Better: Pre-aggregate into a summary table
-- But how do you handle:
--   - New data arriving continuously?
--   - Updating existing aggregates?
--   - Multiple summary metrics?
--
-- Solution: AggregatingMergeTree with -State/-Merge combinators
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- STATE FUNCTIONS AND MERGE FUNCTIONS
-- -----------------------------------------------------------------------------
-- Every aggregation function (sum, avg, count, etc.) has:
--
--   1. -State suffix: Computes PARTIAL aggregation state
--      sumState(amount) → Binary blob representing partial sum
--
--   2. -Merge suffix: Combines partial states into final result
--      sumMerge(amount_state) → Final sum value
--
-- Why is this powerful?
--   - States can be merged incrementally
--   - Insert new data → merge states later
--   - Computes correct results without re-scanning raw data
--
-- Example:
--   Day 1: Insert sumState(100) → state = [100]
--   Day 2: Insert sumState(50)  → state = [50]
--   Merge: Combines [100] + [50] → [150]
--   Query: sumMerge → 150
-- -----------------------------------------------------------------------------


-- Create the aggregated metrics table
DROP TABLE IF EXISTS payment_metrics_agg;

CREATE TABLE payment_metrics_agg
(
    -- Aggregation dimensions
    merchant_id UInt64,
    category LowCardinality(String),
    currency LowCardinality(String),
    metric_date Date,

    -- Aggregated states (stored as binary blobs)
    -- These are NOT regular columns - they store partial aggregation states
    total_amount AggregateFunction(sum, Decimal(18, 2)),
    transaction_count AggregateFunction(count),
    avg_amount AggregateFunction(avg, Decimal(18, 2)),
    min_amount AggregateFunction(min, Decimal(18, 2)),
    max_amount AggregateFunction(max, Decimal(18, 2)),

    -- For distinct counting, we use uniq (HyperLogLog-based)
    unique_users AggregateFunction(uniq, UInt64),

    -- For percentiles, use quantileState
    amount_p50 AggregateFunction(quantile(0.5), Decimal(18, 2)),
    amount_p99 AggregateFunction(quantile(0.99), Decimal(18, 2))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(metric_date)
ORDER BY (merchant_id, category, currency, metric_date);


-- -----------------------------------------------------------------------------
-- INSERTING DATA INTO AggregatingMergeTree
-- -----------------------------------------------------------------------------
-- You MUST use -State functions when inserting!
-- INSERT ... SELECT with aggregation functions + State suffix

INSERT INTO payment_metrics_agg
SELECT
    merchant_id,
    category,
    currency,
    toDate(created_at) AS metric_date,

    -- Use -State combinators for each aggregate
    sumState(amount) AS total_amount,
    countState() AS transaction_count,
    avgState(amount) AS avg_amount,
    minState(amount) AS min_amount,
    maxState(amount) AS max_amount,
    uniqState(user_id) AS unique_users,
    quantileState(0.5)(amount) AS amount_p50,
    quantileState(0.99)(amount) AS amount_p99
FROM transactions
GROUP BY
    merchant_id,
    category,
    currency,
    toDate(created_at);


-- -----------------------------------------------------------------------------
-- QUERYING FROM AggregatingMergeTree
-- -----------------------------------------------------------------------------
-- You MUST use -Merge functions when querying!

-- Get total revenue and transaction count per merchant
-- SELECT
--     merchant_id,
--     sumMerge(total_amount) AS revenue,
--     countMerge(transaction_count) AS txn_count,
--     avgMerge(avg_amount) AS avg_txn_amount
-- FROM payment_metrics_agg
-- GROUP BY merchant_id
-- ORDER BY revenue DESC
-- LIMIT 10;


-- Get daily metrics for a specific merchant
-- SELECT
--     metric_date,
--     sumMerge(total_amount) AS revenue,
--     countMerge(transaction_count) AS txn_count,
--     uniqMerge(unique_users) AS unique_customers,
--     quantileMerge(0.5)(amount_p50) AS median_amount
-- FROM payment_metrics_agg
-- WHERE merchant_id = 5001
-- GROUP BY metric_date
-- ORDER BY metric_date;


-- -----------------------------------------------------------------------------
-- COMBINING WITH MATERIALIZED VIEWS (Automatic Population)
-- -----------------------------------------------------------------------------
-- The real power comes from auto-populating via materialized views!
-- See 05_materialized_views.sql for this pattern.


-- -----------------------------------------------------------------------------
-- SummingMergeTree - A Simpler Alternative
-- -----------------------------------------------------------------------------
-- If you only need SUM (not AVG, COUNT, etc.), SummingMergeTree is simpler.
-- It automatically sums numeric columns during merges.

DROP TABLE IF EXISTS revenue_daily_sum;

CREATE TABLE revenue_daily_sum
(
    merchant_id UInt64,
    metric_date Date,
    -- These will be summed automatically during merges
    total_amount Decimal(18, 2),
    transaction_count UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(metric_date)
ORDER BY (merchant_id, metric_date);

-- Insert example data
INSERT INTO revenue_daily_sum VALUES (5001, today(), 100.00, 5);
INSERT INTO revenue_daily_sum VALUES (5001, today(), 200.00, 10);

-- After merge (or using FINAL):
-- merchant_id=5001, total_amount=300.00, transaction_count=15


-- -----------------------------------------------------------------------------
-- COMPARISON: When to Use Which Engine
-- -----------------------------------------------------------------------------
--
-- ┌─────────────────────────┬──────────────────────┬─────────────────────────┐
-- │ Use Case                │ Engine               │ Notes                   │
-- ├─────────────────────────┼──────────────────────┼─────────────────────────┤
-- │ Only need SUM           │ SummingMergeTree     │ Simpler, automatic      │
-- │ Multiple aggregates     │ AggregatingMergeTree │ Most flexible           │
-- │ AVG, COUNT, Percentiles │ AggregatingMergeTree │ Required for these      │
-- │ Distinct counts         │ AggregatingMergeTree │ Use uniqState/uniqMerge │
-- │ CDC with updates        │ ReplacingMergeTree   │ For dimension tables    │
-- │ Raw fact data           │ MergeTree            │ Base engine             │
-- └─────────────────────────┴──────────────────────┴─────────────────────────┘
--
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- ADVANCED: Multiple Aggregation States in One Query
-- -----------------------------------------------------------------------------

-- Example: Get comprehensive merchant statistics
-- SELECT
--     merchant_id,
--     category,
--
--     -- Revenue metrics
--     sumMerge(total_amount) AS total_revenue,
--     avgMerge(avg_amount) AS average_transaction,
--     minMerge(min_amount) AS smallest_transaction,
--     maxMerge(max_amount) AS largest_transaction,
--
--     -- Volume metrics
--     countMerge(transaction_count) AS total_transactions,
--     uniqMerge(unique_users) AS unique_customers,
--
--     -- Percentiles
--     quantileMerge(0.5)(amount_p50) AS median_transaction,
--     quantileMerge(0.99)(amount_p99) AS p99_transaction,
--
--     -- Derived metrics
--     sumMerge(total_amount) / countMerge(transaction_count) AS calculated_avg
--
-- FROM payment_metrics_agg
-- WHERE metric_date >= today() - 30
-- GROUP BY merchant_id, category
-- ORDER BY total_revenue DESC;


-- -----------------------------------------------------------------------------
-- GOTCHAS
-- -----------------------------------------------------------------------------
-- 1. NEVER query AggregateFunction columns directly without -Merge
--    BAD:  SELECT total_amount FROM payment_metrics_agg;  -- Returns binary blob!
--    GOOD: SELECT sumMerge(total_amount) FROM payment_metrics_agg;
--
-- 2. INSERT must use -State functions
--    BAD:  INSERT ... SELECT sum(amount) ...
--    GOOD: INSERT ... SELECT sumState(amount) ...
--
-- 3. Data types must match exactly
--    sumState(toDecimal64(amount, 2)) ≠ sumState(amount) if types differ
--
-- 4. Use FINAL or GROUP BY to get merged results
--    SELECT ... FROM table FINAL ... (forces merge at query time)
-- -----------------------------------------------------------------------------
