-- =============================================================================
-- 08_analytical_queries.sql
-- ClickHouse Fundamentals - Analytical Query Patterns & Optimization
-- =============================================================================
-- This file demonstrates real-world analytical queries and optimization
-- techniques for ClickHouse.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- QUERY OPTIMIZATION BASICS
-- -----------------------------------------------------------------------------
-- Key principles:
--   1. Use PRIMARY KEY columns in WHERE for index utilization
--   2. PARTITION pruning via date ranges
--   3. PREWHERE for light filters before heavy column reads
--   4. Avoid SELECT * - specify only needed columns
--   5. Use LIMIT early in exploratory queries
-- -----------------------------------------------------------------------------


-- =============================================================================
-- REVENUE ANALYTICS
-- =============================================================================

-- Daily revenue summary with multiple aggregations
-- Uses: Partition pruning, GROUP BY optimization
SELECT
    toDate(created_at) AS txn_date,
    count() AS total_transactions,
    sum(amount) AS total_revenue,
    avg(amount) AS avg_transaction,
    min(amount) AS min_transaction,
    max(amount) AS max_transaction,
    uniq(user_id) AS unique_customers,
    uniq(merchant_id) AS unique_merchants
FROM transactions
WHERE created_at >= today() - 30  -- Partition pruning
GROUP BY txn_date
ORDER BY txn_date DESC;


-- Revenue by merchant with ranking
SELECT
    merchant_id,
    count() AS transaction_count,
    sum(amount) AS total_revenue,
    avg(amount) AS avg_transaction,
    uniq(user_id) AS unique_customers,
    row_number() OVER (ORDER BY sum(amount) DESC) AS revenue_rank
FROM transactions
WHERE created_at >= today() - 30
GROUP BY merchant_id
ORDER BY total_revenue DESC
LIMIT 20;


-- Hourly revenue patterns (time-series analysis)
SELECT
    toHour(created_at) AS hour_of_day,
    toDayOfWeek(created_at) AS day_of_week,
    count() AS transactions,
    sum(amount) AS revenue,
    avg(amount) AS avg_amount
FROM transactions
WHERE created_at >= today() - 90
GROUP BY hour_of_day, day_of_week
ORDER BY day_of_week, hour_of_day;


-- =============================================================================
-- USER ANALYTICS
-- =============================================================================

-- User spending profiles
SELECT
    user_id,
    count() AS total_transactions,
    sum(amount) AS total_spent,
    avg(amount) AS avg_transaction,
    min(created_at) AS first_transaction,
    max(created_at) AS last_transaction,
    dateDiff('day', min(created_at), max(created_at)) AS active_days,
    sum(amount) / greatest(dateDiff('day', min(created_at), max(created_at)), 1) AS daily_avg
FROM transactions
WHERE user_id = 1001  -- Uses primary key index!
GROUP BY user_id;


-- Cohort analysis: Monthly retention
SELECT
    toStartOfMonth(first_txn) AS cohort_month,
    dateDiff('month', first_txn, created_at) AS months_since_first,
    uniq(t.user_id) AS users
FROM transactions t
INNER JOIN (
    SELECT user_id, min(created_at) AS first_txn
    FROM transactions
    GROUP BY user_id
) first_txns ON t.user_id = first_txns.user_id
GROUP BY cohort_month, months_since_first
ORDER BY cohort_month, months_since_first;


-- Top spenders by category
SELECT
    category,
    user_id,
    sum(amount) AS total_spent,
    count() AS transactions,
    row_number() OVER (PARTITION BY category ORDER BY sum(amount) DESC) AS rank_in_category
FROM transactions
WHERE created_at >= today() - 30
GROUP BY category, user_id
QUALIFY rank_in_category <= 5  -- Top 5 per category
ORDER BY category, rank_in_category;


-- =============================================================================
-- TIME-SERIES ANALYTICS
-- =============================================================================

-- Rolling 7-day average revenue
SELECT
    txn_date,
    daily_revenue,
    avg(daily_revenue) OVER (
        ORDER BY txn_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg,
    daily_revenue - avg(daily_revenue) OVER (
        ORDER BY txn_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS deviation
FROM (
    SELECT
        toDate(created_at) AS txn_date,
        sum(amount) AS daily_revenue
    FROM transactions
    WHERE created_at >= today() - 60
    GROUP BY txn_date
)
ORDER BY txn_date;


-- Week-over-week growth
WITH weekly AS (
    SELECT
        toStartOfWeek(created_at) AS week_start,
        sum(amount) AS revenue
    FROM transactions
    WHERE created_at >= today() - 90
    GROUP BY week_start
)
SELECT
    week_start,
    revenue,
    lagInFrame(revenue, 1) OVER (ORDER BY week_start) AS prev_week_revenue,
    round((revenue - lagInFrame(revenue, 1) OVER (ORDER BY week_start))
        / lagInFrame(revenue, 1) OVER (ORDER BY week_start) * 100, 2) AS wow_growth_pct
FROM weekly
ORDER BY week_start;


-- =============================================================================
-- CATEGORY ANALYTICS
-- =============================================================================

-- Category performance breakdown
SELECT
    category,
    count() AS transactions,
    sum(amount) AS revenue,
    avg(amount) AS avg_transaction,
    uniq(user_id) AS unique_users,
    round(sum(amount) / sum(sum(amount)) OVER () * 100, 2) AS revenue_share_pct,
    round(count() / sum(count()) OVER () * 100, 2) AS transaction_share_pct
FROM transactions
WHERE created_at >= today() - 30
GROUP BY category
ORDER BY revenue DESC;


-- Category trends over time
SELECT
    toStartOfMonth(created_at) AS month,
    category,
    sum(amount) AS revenue,
    count() AS transactions
FROM transactions
WHERE created_at >= today() - 365
GROUP BY month, category
ORDER BY month, category;


-- =============================================================================
-- STATUS ANALYTICS
-- =============================================================================

-- Transaction status funnel
SELECT
    status,
    count() AS transactions,
    sum(amount) AS total_amount,
    round(count() / sum(count()) OVER () * 100, 2) AS pct_of_total
FROM transactions
WHERE created_at >= today() - 7
GROUP BY status
ORDER BY
    CASE status
        WHEN 'pending' THEN 1
        WHEN 'processing' THEN 2
        WHEN 'completed' THEN 3
        WHEN 'failed' THEN 4
        WHEN 'refunded' THEN 5
        WHEN 'cancelled' THEN 6
    END;


-- Failed transaction analysis
SELECT
    toDate(created_at) AS txn_date,
    category,
    count() AS failed_count,
    sum(amount) AS failed_amount,
    round(count() / (
        SELECT count() FROM transactions
        WHERE created_at >= today() - 7 AND status = 'failed'
    ) * 100, 2) AS pct_of_failures
FROM transactions
WHERE status = 'failed'
  AND created_at >= today() - 7
GROUP BY txn_date, category
ORDER BY failed_count DESC;


-- =============================================================================
-- OPTIMIZATION TECHNIQUES
-- =============================================================================

-- Using PREWHERE for better performance
-- PREWHERE filters before reading all columns
SELECT
    user_id,
    sum(amount) AS total
FROM transactions
PREWHERE created_at >= today() - 30  -- Evaluated first, efficiently
WHERE status = 'completed'  -- Evaluated after PREWHERE
GROUP BY user_id
ORDER BY total DESC
LIMIT 100;


-- Sampling for approximate results on huge tables
-- NOTE: SAMPLE requires SAMPLE BY in CREATE TABLE, e.g.:
--   SAMPLE BY intHash64(user_id)
-- The transactions table has no SAMPLE BY, so run a full scan instead
-- (fine for this demo; on billion-row tables, add SAMPLE BY and use SAMPLE 0.1)
SELECT
    category,
    count() AS total_count,
    sum(amount) AS total_revenue
FROM transactions
WHERE created_at >= today() - 365
GROUP BY category
ORDER BY total_revenue DESC;


-- Using FINAL efficiently (for ReplacingMergeTree tables)
-- Only use FINAL when necessary, and always with filters
SELECT
    user_id,
    sum(amount) AS total
FROM transactions_dedup FINAL
WHERE user_id = 1001  -- Filter first to limit FINAL scope
GROUP BY user_id;


-- =============================================================================
-- EXPLAIN AND DEBUGGING
-- =============================================================================

-- Basic EXPLAIN
-- EXPLAIN
-- SELECT merchant_id, sum(amount)
-- FROM transactions
-- WHERE user_id = 1001
-- GROUP BY merchant_id;

-- EXPLAIN with index info
-- EXPLAIN indexes = 1
-- SELECT * FROM transactions WHERE user_id = 1001 AND created_at > '2024-01-01';

-- EXPLAIN with actions (detailed plan)
-- EXPLAIN actions = 1
-- SELECT category, count(), sum(amount)
-- FROM transactions
-- WHERE created_at >= today() - 30
-- GROUP BY category;


-- =============================================================================
-- SYSTEM TABLES FOR MONITORING
-- =============================================================================

-- Table sizes and compression
-- SELECT
--     table,
--     formatReadableSize(sum(bytes_on_disk)) AS disk_size,
--     sum(rows) AS total_rows,
--     round(sum(data_uncompressed_bytes) / sum(bytes_on_disk), 2) AS compression_ratio
-- FROM system.parts
-- WHERE active AND database = currentDatabase()
-- GROUP BY table
-- ORDER BY sum(bytes_on_disk) DESC;

-- Recent slow queries
-- SELECT
--     query,
--     query_duration_ms,
--     read_rows,
--     formatReadableSize(read_bytes) AS read_size,
--     result_rows
-- FROM system.query_log
-- WHERE type = 'QueryFinish'
--   AND query_duration_ms > 1000
--   AND event_date >= today() - 1
-- ORDER BY query_duration_ms DESC
-- LIMIT 20;

-- Part count per table (too many = need OPTIMIZE)
-- SELECT
--     table,
--     partition,
--     count() AS part_count,
--     sum(rows) AS total_rows
-- FROM system.parts
-- WHERE active AND database = currentDatabase()
-- GROUP BY table, partition
-- HAVING part_count > 10
-- ORDER BY part_count DESC;

-- Ongoing merges
-- SELECT
--     table,
--     elapsed,
--     progress,
--     num_parts,
--     formatReadableSize(total_size_bytes_compressed) AS size
-- FROM system.merges;


-- =============================================================================
-- DISTRIBUTED QUERY PATTERN (Single Node Reference)
-- =============================================================================
-- On a distributed cluster, you would create a Distributed table like this:
--
-- CREATE TABLE transactions_distributed
-- ENGINE = Distributed(
--     'cluster_name',      -- Cluster defined in config.xml
--     'database_name',     -- Database containing local tables
--     'transactions',      -- Local table name on each shard
--     rand()               -- Sharding key (rand() for even distribution)
-- );
--
-- Then queries to transactions_distributed automatically fan out to all shards
-- and aggregate results.
--
-- For single-node setups, this is informational only.
-- See ClickHouse documentation for cluster configuration.
