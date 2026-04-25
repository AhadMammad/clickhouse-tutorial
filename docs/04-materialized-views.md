# Materialized Views in ClickHouse

## Key Concept: MVs are Triggers, Not Snapshots!

In PostgreSQL, a materialized view is a **snapshot** that must be manually refreshed:

```sql
-- PostgreSQL style (NOT how ClickHouse works!)
CREATE MATERIALIZED VIEW mv AS SELECT ...;
REFRESH MATERIALIZED VIEW mv;  -- Manual refresh
```

In ClickHouse, a materialized view is an **INSERT trigger**:

```sql
-- ClickHouse style
CREATE MATERIALIZED VIEW mv TO target_table AS SELECT ...;
-- Automatically processes each INSERT to source table
-- NO manual refresh needed!
```

## How It Works

```
                    INSERT Flow
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│                   Source Table                          │
│                   (transactions)                        │
└─────────────────────────┬───────────────────────────────┘
                          │
                          │ Trigger fires
                          ▼
┌─────────────────────────────────────────────────────────┐
│                Materialized View                        │
│              (mv_hourly_stats)                          │
│                                                         │
│   SELECT merchant_id, toStartOfHour(created_at),       │
│          sumState(amount), countState()                 │
│   FROM transactions                                     │
│   GROUP BY merchant_id, toStartOfHour(created_at)      │
└─────────────────────────┬───────────────────────────────┘
                          │
                          │ Transformed data
                          ▼
┌─────────────────────────────────────────────────────────┐
│                   Target Table                          │
│              (hourly_stats_target)                      │
│         ENGINE = AggregatingMergeTree()                 │
└─────────────────────────────────────────────────────────┘
```

## Basic Syntax

### Recommended: Explicit Target Table

```sql
-- Step 1: Create target table
CREATE TABLE hourly_stats_target
(
    merchant_id UInt64,
    hour DateTime,
    total_amount AggregateFunction(sum, Decimal(18,2)),
    txn_count AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
ORDER BY (merchant_id, hour);

-- Step 2: Create MV pointing to target
CREATE MATERIALIZED VIEW mv_hourly_stats
TO hourly_stats_target
AS SELECT
    merchant_id,
    toStartOfHour(created_at) AS hour,
    sumState(amount) AS total_amount,
    countState() AS txn_count
FROM transactions
GROUP BY merchant_id, toStartOfHour(created_at);
```

### Not Recommended: Implicit Storage

```sql
-- Creates hidden .inner.mv_hourly_stats table
-- Harder to manage, can't easily query target
CREATE MATERIALIZED VIEW mv_hourly_stats
ENGINE = AggregatingMergeTree()
ORDER BY (merchant_id, hour)
AS SELECT ...;
```

## Common Patterns

### Pattern 1: Pre-Aggregation Pipeline

```sql
-- Source: Raw transactions (millions of rows)
-- Target: Hourly summaries (much smaller)

CREATE TABLE daily_merchant_summary_target
(
    merchant_id UInt64,
    date Date,
    revenue AggregateFunction(sum, Decimal(18,2)),
    orders AggregateFunction(count),
    customers AggregateFunction(uniq, UInt64),
    avg_order AggregateFunction(avg, Decimal(18,2))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (merchant_id, date);

CREATE MATERIALIZED VIEW mv_daily_merchant_summary
TO daily_merchant_summary_target
AS SELECT
    merchant_id,
    toDate(created_at) AS date,
    sumState(amount) AS revenue,
    countState() AS orders,
    uniqState(user_id) AS customers,
    avgState(amount) AS avg_order
FROM transactions
WHERE status = 'completed'
GROUP BY merchant_id, toDate(created_at);
```

### Pattern 2: Fan-Out (Multiple MVs from Same Source)

```sql
-- One source table, multiple aggregations
-- Each MV processes every INSERT independently

-- MV 1: By merchant
CREATE MATERIALIZED VIEW mv_by_merchant TO merchant_stats_target AS
SELECT merchant_id, sumState(amount) ...
FROM transactions GROUP BY merchant_id;

-- MV 2: By category
CREATE MATERIALIZED VIEW mv_by_category TO category_stats_target AS
SELECT category, sumState(amount) ...
FROM transactions GROUP BY category;

-- MV 3: By user
CREATE MATERIALIZED VIEW mv_by_user TO user_stats_target AS
SELECT user_id, sumState(amount) ...
FROM transactions GROUP BY user_id;
```

### Pattern 3: Chained MVs (Multi-Level Aggregation)

```sql
-- Level 1: Raw → Hourly
transactions → mv_hourly → hourly_stats

-- Level 2: Hourly → Daily
hourly_stats → mv_daily → daily_stats

-- Level 3: Daily → Monthly
daily_stats → mv_monthly → monthly_stats
```

### Pattern 4: Data Enrichment

```sql
-- Add computed fields without JOIN at query time
CREATE TABLE enriched_transactions_target
(
    transaction_id UUID,
    user_id UInt64,
    amount Decimal(18,2),
    amount_usd Decimal(18,2),  -- Converted
    hour_of_day UInt8,         -- Extracted
    day_of_week UInt8,         -- Extracted
    is_weekend UInt8           -- Computed
)
ENGINE = MergeTree()
ORDER BY (user_id, transaction_id);

CREATE MATERIALIZED VIEW mv_enriched
TO enriched_transactions_target
AS SELECT
    transaction_id,
    user_id,
    amount,
    -- Use dictionary lookup for currency conversion
    -- Requires: CREATE DICTIONARY currency_rates (...)
    amount * dictGet('currency_rates', 'rate_to_usd', currency) AS amount_usd,
    toHour(created_at) AS hour_of_day,
    toDayOfWeek(created_at) AS day_of_week,
    toDayOfWeek(created_at) IN (6, 7) AS is_weekend
FROM transactions;
```

## Projections vs Materialized Views

| Aspect | Projection | Materialized View |
|--------|-----------|-------------------|
| Storage | Inside source table | Separate table |
| Automatic selection | Yes (optimizer chooses) | No (explicit query) |
| Transformations | Limited (reorder, simple agg) | Full SQL |
| Cross-table JOINs | No | Yes |
| Management | Automatic | Manual (separate table) |
| Use case | Secondary sort order | Complex aggregations |

### When to Use Projections

```sql
-- Good for: Same data, different sort order
-- Main table sorted by (user_id, created_at)
-- Projection sorted by (merchant_id, created_at)

ALTER TABLE transactions
ADD PROJECTION proj_by_merchant
(
    SELECT * ORDER BY (merchant_id, created_at)
);
```

### When to Use Materialized Views

```sql
-- Good for: Aggregations, transformations, JOINs
-- Pre-compute daily summaries from raw transactions

CREATE MATERIALIZED VIEW mv_daily_summary
TO daily_summary_target
AS SELECT
    toDate(created_at) AS date,
    count() AS orders,
    sum(amount) AS revenue
FROM transactions
WHERE status = 'completed'
GROUP BY date;
```

## Important Gotchas

### Gotcha 1: MVs Don't Process Historical Data

```sql
-- Create MV
CREATE MATERIALIZED VIEW mv TO target AS SELECT ... FROM source;

-- Only NEW inserts are processed!
-- Existing data in source is NOT processed

-- To backfill, INSERT manually:
INSERT INTO target
SELECT ... FROM source WHERE created_at < now();
```

### Gotcha 2: MV Errors Can Block Inserts

```sql
-- If MV query fails, the original INSERT may fail too!
-- Always test MV logic thoroughly before deployment

-- Example: Division by zero in MV would block inserts
CREATE MATERIALIZED VIEW mv TO target AS
SELECT amount / quantity AS unit_price  -- Fails if quantity = 0!
FROM source;
```

### Gotcha 3: DROP VIEW Doesn't Drop Target

```sql
-- This only drops the trigger, NOT the data!
DROP VIEW mv_hourly_stats;

-- Data remains in target table
-- Must drop separately if desired:
DROP TABLE hourly_stats_target;
```

### Gotcha 4: JOINs in MVs Read Dimension Tables at Insert Time

```sql
-- JOINs DO work in MVs, but with an important caveat:
-- The joined table is read AT INSERT TIME, not at MV creation time

CREATE MATERIALIZED VIEW mv TO target AS
SELECT t.*, m.name
FROM transactions t
JOIN merchants m ON t.merchant_id = m.merchant_id;

-- This works, BUT:
-- 1. If merchant name changes AFTER the transaction was inserted,
--    the MV will have the OLD name (snapshot at insert time)
-- 2. If merchant doesn't exist at insert time, the row may be dropped
--    (for INNER JOIN) or have NULLs (for LEFT JOIN)

-- Better alternatives:
-- Option 1: Use dictionaries (updates automatically)
SELECT t.*, dictGet('merchants_dict', 'name', t.merchant_id) AS merchant_name
FROM transactions t;

-- Option 2: Denormalize in the application layer before inserting
```

## Debugging MVs

### List All MVs

```sql
SELECT name, engine, create_table_query
FROM system.tables
WHERE engine = 'MaterializedView';
```

### Check for Errors

```sql
SELECT query, exception, event_time
FROM system.query_log
WHERE query LIKE '%mv_%' AND exception != ''
ORDER BY event_time DESC
LIMIT 10;
```

### Monitor Part Count

```sql
-- Too many parts = MV not keeping up
SELECT table, count() AS parts
FROM system.parts
WHERE active AND table LIKE '%_target'
GROUP BY table
HAVING parts > 100;
```

## Best Practices

1. **Always use explicit target tables** (`TO table_name`)
2. **Name conventions**: `mv_*` for views, `*_target` for tables
3. **Test MV query independently** before creating
4. **Monitor part count** on target tables
5. **Backfill historical data** after creating MV
6. **Use AggregatingMergeTree** for aggregation MVs
7. **Filter early** in MV query (WHERE clause)
