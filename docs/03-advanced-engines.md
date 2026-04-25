# Advanced MergeTree Engines

## Engine Family Overview

All engines in the MergeTree family share the same base capabilities:
- Columnar storage
- Background merges
- Sparse primary index
- Partitioning support

Each specialized engine adds specific behavior during merges:

```
                         MergeTree (base)
                               │
       ┌───────────┬───────────┼───────────┬───────────┐
       │           │           │           │           │
       ▼           ▼           ▼           ▼           ▼
  Replacing    Summing    Aggregating  Collapsing  VersionedCollapsing
  MergeTree    MergeTree   MergeTree   MergeTree      MergeTree
     │            │            │           │              │
(deduplication) (auto-sum) (arbitrary) (sign-based)  (versioned
                            aggregates)  collapse)     collapse)
```

## ReplacingMergeTree

### When to Use

- CDC (Change Data Capture) pipelines
- At-least-once message delivery (Kafka)
- Upsert patterns (INSERT as UPDATE)
- Slowly Changing Dimensions (SCD Type 1)

### How It Works

```sql
CREATE TABLE users
(
    user_id UInt64,
    name String,
    email String,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;
```

```
Insert #1: user_id=1, name="Alice", updated_at=10:00
Insert #2: user_id=1, name="Alice Smith", updated_at=10:05

Before merge:
┌───────────┬──────────────┬────────────┐
│ user_id   │ name         │ updated_at │
├───────────┼──────────────┼────────────┤
│ 1         │ Alice        │ 10:00      │ ← Will be removed
│ 1         │ Alice Smith  │ 10:05      │ ← Will be kept
└───────────┴──────────────┴────────────┘

After merge:
┌───────────┬──────────────┬────────────┐
│ user_id   │ name         │ updated_at │
├───────────┼──────────────┼────────────┤
│ 1         │ Alice Smith  │ 10:05      │
└───────────┴──────────────┴────────────┘
```

### Critical Gotcha: Deduplication is Eventual!

```sql
-- Immediately after INSERT, duplicates may exist!
SELECT * FROM users WHERE user_id = 1;
-- Returns BOTH rows!

-- Option 1: Use FINAL (forces dedup at query time)
SELECT * FROM users FINAL WHERE user_id = 1;
-- Returns only latest row

-- Option 2: Wait for background merge (unreliable timing)

-- Option 3: Force merge (resource intensive, use sparingly)
OPTIMIZE TABLE users FINAL;
```

### ReplacingMergeTree Best Practices

```sql
-- Always include a version column
ENGINE = ReplacingMergeTree(updated_at)

-- ORDER BY must include the "unique key"
ORDER BY (user_id)  -- user_id defines uniqueness

-- For queries needing deduped data, use FINAL
SELECT * FROM users FINAL WHERE ...

-- Or use argMax pattern (more flexible)
SELECT
    user_id,
    argMax(name, updated_at) AS name,
    argMax(email, updated_at) AS email,
    max(updated_at) AS updated_at
FROM users
WHERE user_id = 1
GROUP BY user_id;
```

## SummingMergeTree

### When to Use

- Pre-aggregated metrics
- Counter tables
- Simple rollups (only SUM)

### How It Works

```sql
CREATE TABLE daily_sales
(
    date Date,
    product_id UInt32,
    quantity UInt64,      -- Will be summed
    revenue Decimal(18,2) -- Will be summed
)
ENGINE = SummingMergeTree()
ORDER BY (date, product_id);
```

```sql
INSERT INTO daily_sales VALUES ('2024-01-15', 1, 10, 100.00);
INSERT INTO daily_sales VALUES ('2024-01-15', 1, 5, 50.00);
INSERT INTO daily_sales VALUES ('2024-01-15', 1, 3, 30.00);
```

```
Before merge:
┌────────────┬────────────┬──────────┬─────────┐
│ date       │ product_id │ quantity │ revenue │
├────────────┼────────────┼──────────┼─────────┤
│ 2024-01-15 │ 1          │ 10       │ 100.00  │
│ 2024-01-15 │ 1          │ 5        │ 50.00   │
│ 2024-01-15 │ 1          │ 3        │ 30.00   │
└────────────┴────────────┴──────────┴─────────┘

After merge:
┌────────────┬────────────┬──────────┬─────────┐
│ date       │ product_id │ quantity │ revenue │
├────────────┼────────────┼──────────┼─────────┤
│ 2024-01-15 │ 1          │ 18       │ 180.00  │
└────────────┴────────────┴──────────┴─────────┘
```

### Querying SummingMergeTree

```sql
-- Use FINAL or GROUP BY to ensure merged results
SELECT date, product_id, sum(quantity), sum(revenue)
FROM daily_sales
GROUP BY date, product_id;
```

## AggregatingMergeTree

### When to Use

- Complex aggregations (AVG, COUNT, percentiles)
- Pre-aggregation for dashboards
- Materialized view targets

### How It Works

Uses special `-State` and `-Merge` combinators:

```sql
CREATE TABLE metrics_agg
(
    merchant_id UInt64,
    date Date,
    -- These store intermediate aggregation states
    total_amount AggregateFunction(sum, Decimal(18,2)),
    transaction_count AggregateFunction(count),
    avg_amount AggregateFunction(avg, Decimal(18,2)),
    unique_users AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (merchant_id, date);
```

### Inserting Data

```sql
-- MUST use -State functions!
INSERT INTO metrics_agg
SELECT
    merchant_id,
    toDate(created_at) AS date,
    sumState(amount),
    countState(),
    avgState(amount),
    uniqState(user_id)
FROM transactions
GROUP BY merchant_id, toDate(created_at);
```

### Querying Data

```sql
-- MUST use -Merge functions!
SELECT
    merchant_id,
    date,
    sumMerge(total_amount) AS revenue,
    countMerge(transaction_count) AS txn_count,
    avgMerge(avg_amount) AS avg_txn,
    uniqMerge(unique_users) AS customers
FROM metrics_agg
GROUP BY merchant_id, date;
```

### State/Merge Visualization

```
Raw data:
Batch 1: amounts = [100, 200, 150]
Batch 2: amounts = [300, 250]

sumState() creates partial sums:
Batch 1: sumState([100,200,150]) → State{450}
Batch 2: sumState([300,250])     → State{550}

During merge:
State{450} + State{550} → State{1000}

sumMerge() extracts final value:
sumMerge(State{1000}) → 1000
```

## CollapsingMergeTree

### When to Use

- Mutable data with efficient deletes
- Event sourcing patterns
- When you need to "undo" inserts

### How It Works

Uses a sign column (+1 = insert, -1 = delete):

```sql
CREATE TABLE user_balances
(
    user_id UInt64,
    balance Decimal(18,2),
    sign Int8  -- +1 or -1
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY user_id;
```

```sql
-- Initial balance
INSERT INTO user_balances VALUES (1, 100.00, 1);

-- To update: insert cancel row + new row
INSERT INTO user_balances VALUES
    (1, 100.00, -1),  -- Cancel old
    (1, 150.00, 1);   -- Insert new
```

```
Before merge:
┌─────────┬─────────┬──────┐
│ user_id │ balance │ sign │
├─────────┼─────────┼──────┤
│ 1       │ 100.00  │ 1    │
│ 1       │ 100.00  │ -1   │ ← Cancels above
│ 1       │ 150.00  │ 1    │
└─────────┴─────────┴──────┘

After merge:
┌─────────┬─────────┬──────┐
│ user_id │ balance │ sign │
├─────────┼─────────┼──────┤
│ 1       │ 150.00  │ 1    │
└─────────┴─────────┴──────┘
```

## VersionedCollapsingMergeTree

### When to Use

- CDC with out-of-order events
- When collapse order matters
- Distributed systems where events may arrive out of order

```sql
CREATE TABLE user_events
(
    user_id UInt64,
    balance Decimal(18,2),
    sign Int8,
    version UInt64  -- Ensures correct collapse order
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY user_id;
```

### How It Handles Out-of-Order Events

The key difference from CollapsingMergeTree is that version determines collapse order, not insertion order:

```sql
-- Events arrive out of order from distributed system
INSERT INTO user_events VALUES (1, 100.00, 1, 1);   -- v1: Initial balance
INSERT INTO user_events VALUES (1, 200.00, 1, 3);   -- v3: Arrives before v2!
INSERT INTO user_events VALUES (1, 100.00, -1, 1);  -- Cancel v1
INSERT INTO user_events VALUES (1, 150.00, 1, 2);   -- v2: Arrives late
INSERT INTO user_events VALUES (1, 150.00, -1, 2);  -- Cancel v2
```

```
Before merge (insertion order):
┌─────────┬─────────┬──────┬─────────┐
│ user_id │ balance │ sign │ version │
├─────────┼─────────┼──────┼─────────┤
│ 1       │ 100.00  │ 1    │ 1       │
│ 1       │ 200.00  │ 1    │ 3       │
│ 1       │ 100.00  │ -1   │ 1       │ ← Cancels v1
│ 1       │ 150.00  │ 1    │ 2       │
│ 1       │ 150.00  │ -1   │ 2       │ ← Cancels v2
└─────────┴─────────┴──────┴─────────┘

After merge (sorted by version, then collapsed):
┌─────────┬─────────┬──────┬─────────┐
│ user_id │ balance │ sign │ version │
├─────────┼─────────┼──────┼─────────┤
│ 1       │ 200.00  │ 1    │ 3       │ ← Only latest survives
└─────────┴─────────┴──────┴─────────┘
```

CollapsingMergeTree would fail here because it relies on insertion order for matching +1/-1 pairs.

## Engine Selection Guide

```
┌────────────────────────────────────────────────────────────────┐
│                  Which Engine Should I Use?                    │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Need to deduplicate rows?                                     │
│    └── Yes → ReplacingMergeTree                               │
│                                                                │
│  Only need SUM aggregation?                                    │
│    └── Yes → SummingMergeTree (simpler than Aggregating)      │
│                                                                │
│  Need AVG, COUNT, percentiles, uniq?                          │
│    └── Yes → AggregatingMergeTree                             │
│                                                                │
│  Need to delete/update with efficiency?                        │
│    └── Yes → CollapsingMergeTree                              │
│    └── + Out-of-order events? → VersionedCollapsingMergeTree  │
│                                                                │
│  Just storing raw data?                                        │
│    └── MergeTree (base engine)                                │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

## Performance Comparison

| Engine | Insert Speed | Query Speed | Storage | Complexity |
|--------|-------------|-------------|---------|------------|
| MergeTree | Fastest | Fast | Largest | Lowest |
| ReplacingMergeTree | Fast | Fast (with FINAL) | Medium | Low |
| SummingMergeTree | Fast | Fastest | Smallest | Medium |
| AggregatingMergeTree | Medium | Fastest | Smallest | Highest |
| CollapsingMergeTree | Medium | Medium | Medium | Medium |

## Common Mistakes

### Mistake 1: Forgetting -State/-Merge

```sql
-- WRONG: Returns binary blob!
SELECT total_amount FROM metrics_agg;

-- CORRECT: Use -Merge
SELECT sumMerge(total_amount) FROM metrics_agg;
```

### Mistake 2: Using FINAL on huge tables

```sql
-- SLOW: Forces merge of entire table
SELECT * FROM huge_table FINAL;

-- BETTER: Use FINAL with filters
SELECT * FROM huge_table FINAL WHERE user_id = 123;

-- OR: Use GROUP BY pattern
SELECT user_id, argMax(data, version) FROM huge_table GROUP BY user_id;
```

### Mistake 3: Wrong ORDER BY for deduplication

```sql
-- If you want to dedupe by (user_id, event_type):
ORDER BY (user_id, event_type)  -- Both columns define uniqueness

-- NOT:
ORDER BY (user_id)  -- Only user_id defines uniqueness
```
