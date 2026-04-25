# Query Optimization in ClickHouse

## Understanding Query Execution

### The Query Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                         Query Pipeline                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Parse SQL                                                   │
│     └── Syntax validation, AST creation                        │
│                                                                 │
│  2. Analyze & Optimize                                          │
│     └── Predicate pushdown, partition pruning                  │
│                                                                 │
│  3. Build Execution Plan                                        │
│     └── Choose indexes, projections                            │
│                                                                 │
│  4. Execute                                                     │
│     ├── Read from disk (or cache)                              │
│     ├── Decompress                                             │
│     ├── Apply PREWHERE                                         │
│     ├── Apply WHERE                                            │
│     └── Aggregate, sort, limit                                 │
│                                                                 │
│  5. Return Results                                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Reading EXPLAIN Output

### Basic EXPLAIN

```sql
EXPLAIN
SELECT merchant_id, sum(amount)
FROM transactions
WHERE created_at >= '2024-01-01'
GROUP BY merchant_id;
```

Output:
```
┌─explain─────────────────────────────────────────────────────────┐
│ Expression ((Project names + Projection))                       │
│   Aggregating                                                   │
│     Expression (Before GROUP BY)                                │
│       Filter (WHERE)                                            │
│         ReadFromMergeTree (default.transactions)               │
└─────────────────────────────────────────────────────────────────┘
```

### EXPLAIN with Indexes

```sql
EXPLAIN indexes = 1
SELECT * FROM transactions
WHERE user_id = 1001 AND created_at >= '2024-01-01';
```

Output:
```
┌─explain──────────────────────────────────────────────────────────┐
│ Expression (Project names)                                       │
│   ReadFromMergeTree (default.transactions)                      │
│     Indexes:                                                     │
│       PrimaryKey                                                 │
│         Keys: user_id, created_at                                │
│         Condition: (user_id = 1001) AND (created_at >= '...')   │
│         Parts: 3/12                                              │  ← Only 3 of 12 parts read!
│         Granules: 45/1000                                        │  ← Only 45 of 1000 granules!
│       Partition                                                  │
│         Condition: (created_at >= '2024-01-01')                 │
│         Parts: 3/12                                              │
└──────────────────────────────────────────────────────────────────┘
```

### Key Metrics to Watch

| Metric | Good | Bad | Meaning |
|--------|------|-----|---------|
| Parts read | Low ratio (e.g., 3/100) | High ratio (e.g., 95/100) | How many parts scanned |
| Granules read | Low ratio | High ratio | How many granules scanned |
| Partition pruning | "Parts: 2/24" | "Parts: 24/24" | Partition filter working? |
| Index used | "PrimaryKey" shown | Not shown | Primary index being used? |

## Designing Efficient Schemas

### ORDER BY Column Order Matters!

```sql
-- Your queries determine optimal ORDER BY

-- If most queries filter by user_id first:
WHERE user_id = 123 AND created_at > '2024-01-01'
-- Then: ORDER BY (user_id, created_at) ✓

-- If most queries filter by date first:
WHERE created_at > '2024-01-01' AND user_id = 123
-- Then: ORDER BY (created_at, user_id) might be better

-- If queries vary:
-- Consider projections for alternative sort orders
```

### Primary Key Design Principles

```
┌───────────────────────────────────────────────────────────────┐
│              Column Position in ORDER BY                      │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  Position 1    Position 2    Position 3    Position 4        │
│  (best)        (good)        (okay)        (poor)            │
│                                                               │
│  Binary        Efficient     Less          Mostly            │
│  search        with pos 1    efficient     full scan         │
│  works         fixed                                          │
│                                                               │
│  Example: ORDER BY (country, city, user_id, timestamp)       │
│                                                               │
│  WHERE country = 'US'                     → Very fast        │
│  WHERE country = 'US' AND city = 'NYC'    → Very fast        │
│  WHERE city = 'NYC'                       → SLOW (no index)  │
│  WHERE user_id = 123                      → SLOW (no index)  │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

## PREWHERE vs WHERE

### How PREWHERE Works

```sql
-- Without PREWHERE (reads all columns first)
SELECT * FROM transactions
WHERE status = 'completed' AND amount > 100;

-- With PREWHERE (reads filter columns first, then others)
SELECT * FROM transactions
PREWHERE status = 'completed'  -- Evaluated first
WHERE amount > 100;            -- Evaluated after
```

### When to Use PREWHERE

```
┌─────────────────────────────────────────────────────────────┐
│                    PREWHERE Benefits                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Step 1: Read only PREWHERE columns                         │
│          ┌─────────┐                                        │
│          │ status  │  ← Small column, fast to read         │
│          └─────────┘                                        │
│                                                             │
│  Step 2: Filter rows                                         │
│          Keep only rows where status = 'completed'          │
│          (e.g., 20% of rows pass)                           │
│                                                             │
│  Step 3: Read remaining columns ONLY for filtered rows      │
│          ┌──────────────────────────────────────────┐       │
│          │ amount │ user_id │ merchant_id │ ... │   │       │
│          └──────────────────────────────────────────┘       │
│          (80% less data to read!)                           │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### PREWHERE Best Practices

```sql
-- Use PREWHERE for highly selective filters on small columns
SELECT * FROM transactions
PREWHERE status = 'failed'  -- Only 2% of rows match
WHERE created_at > '2024-01-01';

-- ClickHouse often moves simple filters to PREWHERE automatically
-- (optimize_move_to_prewhere setting)

-- Don't use PREWHERE for complex expressions
-- (can be slower than WHERE for non-selective filters)
```

## TTL Strategies

### Delete Old Data

```sql
-- Delete after 1 year
ALTER TABLE transactions
MODIFY TTL created_at + INTERVAL 1 YEAR DELETE;
```

### Move to Cheaper Storage

```sql
-- Tiered storage (requires storage policy configuration)
-- First, configure storage_policies in config.xml:
--
-- <storage_configuration>
--   <disks>
--     <ssd><path>/mnt/ssd/clickhouse/</path></ssd>
--     <hdd><path>/mnt/hdd/clickhouse/</path></hdd>
--   </disks>
--   <policies>
--     <tiered>
--       <volumes>
--         <hot><disk>ssd</disk></hot>
--         <cold><disk>hdd</disk></cold>
--       </volumes>
--     </tiered>
--   </policies>
-- </storage_configuration>

ALTER TABLE transactions
MODIFY TTL
    created_at + INTERVAL 30 DAY TO DISK 'ssd',
    created_at + INTERVAL 90 DAY TO DISK 'hdd',
    created_at + INTERVAL 365 DAY DELETE;
```

### Aggregate Old Data (Rollup)

```sql
-- Convert detailed → summary
ALTER TABLE metrics
MODIFY TTL
    timestamp + INTERVAL 7 DAY
    GROUP BY metric_name, toStartOfHour(timestamp)
    SET
        timestamp = toStartOfHour(timestamp),
        value = sum(value),
        count = sum(count);
```

## Compression Strategies

### Default Compression

```sql
-- LZ4 is default (fast, good compression)
-- Works well for most use cases
```

### Specialized Codecs

```sql
CREATE TABLE optimized_metrics
(
    -- Timestamps: DoubleDelta for monotonic sequences
    timestamp DateTime CODEC(DoubleDelta, ZSTD),

    -- Sequential integers: Delta encoding
    event_id UInt64 CODEC(Delta, LZ4),

    -- Float time-series: Gorilla encoding
    value Float64 CODEC(Gorilla, LZ4),

    -- Text: Maximum compression
    description String CODEC(ZSTD(9)),

    -- Low cardinality: Dictionary handles most compression
    status LowCardinality(String) CODEC(LZ4)
);
```

### Analyzing Compression

```sql
SELECT
    column,
    type,
    formatReadableSize(data_compressed_bytes) AS compressed,
    formatReadableSize(data_uncompressed_bytes) AS uncompressed,
    round(data_uncompressed_bytes / data_compressed_bytes, 1) AS ratio
FROM system.columns
WHERE table = 'transactions'
ORDER BY data_compressed_bytes DESC;
```

## Useful System Tables

### system.query_log

```sql
-- Find slow queries
SELECT
    query,
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_size,
    result_rows
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_duration_ms > 1000  -- Slower than 1 second
  AND event_date = today()
ORDER BY query_duration_ms DESC
LIMIT 20;
```

### system.parts

```sql
-- Monitor part count (too many = slow queries)
SELECT
    table,
    partition,
    count() AS part_count,
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE active AND database = currentDatabase()
GROUP BY table, partition
HAVING part_count > 50
ORDER BY part_count DESC;
```

### system.merges

```sql
-- Check ongoing merges
SELECT
    table,
    elapsed,
    progress,
    num_parts,
    formatReadableSize(total_size_bytes_compressed) AS size
FROM system.merges
ORDER BY elapsed DESC;
```

### system.columns

```sql
-- Storage analysis by column
SELECT
    table,
    column,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed
FROM system.columns
WHERE database = currentDatabase()
GROUP BY table, column
ORDER BY sum(data_compressed_bytes) DESC
LIMIT 20;
```

## Query Optimization Checklist

```
□ Check EXPLAIN output
  - Are indexes being used?
  - How many parts/granules are read?
  - Is partition pruning working?

□ Review ORDER BY design
  - Most filtered columns first?
  - Matches query patterns?

□ Use PREWHERE for selective filters
  - Small columns
  - High selectivity (filters out most rows)

□ Avoid SELECT *
  - Specify only needed columns
  - Especially important for wide tables

□ Use LIMIT early
  - Especially in exploratory queries

□ Check part count
  - Too many parts = run OPTIMIZE TABLE

□ Monitor compression
  - Consider specialized codecs for time-series

□ Use sampling for approximations
  - SAMPLE 0.1 for 10% of data
  - Much faster for exploratory analysis

□ Consider materialized views
  - Pre-aggregate common query patterns
  - Trade storage for query speed

□ Add projections for alternative access patterns
  - Different sort orders
  - Simple pre-aggregations
```

## Common Anti-Patterns

### Anti-Pattern 1: SELECT * on Wide Tables

```sql
-- BAD: Reads all 50 columns
SELECT * FROM wide_table WHERE user_id = 1;

-- GOOD: Reads only needed columns
SELECT user_id, name, email FROM wide_table WHERE user_id = 1;
```

### Anti-Pattern 2: Functions on Indexed Columns

```sql
-- BAD: Can't use index on transformed column
WHERE toDate(created_at) = '2024-01-15'

-- GOOD: Use range instead
WHERE created_at >= '2024-01-15' AND created_at < '2024-01-16'
```

### Anti-Pattern 3: High FINAL Usage

```sql
-- BAD: Forces merge on entire table
SELECT * FROM huge_dedup_table FINAL;

-- GOOD: Use FINAL with filters
SELECT * FROM huge_dedup_table FINAL WHERE user_id = 123;

-- BETTER: Use GROUP BY pattern when possible
SELECT user_id, argMax(data, version) FROM table GROUP BY user_id;
```

### Anti-Pattern 4: Missing Partition Pruning

```sql
-- Table partitioned by toYYYYMM(created_at)

-- BAD: No partition pruning
WHERE user_id = 123

-- GOOD: Include partition key in filter
WHERE user_id = 123 AND created_at >= '2024-01-01'
```
