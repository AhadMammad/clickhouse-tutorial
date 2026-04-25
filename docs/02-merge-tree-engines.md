# MergeTree Engine Deep Dive

## Overview

MergeTree is the foundational table engine in ClickHouse. All other *MergeTree family engines (ReplacingMergeTree, SummingMergeTree, etc.) build on top of it.

## Basic MergeTree Syntax

```sql
CREATE TABLE events
(
    event_id UUID,
    user_id UInt64,
    event_type String,
    created_at DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (user_id, created_at)
PRIMARY KEY (user_id)
SETTINGS index_granularity = 8192;
```

## PRIMARY KEY vs ORDER BY

This is the **most common misconception** in ClickHouse!

### In Traditional Databases (PostgreSQL, MySQL)

```
PRIMARY KEY = Uniqueness Constraint + Clustered Index
- Enforces unique values
- Determines physical storage order
- Creates an index for fast lookups
```

### In ClickHouse

```
ORDER BY   = Physical sort order on disk + Sparse index columns
PRIMARY KEY = Prefix of ORDER BY used for sparse index (optional)

- NO uniqueness enforcement!
- PRIMARY KEY must be a prefix of ORDER BY
- If PRIMARY KEY is omitted, it equals ORDER BY
```

### Visual Explanation

```
ORDER BY (user_id, created_at, event_id)
         ↓
         ├── Determines how rows are physically sorted on disk
         ├── All three columns participate in sorting
         └── More columns = better uniqueness in sort

PRIMARY KEY (user_id, created_at)
         ↓
         ├── Determines what's stored in the sparse index
         ├── Smaller key = smaller index file
         └── Must be a prefix of ORDER BY
```

### Why Would You Want Different PRIMARY KEY and ORDER BY?

```sql
-- Example: Large table with many columns in ORDER BY for uniqueness
CREATE TABLE clicks
(
    click_id UUID,
    user_id UInt64,
    page_url String,
    clicked_at DateTime64(3)
)
ENGINE = MergeTree()
ORDER BY (user_id, clicked_at, click_id)  -- Full sort for uniqueness
PRIMARY KEY (user_id, clicked_at);         -- Smaller index (no UUID)

-- The sparse index only stores (user_id, clicked_at)
-- But data is sorted by all three columns
-- Index is smaller, faster to load, better cache utilization
```

## Partitioning

### What is a Partition?

Partitions are the highest level of data organization:

```
Table: events
│
├── Partition: 202401 (January 2024)
│   ├── Part: 202401_1_1_0
│   └── Part: 202401_2_3_1
│
├── Partition: 202402 (February 2024)
│   ├── Part: 202402_1_1_0
│   └── Part: 202402_2_2_0
│
└── Partition: 202403 (March 2024)
    └── Part: 202403_1_1_0
```

### Partition Pruning

When you filter by partition key, entire partitions are skipped:

```sql
-- Query with partition pruning
SELECT count() FROM events
WHERE created_at >= '2024-02-01' AND created_at < '2024-03-01';

-- ClickHouse only reads Partition 202402
-- Partitions 202401, 202403, etc. are completely skipped!
```

### Common Partitioning Strategies

| Strategy | Expression | Partitions/Year | Use Case |
|----------|-----------|-----------------|----------|
| Monthly | `toYYYYMM(date)` | 12 | Multi-year retention |
| Weekly | `toMonday(date)` | 52 | Medium retention |
| Daily | `toYYYYMMDD(date)` | 365 | Short retention, TTL |
| None | - | 1 | Small tables |

### Partitioning Anti-Patterns

```sql
-- BAD: Too many partitions (hourly for years)
PARTITION BY toStartOfHour(created_at)
-- 8,760 partitions/year = performance issues!

-- BAD: High cardinality partition key
PARTITION BY user_id
-- Millions of partitions = disaster!

-- BAD: Partition key not in queries
PARTITION BY toYYYYMM(created_at)
-- But queries filter by: WHERE status = 'active'
-- No partition pruning benefit!

-- GOOD: Partition matches query patterns
PARTITION BY toYYYYMM(created_at)
-- Queries use: WHERE created_at BETWEEN '2024-01-01' AND '2024-01-31'
```

## Sparse Index and Granules

### How the Sparse Index Works

```
                         Sparse Index
                              │
    ┌─────────────────────────┼─────────────────────────┐
    │                         │                         │
    ▼                         ▼                         ▼
┌─────────┐            ┌─────────┐            ┌─────────┐
│Entry 0  │            │Entry 1  │            │Entry 2  │
│user=1   │            │user=500 │            │user=1200│
│date=Jan1│            │date=Jan5│            │date=Jan10│
└────┬────┘            └────┬────┘            └────┬────┘
     │                      │                      │
     ▼                      ▼                      ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Granule 0     │  │   Granule 1     │  │   Granule 2     │
│   8192 rows     │  │   8192 rows     │  │   8192 rows     │
│   (rows 0-8191) │  │  (rows 8192-    │  │  (rows 16384-   │
│                 │  │      16383)     │  │      24575)     │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

### Query Execution with Sparse Index

```sql
-- Query: Find transactions for user 750
SELECT * FROM transactions WHERE user_id = 750;

-- Step 1: Binary search in sparse index
-- Index entries: [user=1], [user=500], [user=1200], ...
-- user=750 is between entry 1 (user=500) and entry 2 (user=1200)
-- → Read granules 1 and 2 (they might contain user=750)

-- Step 2: Scan granules 1 and 2
-- Filter rows where user_id = 750

-- Result: Only read 2 granules (~16K rows) instead of entire table!
```

## Data Types Guide

### Integer Types

| Type | Range | Size | Use Case |
|------|-------|------|----------|
| UInt8 | 0 to 255 | 1 byte | Boolean, small flags |
| UInt16 | 0 to 65,535 | 2 bytes | Port numbers |
| UInt32 | 0 to 4.3B | 4 bytes | IDs, counters |
| UInt64 | 0 to 18.4Q | 8 bytes | Large IDs, timestamps |
| Int8/16/32/64 | Signed | Same | When negatives needed |

### Decimal (for Money!)

```sql
-- NEVER use Float for money!
amount Float64      -- BAD: 0.1 + 0.2 = 0.30000000000000004

-- Use Decimal instead
amount Decimal(18, 2)  -- GOOD: Exact decimal arithmetic
-- Precision 18, Scale 2 = up to 16 digits before decimal, 2 after
-- Range: -9,999,999,999,999,999.99 to 9,999,999,999,999,999.99
```

### String Types

```sql
-- Variable length string (most common)
name String

-- Fixed length (exactly N bytes, padded with nulls)
country_code FixedString(2)  -- 'US', 'UK', etc.

-- Dictionary-encoded (HUGE savings for low cardinality!)
currency LowCardinality(String)  -- Only ~200 currencies
status LowCardinality(String)    -- Only ~10 statuses
```

### LowCardinality Deep Dive

```
Without LowCardinality:
┌─────────────────────────────────────────┐
│ Row 1: "United States"                  │
│ Row 2: "United Kingdom"                 │
│ Row 3: "United States"                  │
│ Row 4: "United States"                  │
│ Row 5: "Germany"                        │
│ ... (repeated strings waste space)      │
└─────────────────────────────────────────┘

With LowCardinality:
┌──────────────────────┐    ┌─────────────────────┐
│ Dictionary:          │    │ Data (indices only) │
│ 0: "United States"   │    │ Row 1: 0            │
│ 1: "United Kingdom"  │    │ Row 2: 1            │
│ 2: "Germany"         │    │ Row 3: 0            │
└──────────────────────┘    │ Row 4: 0            │
                            │ Row 5: 2            │
                            └─────────────────────┘
                            (Much smaller!)
```

### Date and Time Types

```sql
-- Day precision (2 bytes)
date Date              -- Range: 1970-01-01 to 2299-12-31 (ClickHouse 22.8+)
                       -- Note: Older versions support up to 2149-06-06

-- Extended date range (4 bytes)
date Date32            -- Range: 1900-01-01 to 2299-12-31

-- Second precision (4 bytes)
timestamp DateTime     -- Range: 1970-01-01 to 2106-02-07

-- Sub-second precision
timestamp_ms DateTime64(3)   -- Milliseconds
timestamp_us DateTime64(6)   -- Microseconds
timestamp_ns DateTime64(9)   -- Nanoseconds
```

### Complex Types

```sql
-- Array
tags Array(String)
-- Insert: ['tag1', 'tag2', 'tag3']
-- Query: WHERE has(tags, 'important')

-- Tuple (fixed structure)
location Tuple(lat Float64, lon Float64)
-- Insert: (37.7749, -122.4194)
-- Query: SELECT location.lat

-- Map (key-value pairs)
metadata Map(String, String)
-- Insert: {'source': 'web', 'campaign': 'summer'}
-- Query: SELECT metadata['source']

-- Nullable (use sparingly - adds overhead!)
optional_field Nullable(String)
-- Can be NULL or String
-- Adds 1 byte per row for null bitmap
```

## Inspecting Your Tables

### View Parts

```sql
SELECT
    partition,
    name,
    rows,
    bytes_on_disk,
    data_compressed_bytes,
    data_uncompressed_bytes,
    marks_bytes,
    modification_time
FROM system.parts
WHERE table = 'transactions' AND active = 1
ORDER BY modification_time DESC;
```

### View Columns and Compression

```sql
SELECT
    column,
    type,
    formatReadableSize(data_compressed_bytes) AS compressed,
    formatReadableSize(data_uncompressed_bytes) AS uncompressed,
    round(data_uncompressed_bytes / data_compressed_bytes, 2) AS ratio
FROM system.columns
WHERE table = 'transactions'
ORDER BY data_compressed_bytes DESC;
```

### View Primary Key

```sql
SELECT
    name,
    primary_key,
    sorting_key,
    partition_key
FROM system.tables
WHERE name = 'transactions';
```

## Best Practices Summary

1. **ORDER BY column order matters**
   - Most filtered columns first
   - Time column usually second or third
   - Add columns for uniqueness if needed

2. **Use LowCardinality liberally**
   - Any column with <10,000 unique values
   - Especially: country, status, category, type

3. **Choose appropriate partition granularity**
   - Monthly for most use cases
   - Daily only with TTL/short retention
   - Aim for 100-1000 total partitions

4. **Never use Float for money**
   - Always Decimal(18, 2) or similar

5. **Avoid Nullable unless necessary**
   - Use empty strings or 0 as defaults instead
   - Each Nullable adds storage overhead
