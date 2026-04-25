# ClickHouse Architecture

## Overview

ClickHouse is a column-oriented OLAP (Online Analytical Processing) database designed for real-time analytics on large datasets. It was developed at Yandex and open-sourced in 2016.

## Column-Oriented vs Row-Oriented Storage

### Row-Oriented Storage (PostgreSQL, MySQL)

Data is stored row by row, optimized for transactional workloads (OLTP).

```
┌─────────────────────────────────────────────────────────────────┐
│ Block 1                                                          │
├─────────────────────────────────────────────────────────────────┤
│ Row 1: | user_id=1 | name="Alice" | amount=100 | date="2024-01" │
│ Row 2: | user_id=2 | name="Bob"   | amount=200 | date="2024-01" │
│ Row 3: | user_id=3 | name="Carol" | amount=150 | date="2024-02" │
└─────────────────────────────────────────────────────────────────┘

Query: SELECT * FROM users WHERE user_id = 1
Result: Read Row 1 (very fast - single row fetch)

Query: SELECT SUM(amount) FROM users
Result: Must read ALL rows and extract amount from each (slow)
```

**Best for:**
- Single row lookups (WHERE id = X)
- INSERT/UPDATE/DELETE operations
- Transactional workloads
- Small result sets

### Column-Oriented Storage (ClickHouse)

Data is stored column by column, optimized for analytical workloads (OLAP).

```
┌─────────────────────────────────────────────────────────────────┐
│ user_id column:  | 1 | 2 | 3 | 4 | 5 | 6 | ...                 │
├─────────────────────────────────────────────────────────────────┤
│ name column:     | "Alice" | "Bob" | "Carol" | ...              │
├─────────────────────────────────────────────────────────────────┤
│ amount column:   | 100 | 200 | 150 | 300 | 250 | ...           │
├─────────────────────────────────────────────────────────────────┤
│ date column:     | "2024-01" | "2024-01" | "2024-02" | ...      │
└─────────────────────────────────────────────────────────────────┘

Query: SELECT SUM(amount) FROM users
Result: Read ONLY the amount column (extremely fast!)

Query: SELECT * FROM users WHERE user_id = 1
Result: Must read from all columns and reconstruct row (slower)
```

**Best for:**
- Aggregations (SUM, AVG, COUNT, etc.)
- Scanning large datasets
- Reading subset of columns
- Analytics and reporting

## How ClickHouse Stores Data on Disk

### The MergeTree Data Structure

```
Database: analytics
└── Table: transactions
    ├── Partition: 202401 (January 2024)
    │   ├── Part: 202401_1_1_0
    │   │   ├── primary.idx          (sparse primary index)
    │   │   ├── user_id.bin          (column data)
    │   │   ├── user_id.mrk2         (marks/offsets)
    │   │   ├── amount.bin
    │   │   ├── amount.mrk2
    │   │   ├── created_at.bin
    │   │   ├── created_at.mrk2
    │   │   └── ...
    │   ├── Part: 202401_2_2_0
    │   └── Part: 202401_1_2_1       (merged from 1_1_0 and 2_2_0)
    │
    ├── Partition: 202402 (February 2024)
    │   └── ...
    └── ...
```

### Parts

- **Part** = A directory containing column files for a batch of inserted data
- Each INSERT creates a new part
- Parts are immutable (never modified)
- Merges combine small parts into larger ones

### Granules

Data within a part is divided into granules (default 8192 rows):

```
Part: 202401_1_1_0
┌────────────────────────────────────────────────────────────┐
│ Granule 0 (rows 0-8191)                                    │
│ Primary Index Entry: (user_id=1, created_at=2024-01-01)    │
├────────────────────────────────────────────────────────────┤
│ Granule 1 (rows 8192-16383)                                │
│ Primary Index Entry: (user_id=500, created_at=2024-01-05)  │
├────────────────────────────────────────────────────────────┤
│ Granule 2 (rows 16384-24575)                               │
│ Primary Index Entry: (user_id=1200, created_at=2024-01-10) │
└────────────────────────────────────────────────────────────┘
```

The sparse index stores only the first row of each granule, making it very small and fast to search.

## The Merge Process

ClickHouse continuously merges parts in the background:

```
Time T0: After inserts
┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Part 1      │ │ Part 2      │ │ Part 3      │ │ Part 4      │
│ 1000 rows   │ │ 500 rows    │ │ 2000 rows   │ │ 100 rows    │
└─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘

Time T1: After merge
┌─────────────────────────────┐ ┌─────────────────────────────┐
│ Part 1_2 (merged)           │ │ Part 3_4 (merged)           │
│ 1500 rows                   │ │ 2100 rows                   │
└─────────────────────────────┘ └─────────────────────────────┘

Time T2: After another merge
┌─────────────────────────────────────────────────────────────┐
│ Part 1_4 (final merged)                                      │
│ 3600 rows                                                    │
└─────────────────────────────────────────────────────────────┘
```

**Why merging is important:**
1. Reduces number of parts (faster queries)
2. Applies special engines (ReplacingMergeTree deduplication)
3. Reclaims space from deleted rows
4. Improves compression ratios

## Shared-Nothing Architecture

ClickHouse uses a shared-nothing architecture for horizontal scaling:

```
┌─────────────────────────────────────────────────────────────────┐
│                         ClickHouse Cluster                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Shard 1    │    │   Shard 2    │    │   Shard 3    │       │
│  │              │    │              │    │              │       │
│  │ ┌──────────┐ │    │ ┌──────────┐ │    │ ┌──────────┐ │       │
│  │ │ Replica A│ │    │ │ Replica A│ │    │ │ Replica A│ │       │
│  │ │ (node 1) │ │    │ │ (node 3) │ │    │ │ (node 5) │ │       │
│  │ └──────────┘ │    │ └──────────┘ │    │ └──────────┘ │       │
│  │ ┌──────────┐ │    │ ┌──────────┐ │    │ ┌──────────┐ │       │
│  │ │ Replica B│ │    │ │ Replica B│ │    │ │ Replica B│ │       │
│  │ │ (node 2) │ │    │ │ (node 4) │ │    │ │ (node 6) │ │       │
│  │ └──────────┘ │    │ └──────────┘ │    │ └──────────┘ │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                                                                  │
│         Data is partitioned across shards                        │
│         Each shard has replicas for HA                           │
└─────────────────────────────────────────────────────────────────┘
```

**Key concepts:**
- **Shard**: A subset of the data (horizontal partitioning)
- **Replica**: A copy of a shard (high availability)
- **Distributed table**: Virtual table that queries all shards

## When to Use ClickHouse

### ClickHouse is ideal for:

| Use Case | Why |
|----------|-----|
| Real-time analytics dashboards | Sub-second queries on billions of rows |
| Log analysis | Efficient time-series storage and querying |
| Business intelligence | Fast aggregations across large datasets |
| Ad-tech / clickstream | High ingestion rates, quick analysis |
| IoT data processing | Handles high-cardinality time-series |
| Financial analytics | Precise decimal support, fast calculations |

### ClickHouse is NOT ideal for:

| Use Case | Why Not | Better Alternative |
|----------|---------|-------------------|
| OLTP (transactions) | No row-level updates, eventual consistency | PostgreSQL, MySQL |
| Key-value lookups | Optimized for scans, not point queries | Redis, DynamoDB |
| Full-text search | Limited text search capabilities | Elasticsearch |
| Graph relationships | No native graph support | Neo4j, Neptune |
| Small datasets | Overhead not worth it for <1M rows | PostgreSQL |
| Frequent updates | Insert-only model, no efficient updates | PostgreSQL |

## Comparison with Other Databases

```
                    ClickHouse vs Other Databases
            (Analytical/Aggregation Queries on 1B+ rows)

     Query Latency (lower is better)
     ├────────────────────────────────────────────┤
     │                                            │
     │  ClickHouse  ████                          │  ~100ms
     │  Druid       ████████                      │  ~500ms
     │  Presto      ████████████████              │  ~2s
     │  Spark SQL   ████████████████████████      │  ~5s
     │  PostgreSQL  ████████████████████████████  │  ~10s+ (full scan)
     │                                            │
     └────────────────────────────────────────────┘

     Ingestion Rate (higher is better)
     ├────────────────────────────────────────────┤
     │                                            │
     │  ClickHouse  ████████████████████████████  │  1M+ rows/sec
     │  TimescaleDB ████████████████              │  ~200K rows/sec
     │  PostgreSQL  ████████                      │  ~50K rows/sec
     │                                            │
     └────────────────────────────────────────────┘

Note: These comparisons are for analytical workloads (aggregations, scans).
For indexed point lookups (WHERE id = X), PostgreSQL can be very fast.
Actual performance varies significantly based on hardware, schema design,
and query patterns.
```

## Summary

ClickHouse excels at:
1. **Speed**: Processes billions of rows in milliseconds
2. **Compression**: 10-20x compression ratios
3. **Scalability**: Linear scale-out with sharding
4. **Real-time**: Data queryable immediately after insert
5. **SQL**: Full SQL support with extensions

Understanding the architecture helps you:
- Design efficient schemas (ORDER BY matters!)
- Choose appropriate data types
- Optimize query patterns
- Plan for scaling
