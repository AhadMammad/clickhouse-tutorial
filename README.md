# ClickHouse Fundamentals

A comprehensive hands-on project for learning ClickHouse with Python, covering Lessons 11 & 12 of a database engineering curriculum. Features both local development and fully containerized workflows.

## Prerequisites

- **Docker Desktop 4.x+** (or Docker Engine + Compose plugin)
- **Python 3.11+** (only required for Option A / local mode)
- **Make** (pre-installed on macOS/Linux; Windows users can use WSL2)

## Quick Start

```bash
# Copy environment configuration
cp .env.example .env

# Option A: Local development (requires Python)
make demo

# Option B: Fully containerized (no local Python needed)
make docker-demo
```

## Running Modes

This project supports two running modes depending on your needs:

### Option A — Local Python (recommended for development)

```bash
make demo
```

This command:
1. Creates `.env` from `.env.example` if needed
2. Starts ClickHouse via Docker Compose
3. Waits for ClickHouse to become healthy
4. Creates a local Python virtual environment (`.venv`)
5. Installs dependencies
6. Runs all SQL schema files
7. Generates 100k sample transactions
8. Prints analytics reports

**Best for:** Iterating on Python code quickly with fast feedback loops.

### Option B — Fully Containerized (no local Python needed)

```bash
make docker-demo
```

This command:
1. Creates `.env` from `.env.example` if needed
2. Builds the Python app Docker image
3. Starts ClickHouse via Docker Compose
4. Waits for ClickHouse to become healthy
5. Runs schema setup inside the container
6. Generates 100k sample transactions inside the container
7. Prints analytics reports inside the container

**Best for:** Testing the production container or sharing with teammates who don't have Python installed locally.

## Docker Profiles System

The `docker-compose.yml` uses Docker profiles to control which services start:

```bash
# Start only ClickHouse (default)
docker compose up -d

# Start both ClickHouse and the app
docker compose --profile app up -d

# Run the app container interactively
docker compose --profile app run --rm app

# Debug the container environment
make shell
```

The `app` service uses `profiles: [app]`, so it's opt-in and won't start with a plain `docker compose up`.

## Project Structure

```
clickhouse-fundamentals/
├── docker-compose.yml        # ClickHouse + app service definitions
├── Makefile                  # Build automation and common tasks
├── .env.example              # Environment template
├── README.md                 # This file
├── clickhouse-data/          # ClickHouse data (git-ignored)
│
├── docs/                     # Learning documentation
│   ├── 01-architecture.md        # Column-oriented storage, merges
│   ├── 02-merge-tree-engines.md  # MergeTree deep dive
│   ├── 03-advanced-engines.md    # Replacing, Summing, Aggregating
│   ├── 04-materialized-views.md  # MVs as triggers
│   └── 05-query-optimization.md  # EXPLAIN, PREWHERE, TTL
│
├── sql/                      # SQL schema files
│   ├── 01_create_tables.sql      # Core tables, data types
│   ├── 02_partitioning_and_keys.sql
│   ├── 03_replacing_merge_tree.sql
│   ├── 04_aggregating_merge_tree.sql
│   ├── 05_materialized_views.sql
│   ├── 06_projections.sql
│   ├── 07_ttl_and_compression.sql
│   └── 08_analytical_queries.sql
│
└── python/                   # Python application
    ├── Dockerfile            # Multi-stage build
    ├── .dockerignore
    ├── requirements.txt
    ├── config.py             # Environment configuration
    ├── main.py               # CLI entry point
    ├── models/               # Data models
    │   ├── transaction.py
    │   └── payment_metric.py
    ├── db/                   # Database layer
    │   ├── client.py         # ClickHouse client wrapper
    │   └── repository.py     # Data access patterns
    ├── generators/           # Test data generation
    │   └── transaction_generator.py
    └── readers/              # Analytics output
        └── analytics_reader.py
```

## Documentation

| Document | Topics Covered |
|----------|----------------|
| [01-architecture.md](docs/01-architecture.md) | Column vs row storage, parts, granules, merges |
| [02-merge-tree-engines.md](docs/02-merge-tree-engines.md) | PRIMARY KEY vs ORDER BY, partitioning, data types |
| [03-advanced-engines.md](docs/03-advanced-engines.md) | ReplacingMergeTree, AggregatingMergeTree |
| [04-materialized-views.md](docs/04-materialized-views.md) | MVs as triggers, pre-aggregation patterns |
| [05-query-optimization.md](docs/05-query-optimization.md) | EXPLAIN, PREWHERE, TTL, compression |

## Available Commands

Run `make help` to see all available commands:

```
INFRASTRUCTURE
  make up               Start ClickHouse via Docker Compose
  make down             Stop and remove containers
  make restart          Restart containers
  make logs             Tail ClickHouse container logs
  make wait             Wait until ClickHouse is healthy (60s timeout)
  make ps               Show running containers

DATABASE
  make setup            Apply all SQL schemas
  make reset-db         Drop and recreate database (prompts confirmation)
  make sql              Open ClickHouse interactive shell

PYTHON APP — local venv
  make install          Create .venv and install Python dependencies
  make generate         Insert 100k sample transactions (local)
  make report           Print analytics reports (local)
  make demo             Full local end-to-end demo

DOCKER APP — no local Python needed
  make build            Build the Python app Docker image
  make run              Run app container with default command
  make run-setup        Run schema setup inside container
  make run-generate     Run data generator inside container
  make run-report       Run analytics report inside container
  make shell            Open bash shell inside app container
  make docker-demo      Full containerized end-to-end demo

DEVELOPMENT
  make lint             Lint with ruff
  make format           Format with ruff
  make typecheck        Type-check with mypy
  make test             Run pytest
  make check            lint + typecheck

UTILITIES
  make env              Create .env from .env.example
  make clean            Remove caches, .venv, and compiled files
  make docs             Show docs index
  make help             Show this help message
```

## Python CLI Usage

```bash
# Set up database schema
python main.py setup

# Generate sample transactions
python main.py generate --rows 100000

# Print analytics reports
python main.py report --days 30

# Show user profile
python main.py user --id 1001

# Run full demo
python main.py demo
```

## Example SQL Queries

After running `make demo`, you can explore the data:

```sql
-- Connect to ClickHouse
make sql

-- Check row count
SELECT count() FROM transactions;

-- Revenue by category
SELECT
    category,
    count() AS transactions,
    sum(amount) AS revenue,
    avg(amount) AS avg_transaction
FROM transactions
WHERE status = 'completed'
GROUP BY category
ORDER BY revenue DESC;

-- Top merchants
SELECT
    merchant_id,
    sum(amount) AS revenue,
    uniq(user_id) AS customers
FROM transactions
WHERE created_at >= today() - 30
GROUP BY merchant_id
ORDER BY revenue DESC
LIMIT 10;

-- Hourly patterns
SELECT
    toHour(created_at) AS hour,
    count() AS transactions
FROM transactions
GROUP BY hour
ORDER BY hour;
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_HOST` | ClickHouse server hostname | `localhost` |
| `CLICKHOUSE_PORT` | ClickHouse HTTP port | `8123` |
| `CLICKHOUSE_USER` | ClickHouse username | `default` |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | (empty) |
| `CLICKHOUSE_DATABASE` | Target database | `default` |

When running inside Docker, `CLICKHOUSE_HOST` is automatically set to `clickhouse` (the Docker service name) so containers can communicate over Docker's internal network.

## Troubleshooting

### ClickHouse won't start
```bash
make logs  # Check container logs
make down && make up  # Restart the container
```

### Connection refused errors
```bash
make wait  # Ensure ClickHouse is healthy before running commands
```

### Reset everything
```bash
make down
make clean
rm -rf clickhouse-data
make demo  # or make docker-demo
```

### Python import errors
```bash
# Ensure you're using the virtual environment
source .venv/bin/activate
# Or reinstall dependencies
make install
```

## Learning Path

1. **Start with the demo**: Run `make demo` to see everything working
2. **Read the docs**: Start with `docs/01-architecture.md`
3. **Explore SQL files**: Each file has extensive comments
4. **Run queries**: Use `make sql` to experiment
5. **Modify the Python code**: Add new reports or generators
6. **Check the system tables**: `SELECT * FROM system.parts`
