# Liquibase — Project Implementation

## How Everything Is Wired Together

```mermaid
graph LR
    subgraph HOST["Host Machine"]
        MK["make pg-migrate"]
        ENV[".env file\nPG_USER / PG_PASSWORD\nPG_DATABASE"]
        VOL["./liquibase/\n(bind mount)"]
    end

    subgraph DOCKER["Docker (profile: migrate)"]
        LB["liquibase:4.27\ncontainer\ncommand: update"]
        PROPS["liquibase.properties\n(local dev only)"]
    end

    subgraph PG["PostgreSQL Container"]
        DB["appdb database"]
        DCHL["DATABASECHANGELOG\n(tracking table)"]
        SCHEMA["9 application tables"]
    end

    MK -->|"docker compose\n--profile migrate\nrun --rm"| LB
    ENV -->|env vars injected| LB
    VOL -->|mounted at\n/liquibase/changelog| LB
    LB -->|"JDBC\npostgres:5432"| DB
    DB --- DCHL
    DB --- SCHEMA
```

### The chain, step by step

1. Developer runs `make pg-migrate`
2. Docker Compose starts a one-shot `liquibase:4.27` container (profile `migrate`)
3. The `./liquibase/` directory is bind-mounted into the container at `/liquibase/changelog`
4. Credentials from `.env` are injected as environment variables (`LIQUIBASE_COMMAND_URL`, `LIQUIBASE_COMMAND_USERNAME`, etc.)
5. The container runs `liquibase update`, connecting to `postgres:5432` (the service name inside Docker's network)
6. Liquibase applies any unapplied changesets and exits — the container is removed (`--rm`)

---

## Directory Layout

```
liquibase/
├── liquibase.properties              ← local dev config (JDBC URL, credentials)
└── changelogs/
    ├── db.changelog-master.yaml      ← master file, includes all others in order
    ├── 001_create_user_tiers.yaml
    ├── 002_create_users.yaml
    ├── 003_create_devices.yaml
    ├── 004_create_app_versions.yaml
    ├── 005_create_screens.yaml
    ├── 006_create_event_types.yaml
    ├── 007_create_sessions.yaml
    ├── 008_create_events.yaml
    └── 009_create_user_devices.yaml
```

The numbering enforces dependency order — `users` must exist before `sessions` (which has a FK to `users`).

---

## Anatomy of a Real Changeset

From `001_create_user_tiers.yaml`:

```yaml
databaseChangeLog:
  - changeSet:
      id: 001-enable-pgcrypto          # unique within this file
      author: bootcamp
      changes:
        - sql:
            sql: CREATE EXTENSION IF NOT EXISTS pgcrypto;   # raw SQL fallback

  - changeSet:
      id: 001-create-user-tiers        # descriptive, not just a number
      author: bootcamp
      changes:
        - createTable:
            tableName: user_tiers
            columns:
              - column:
                  name: tier_id
                  type: SERIAL
                  constraints:
                    primaryKey: true
                    primaryKeyName: pk_user_tiers
              - column:
                  name: tier_name
                  type: VARCHAR(20)
                  constraints:
                    nullable: false
                    unique: true
                    uniqueConstraintName: uq_user_tiers_name

  - changeSet:
      id: 001-seed-user-tiers          # seed data is its own changeset
      author: bootcamp
      changes:
        - insert:
            tableName: user_tiers
            columns:
              - column: { name: tier_name, value: free }
              - column: { name: max_monthly_events, valueNumeric: 1000 }
```

Three changesets per file here: enable extension → create table → seed reference data. Each is atomic and independently tracked.

---

## The 9-Table Schema

All 9 tables are created by the changelogs. This is the PostgreSQL 3NF source schema for the mobile app interaction data pipeline.

```mermaid
erDiagram
    user_tiers {
        int tier_id PK
        varchar tier_name
        int max_monthly_events
        text description
    }
    users {
        bigint user_id PK
        varchar external_user_id
        varchar username
        varchar email
        int tier_id FK
        timestamp registered_at
    }
    devices {
        bigint device_id PK
        varchar device_fingerprint
        varchar device_type
        varchar os_name
        varchar os_version
        varchar device_model
        varchar screen_resolution
        timestamp created_at
    }
    app_versions {
        int version_id PK
        varchar version_code
        varchar platform
        date release_date
        boolean is_force_update
    }
    screens {
        int screen_id PK
        varchar screen_name
        varchar screen_category
        text description
    }
    event_types {
        int event_type_id PK
        varchar event_name
        varchar event_category
        text description
    }
    sessions {
        uuid session_id PK
        bigint user_id FK
        bigint device_id FK
        int version_id FK
        timestamp session_start
        timestamp session_end
        inet ip_address
        char country_code
    }
    events {
        bigint event_id PK
        uuid session_id FK
        int screen_id FK
        int event_type_id FK
        timestamp event_timestamp
        int sequence_number
        int duration_ms
        jsonb properties
    }
    user_devices {
        bigint user_id FK
        bigint device_id FK
        timestamp first_seen
        timestamp last_seen
    }

    user_tiers ||--o{ users : "tier_id"
    users ||--o{ sessions : "user_id"
    users ||--o{ user_devices : "user_id"
    devices ||--o{ sessions : "device_id"
    devices ||--o{ user_devices : "device_id"
    app_versions ||--o{ sessions : "version_id"
    sessions ||--o{ events : "session_id"
    event_types ||--o{ events : "event_type_id"
    screens |o--o{ events : "screen_id"
```

### Design decisions embedded in the schema

| Decision | Why |
|----------|-----|
| `country_code` on `sessions`, not `users` | A user's location is session-specific — they may travel |
| `duration_seconds` not stored on `sessions` | Derived from `session_end - session_start`; storing it would duplicate data |
| `screen_id` is nullable on `events` | Some events are not tied to a screen (e.g., background sync events) |
| `properties JSONB` on `events` | Flexible event payload without schema changes for every new event attribute |
| `device_fingerprint` unique on `devices` | One physical device = one row, regardless of how many users share it |
| UUID for `session_id` | Sessions can be generated client-side without a DB round-trip |

---

## Running Migrations

```bash
# 1. Start PostgreSQL (if not already running)
make pg-up

# 2. Apply all pending migrations
make pg-migrate
```

`make pg-migrate` expands to:
```bash
docker compose --profile migrate run --rm --no-deps liquibase
```

- `--profile migrate` — opt-in profile; the Liquibase service doesn't start unless requested
- `run --rm` — one-shot container, removed after exit
- `--no-deps` — don't try to start postgres again (it's already healthy)

On first run: all 9 changelogs execute, ~15 changesets applied.
On subsequent runs: all changesets skipped, exits in under a second.

---

## Where This Fits in the Full Pipeline

Liquibase runs **once, at setup time**. The rest of the pipeline builds on top of the schema it creates.

```mermaid
flowchart LR
    LB["make pg-migrate\nLiquibase creates\nPostgreSQL schema"]
    GEN["make pg-generate\nSynthetic data\ninserted into PG"]
    EXP["make export-parquet\nPG → HDFS\n(flat Parquet)"]
    STAR["make star-setup\n+ star-load\nHDFS → ClickHouse\nstar schema"]

    LB --> GEN --> EXP --> STAR
```
