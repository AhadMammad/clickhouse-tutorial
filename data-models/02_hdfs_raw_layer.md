# HDFS Raw Layer Schemas

The raw layer is a **faithful mirror of the PostgreSQL source tables** — no JOINs, no computed columns, no transformations. Its purpose is to preserve the original data so that any downstream transformation can be replayed without re-querying PostgreSQL.

Written by `export-raw` via `etl/pg_to_raw.py`.

## Path Structure

```
{HDFS_BASE_PATH}/raw/
├── events/        dt=YYYY-MM-DD/part-00000.parquet    ← fact, daily partition
├── sessions/      dt=YYYY-MM-DD/part-00000.parquet    ← fact, daily partition
├── users/         snapshot=YYYY-MM-DD/part-00000.parquet
├── devices/       snapshot=YYYY-MM-DD/part-00000.parquet
├── user_tiers/    snapshot=YYYY-MM-DD/part-00000.parquet
├── screens/       snapshot=YYYY-MM-DD/part-00000.parquet
├── event_types/   snapshot=YYYY-MM-DD/part-00000.parquet
└── app_versions/  snapshot=YYYY-MM-DD/part-00000.parquet
```

- **Fact tables** (`events`, `sessions`) use `dt=` partitioning — only rows for that day are written.
- **Dimension tables** use `snapshot=` — the entire table is written on each export date, ensuring consistent dimension state for that day's facts.

## Type Mapping (PG → PyArrow)

| PostgreSQL type | PyArrow type | Note |
|---|---|---|
| BIGSERIAL / BIGINT | `int64` | |
| SERIAL / INTEGER | `int32` | |
| VARCHAR / TEXT / CHAR | `string` | |
| UUID | `string` | cast via `::text` in SQL |
| INET | `string` | cast via `::text` in SQL |
| JSONB | `string` | cast via `::text` in SQL |
| TIMESTAMP | `timestamp("us")` | microsecond precision, no tz |
| DATE | `date32` | |
| BOOLEAN | `bool_` | |

## Table Schemas

### events (fact — `dt=`)
| Column | PyArrow type | Nullable | Source PG column |
|---|---|---|---|
| event_id | int64 | No | events.event_id |
| session_id | string | No | events.session_id (UUID→text) |
| screen_id | int32 | Yes | events.screen_id |
| event_type_id | int32 | No | events.event_type_id |
| event_timestamp | timestamp(us) | No | events.event_timestamp |
| sequence_number | int32 | Yes | events.sequence_number |
| duration_ms | int32 | Yes | events.duration_ms |
| properties | string | No | events.properties (JSONB→text) |

### sessions (fact — `dt=`)
| Column | PyArrow type | Nullable | Source PG column |
|---|---|---|---|
| session_id | string | No | sessions.session_id (UUID→text) |
| user_id | int64 | No | sessions.user_id |
| device_id | int64 | No | sessions.device_id |
| version_id | int32 | No | sessions.version_id |
| session_start | timestamp(us) | No | sessions.session_start |
| session_end | timestamp(us) | Yes | sessions.session_end |
| ip_address | string | Yes | sessions.ip_address (INET→text) |
| country_code | string | Yes | sessions.country_code |

### users (dimension — `snapshot=`)
| Column | PyArrow type | Nullable | Source PG column |
|---|---|---|---|
| user_id | int64 | No | users.user_id |
| external_user_id | string | No | users.external_user_id |
| username | string | No | users.username |
| email | string | No | users.email |
| tier_id | int32 | No | users.tier_id |
| registered_at | timestamp(us) | Yes | users.registered_at |

### devices (dimension — `snapshot=`)
| Column | PyArrow type | Nullable | Source PG column |
|---|---|---|---|
| device_id | int64 | No | devices.device_id |
| device_fingerprint | string | No | devices.device_fingerprint |
| device_type | string | No | devices.device_type |
| os_name | string | No | devices.os_name |
| os_version | string | No | devices.os_version |
| device_model | string | Yes | devices.device_model |
| screen_resolution | string | Yes | devices.screen_resolution |
| created_at | timestamp(us) | Yes | devices.created_at |

### user_tiers (dimension — `snapshot=`)
| Column | PyArrow type | Nullable | Source PG column |
|---|---|---|---|
| tier_id | int32 | No | user_tiers.tier_id |
| tier_name | string | No | user_tiers.tier_name |
| max_monthly_events | int32 | Yes | user_tiers.max_monthly_events |
| description | string | Yes | user_tiers.description |

### screens (dimension — `snapshot=`)
| Column | PyArrow type | Nullable | Source PG column |
|---|---|---|---|
| screen_id | int32 | No | screens.screen_id |
| screen_name | string | No | screens.screen_name |
| screen_category | string | Yes | screens.screen_category |
| description | string | Yes | screens.description |

### event_types (dimension — `snapshot=`)
| Column | PyArrow type | Nullable | Source PG column |
|---|---|---|---|
| event_type_id | int32 | No | event_types.event_type_id |
| event_name | string | No | event_types.event_name |
| event_category | string | Yes | event_types.event_category |
| description | string | Yes | event_types.description |

### app_versions (dimension — `snapshot=`)
| Column | PyArrow type | Nullable | Source PG column |
|---|---|---|---|
| version_id | int32 | No | app_versions.version_id |
| version_code | string | No | app_versions.version_code |
| platform | string | No | app_versions.platform |
| release_date | date32 | No | app_versions.release_date |
| is_force_update | bool | No | app_versions.is_force_update |
