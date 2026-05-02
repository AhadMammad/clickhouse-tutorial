# HDFS Silver Layer Schema

The silver layer contains a **denormalized, flat representation** of each day's events — one row per event with all related dimension attributes merged in. It is produced by `raw-to-silver` via `etl/raw_to_silver.py` and consumed by `star-load`.

The schema is identical to what the legacy `export-parquet` command produced, which means `parquet_to_star.py` works without any changes to its deserialization logic.

## Path Structure

```
{HDFS_BASE_PATH}/silver/
└── dt=YYYY-MM-DD/
    └── part-00000.parquet
```

## How It Is Produced

`raw_to_silver.py` reads all 8 raw tables for the target date and reproduces the 7-table SQL JOIN in pandas:

```
events
  INNER JOIN sessions       ON session_id
  INNER JOIN users          ON user_id
  INNER JOIN user_tiers     ON tier_id          ← resolves tier_name, drops tier_id
  INNER JOIN devices        ON device_id
  LEFT  JOIN screens        ON screen_id        ← nullable: events may have no screen
  INNER JOIN event_types    ON event_type_id
  INNER JOIN app_versions   ON version_id
```

**Dropped columns** (present in raw, absent from silver):
- `ip_address` — not needed for analytics
- `tier_id` — replaced by `tier_name` after joining `user_tiers`

**Computed column** (not stored in PostgreSQL):
- `session_duration_seconds` = `(session_end − session_start).total_seconds()` — was computed in SQL via `EXTRACT(EPOCH FROM ...)` in the legacy pipeline

## Full Schema (29 columns)

| # | Column | PyArrow type | Source table | Note |
|---|---|---|---|---|
| 1 | event_id | int64 | events | |
| 2 | event_timestamp | timestamp(us) | events | |
| 3 | sequence_number | int32 | events | |
| 4 | duration_ms | int32 | events | |
| 5 | properties | string | events | JSONB serialised as text |
| 6 | session_id | string | sessions | UUID as text |
| 7 | session_start | timestamp(us) | sessions | |
| 8 | session_end | timestamp(us) | sessions | |
| 9 | session_duration_seconds | int32 | computed | session_end − session_start |
| 10 | country_code | string | sessions | |
| 11 | user_id | int64 | sessions/users | |
| 12 | external_user_id | string | users | |
| 13 | username | string | users | |
| 14 | tier_name | string | user_tiers | replaces tier_id |
| 15 | device_id | int64 | sessions/devices | |
| 16 | device_fingerprint | string | devices | |
| 17 | device_type | string | devices | |
| 18 | os_name | string | devices | |
| 19 | os_version | string | devices | |
| 20 | device_model | string | devices | |
| 21 | screen_id | int32 | events | 0 when NULL (no screen) |
| 22 | screen_name | string | screens | "" when NULL |
| 23 | screen_category | string | screens | "" when NULL |
| 24 | event_type_id | int32 | events/event_types | |
| 25 | event_name | string | event_types | |
| 26 | event_category | string | event_types | |
| 27 | version_id | int32 | sessions/app_versions | |
| 28 | version_code | string | app_versions | |
| 29 | platform | string | app_versions | |

## Compression

Files are written with Snappy compression. No local disk I/O — serialisation happens in an in-memory `io.BytesIO` buffer before upload to HDFS via WebHDFS.
