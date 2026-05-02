"""ETL: Export each PostgreSQL table as its own Parquet file to HDFS (raw layer).

No JOINs or transformations are applied — this is a faithful mirror of the source
tables, partitioned so downstream jobs can replay transformations without re-hitting
PostgreSQL.

Data flows:
    PostgreSQL (one SELECT per table) → pd.DataFrame → pa.Table → io.BytesIO → HDFS

HDFS path structure:
    Fact tables   : {base_path}/raw/events/dt=YYYY-MM-DD/part-00000.parquet
                    {base_path}/raw/sessions/dt=YYYY-MM-DD/part-00000.parquet
    Dimension tables: {base_path}/raw/{table}/snapshot=YYYY-MM-DD/part-00000.parquet
"""

from __future__ import annotations

import io
import logging
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from clickhouse_fundamentals.db.pg_repository import AppInteractionRepository
from clickhouse_fundamentals.hdfs.client import HdfsClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-table Parquet schemas
# ---------------------------------------------------------------------------

_SCHEMA_EVENTS = pa.schema([
    pa.field("event_id",        pa.int64()),
    pa.field("session_id",      pa.string()),
    pa.field("screen_id",       pa.int32()),
    pa.field("event_type_id",   pa.int32()),
    pa.field("event_timestamp", pa.timestamp("us")),
    pa.field("sequence_number", pa.int32()),
    pa.field("duration_ms",     pa.int32()),
    pa.field("properties",      pa.string()),
])

_SCHEMA_SESSIONS = pa.schema([
    pa.field("session_id",    pa.string()),
    pa.field("user_id",       pa.int64()),
    pa.field("device_id",     pa.int64()),
    pa.field("version_id",    pa.int32()),
    pa.field("session_start", pa.timestamp("us")),
    pa.field("session_end",   pa.timestamp("us")),
    pa.field("ip_address",    pa.string()),
    pa.field("country_code",  pa.string()),
])

_SCHEMA_USERS = pa.schema([
    pa.field("user_id",          pa.int64()),
    pa.field("external_user_id", pa.string()),
    pa.field("username",         pa.string()),
    pa.field("email",            pa.string()),
    pa.field("tier_id",          pa.int32()),
    pa.field("registered_at",    pa.timestamp("us")),
])

_SCHEMA_DEVICES = pa.schema([
    pa.field("device_id",          pa.int64()),
    pa.field("device_fingerprint", pa.string()),
    pa.field("device_type",        pa.string()),
    pa.field("os_name",            pa.string()),
    pa.field("os_version",         pa.string()),
    pa.field("device_model",       pa.string()),
    pa.field("screen_resolution",  pa.string()),
    pa.field("created_at",         pa.timestamp("us")),
])

_SCHEMA_USER_TIERS = pa.schema([
    pa.field("tier_id",            pa.int32()),
    pa.field("tier_name",          pa.string()),
    pa.field("max_monthly_events", pa.int32()),
    pa.field("description",        pa.string()),
])

_SCHEMA_SCREENS = pa.schema([
    pa.field("screen_id",       pa.int32()),
    pa.field("screen_name",     pa.string()),
    pa.field("screen_category", pa.string()),
    pa.field("description",     pa.string()),
])

_SCHEMA_EVENT_TYPES = pa.schema([
    pa.field("event_type_id",   pa.int32()),
    pa.field("event_name",      pa.string()),
    pa.field("event_category",  pa.string()),
    pa.field("description",     pa.string()),
])

_SCHEMA_APP_VERSIONS = pa.schema([
    pa.field("version_id",      pa.int32()),
    pa.field("version_code",    pa.string()),
    pa.field("platform",        pa.string()),
    pa.field("release_date",    pa.date32()),
    pa.field("is_force_update", pa.bool_()),
])


class PgToRawExporter:
    """Exports each PostgreSQL table as a separate Parquet file to the HDFS raw layer."""

    def __init__(
        self,
        pg_repo: AppInteractionRepository,
        hdfs_client: HdfsClient,
    ) -> None:
        self._pg_repo = pg_repo
        self._hdfs_client = hdfs_client

    def export_date(self, dt: date) -> list[str]:
        """Export all tables for one day. Returns list of HDFS paths written."""
        logger.info("Exporting raw tables for %s", dt)
        paths: list[str] = []

        for result in [
            self._export_events(dt),
            self._export_sessions(dt),
            self._export_users(dt),
            self._export_devices(dt),
            self._export_user_tiers(dt),
            self._export_screens(dt),
            self._export_event_types(dt),
            self._export_app_versions(dt),
        ]:
            if result is not None:
                paths.append(result)

        logger.info("export-raw %s: %d file(s) written", dt, len(paths))
        return paths

    def export_date_range(self, start: date, end: date) -> list[str]:
        """Export all tables for a range of dates. Returns all HDFS paths written."""
        paths: list[str] = []
        current = start
        while current <= end:
            paths.extend(self.export_date(current))
            current += timedelta(days=1)
        logger.info("export-raw complete: %d file(s) from %s to %s", len(paths), start, end)
        return paths

    # -------------------------------------------------------------------------
    # Per-table export methods
    # -------------------------------------------------------------------------

    def _export_events(self, dt: date) -> str | None:
        df = self._pg_repo.get_events_raw(dt)
        if df.empty:
            logger.info("No events for %s — skipping raw/events", dt)
            return None
        path = f"{self._hdfs_client.config.base_path}/raw/events/dt={dt}/part-00000.parquet"
        return self._write_to_hdfs(self._build_table(df, _SCHEMA_EVENTS), path)

    def _export_sessions(self, dt: date) -> str | None:
        df = self._pg_repo.get_sessions_raw(dt)
        if df.empty:
            logger.info("No sessions for %s — skipping raw/sessions", dt)
            return None
        path = f"{self._hdfs_client.config.base_path}/raw/sessions/dt={dt}/part-00000.parquet"
        return self._write_to_hdfs(self._build_table(df, _SCHEMA_SESSIONS), path)

    def _export_users(self, dt: date) -> str:
        df = self._pg_repo.get_users_snapshot()
        path = f"{self._hdfs_client.config.base_path}/raw/users/snapshot={dt}/part-00000.parquet"
        return self._write_to_hdfs(self._build_table(df, _SCHEMA_USERS), path)

    def _export_devices(self, dt: date) -> str:
        df = self._pg_repo.get_devices_snapshot()
        path = f"{self._hdfs_client.config.base_path}/raw/devices/snapshot={dt}/part-00000.parquet"
        return self._write_to_hdfs(self._build_table(df, _SCHEMA_DEVICES), path)

    def _export_user_tiers(self, dt: date) -> str:
        df = self._pg_repo.get_user_tiers_snapshot()
        path = f"{self._hdfs_client.config.base_path}/raw/user_tiers/snapshot={dt}/part-00000.parquet"
        return self._write_to_hdfs(self._build_table(df, _SCHEMA_USER_TIERS), path)

    def _export_screens(self, dt: date) -> str:
        df = self._pg_repo.get_screens_snapshot()
        path = f"{self._hdfs_client.config.base_path}/raw/screens/snapshot={dt}/part-00000.parquet"
        return self._write_to_hdfs(self._build_table(df, _SCHEMA_SCREENS), path)

    def _export_event_types(self, dt: date) -> str:
        df = self._pg_repo.get_event_types_snapshot()
        path = f"{self._hdfs_client.config.base_path}/raw/event_types/snapshot={dt}/part-00000.parquet"
        return self._write_to_hdfs(self._build_table(df, _SCHEMA_EVENT_TYPES), path)

    def _export_app_versions(self, dt: date) -> str:
        df = self._pg_repo.get_app_versions_snapshot()
        path = f"{self._hdfs_client.config.base_path}/raw/app_versions/snapshot={dt}/part-00000.parquet"
        return self._write_to_hdfs(self._build_table(df, _SCHEMA_APP_VERSIONS), path)

    # -------------------------------------------------------------------------
    # Shared infrastructure
    # -------------------------------------------------------------------------

    def _build_table(self, df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
        """Convert DataFrame to a typed PyArrow Table using the given schema."""
        for field in schema:
            col = field.name
            if col not in df.columns:
                continue
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                df[col] = df[col].fillna("")
            elif pa.types.is_integer(field.type):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64" if field.type == pa.int64() else "int32")
            elif pa.types.is_timestamp(field.type):
                df[col] = pd.to_datetime(df[col], utc=False)
            elif pa.types.is_boolean(field.type):
                df[col] = df[col].fillna(False).astype(bool)
        return pa.Table.from_pandas(df, schema=schema, safe=False)

    def _write_to_hdfs(self, table: pa.Table, hdfs_path: str) -> str:
        """Serialize as Parquet bytes in-memory and push to HDFS. No local disk I/O."""
        partition_dir = hdfs_path.rsplit("/", 1)[0]
        self._hdfs_client.makedirs(partition_dir)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        self._hdfs_client.write(hdfs_path, buf.getvalue(), overwrite=True)
        logger.debug("Wrote %d rows → %s", table.num_rows, hdfs_path)
        return hdfs_path
