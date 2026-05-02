"""ETL: Join raw HDFS Parquet files into the denormalized silver layer.

Reads per-table Parquet files from the raw layer and reproduces the same
7-table JOIN that was previously done at PostgreSQL query time. The result
is a single flat Parquet file per day — identical in schema to what
pg_to_parquet.py produced — stored in the silver layer for star-load to consume.

No PostgreSQL dependency: this stage only reads from HDFS.

Data flows:
    HDFS raw/ (8 tables) → pd.DataFrames → pandas merge → pa.Table → HDFS silver/

HDFS path:
    Input : {base_path}/raw/{table}/dt=YYYY-MM-DD/   (facts)
             {base_path}/raw/{table}/snapshot=YYYY-MM-DD/   (dims)
    Output: {base_path}/silver/dt=YYYY-MM-DD/part-00000.parquet
"""

from __future__ import annotations

import io
import logging
from datetime import date, timedelta

import pandas as pd
import pyarrow.parquet as pq

from clickhouse_fundamentals.etl.pg_to_parquet import _PARQUET_SCHEMA as _SILVER_SCHEMA
from clickhouse_fundamentals.hdfs.client import HdfsClient

logger = logging.getLogger(__name__)

# Columns to carry forward from each raw table into the silver output.
# Matches _PARQUET_SCHEMA field order exactly so pa.Table.from_pandas works cleanly.
_SILVER_COLS = [
    "event_id", "event_timestamp", "sequence_number", "duration_ms", "properties",
    "session_id", "session_start", "session_end", "session_duration_seconds",
    "country_code", "user_id", "external_user_id", "username", "tier_name",
    "device_id", "device_fingerprint", "device_type", "os_name", "os_version", "device_model",
    "screen_id", "screen_name", "screen_category",
    "event_type_id", "event_name", "event_category",
    "version_id", "version_code", "platform",
]


class RawToSilverTransformer:
    """Reads raw HDFS Parquet tables and writes a denormalized silver partition per day."""

    def __init__(self, hdfs_client: HdfsClient) -> None:
        self._hdfs_client = hdfs_client

    def transform_date(self, dt: date) -> str | None:
        """Transform one day's raw partitions into a silver Parquet file.

        Returns the HDFS silver path written, or None if raw events are missing.
        """
        events_path = f"{self._hdfs_client.config.base_path}/raw/events/dt={dt}/part-00000.parquet"
        if not self._hdfs_client.exists(events_path):
            logger.warning("Raw events not found for %s — skipping silver transform", dt)
            return None

        logger.info("Transforming raw → silver for %s", dt)
        df = self._join_silver(dt)
        if df.empty:
            logger.info("Join produced empty result for %s — skipping", dt)
            return None

        table = self._build_silver_table(df)
        path = self._write_silver(table, dt)
        logger.info("Silver partition written: %s (%d rows)", path, len(df))
        return path

    def transform_date_range(self, start: date, end: date) -> list[str]:
        """Transform a range of dates. Returns list of silver paths written."""
        paths: list[str] = []
        current = start
        while current <= end:
            path = self.transform_date(current)
            if path is not None:
                paths.append(path)
            current += timedelta(days=1)
        logger.info("raw-to-silver complete: %d partition(s) from %s to %s", len(paths), start, end)
        return paths

    # -------------------------------------------------------------------------
    # Core transformation
    # -------------------------------------------------------------------------

    def _join_silver(self, dt: date) -> pd.DataFrame:
        """Read all raw tables for dt and reproduce the original 7-table JOIN."""
        events   = self._read_raw("events",      "dt",       dt)
        sessions = self._read_raw("sessions",    "dt",       dt)
        users    = self._read_raw("users",       "snapshot", dt)
        tiers    = self._read_raw("user_tiers",  "snapshot", dt)
        devices  = self._read_raw("devices",     "snapshot", dt)
        screens  = self._read_raw("screens",     "snapshot", dt)
        etypes   = self._read_raw("event_types", "snapshot", dt)
        avers    = self._read_raw("app_versions","snapshot", dt)

        # Reproduce the computed column from the original SQL EXTRACT(EPOCH ...) cast
        sessions = sessions.copy()
        sessions["session_duration_seconds"] = (
            (pd.to_datetime(sessions["session_end"]) - pd.to_datetime(sessions["session_start"]))
            .dt.total_seconds()
            .fillna(0)
            .astype("int32")
        )

        # Step 1: events JOIN sessions
        df = events.merge(
            sessions[[
                "session_id", "user_id", "device_id", "version_id",
                "session_start", "session_end", "session_duration_seconds", "country_code",
            ]],
            on="session_id",
            how="inner",
        )

        # Step 2: JOIN users
        df = df.merge(
            users[["user_id", "external_user_id", "username", "tier_id"]],
            on="user_id",
            how="inner",
        )

        # Step 3: JOIN user_tiers (resolves tier_name from tier_id)
        df = df.merge(
            tiers[["tier_id", "tier_name"]],
            on="tier_id",
            how="inner",
        )

        # Step 4: JOIN devices
        df = df.merge(
            devices[[
                "device_id", "device_fingerprint", "device_type",
                "os_name", "os_version", "device_model",
            ]],
            on="device_id",
            how="inner",
        )

        # Step 5: LEFT JOIN screens (screen_id is nullable on events)
        df = df.merge(
            screens[["screen_id", "screen_name", "screen_category"]],
            on="screen_id",
            how="left",
        )

        # Step 6: JOIN event_types
        df = df.merge(
            etypes[["event_type_id", "event_name", "event_category"]],
            on="event_type_id",
            how="inner",
        )

        # Step 7: JOIN app_versions
        df = df.merge(
            avers[["version_id", "version_code", "platform"]],
            on="version_id",
            how="inner",
        )

        # Drop join-key columns not in silver output (tier_id, ip_address)
        # and enforce exact column order matching _SILVER_SCHEMA
        return df[_SILVER_COLS]

    # -------------------------------------------------------------------------
    # HDFS I/O
    # -------------------------------------------------------------------------

    def _read_raw(self, table_name: str, partition_key: str, dt: date) -> pd.DataFrame:
        path = (
            f"{self._hdfs_client.config.base_path}/raw/{table_name}"
            f"/{partition_key}={dt}/part-00000.parquet"
        )
        raw_bytes = self._hdfs_client.read(path)
        return pq.read_table(io.BytesIO(raw_bytes)).to_pandas()

    def _build_silver_table(self, df: pd.DataFrame):
        """Apply null-fills and type casts then convert to PyArrow Table."""
        import pyarrow as pa

        string_cols = [
            "country_code", "screen_name", "screen_category",
            "properties", "tier_name", "device_fingerprint",
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna("")

        int_cols = ["screen_id", "session_duration_seconds", "duration_ms", "sequence_number"]
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int32")

        for ts_col in ("event_timestamp", "session_start", "session_end"):
            if ts_col in df.columns:
                df[ts_col] = pd.to_datetime(df[ts_col], utc=False)

        return pa.Table.from_pandas(df, schema=_SILVER_SCHEMA, safe=False)

    def _write_silver(self, table, dt: date) -> str:
        """Serialize to Parquet bytes in-memory and push to HDFS silver layer."""
        import pyarrow.parquet as pq

        partition_dir = f"{self._hdfs_client.config.base_path}/silver/dt={dt}"
        path = f"{partition_dir}/part-00000.parquet"
        self._hdfs_client.makedirs(partition_dir)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        self._hdfs_client.write(path, buf.getvalue(), overwrite=True)
        return path
