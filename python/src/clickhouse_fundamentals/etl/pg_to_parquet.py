"""ETL: Read mobile app interaction data from PostgreSQL, write Parquet to HDFS.

Data flows:
    PostgreSQL (3NF joined query) → pd.DataFrame → pa.Table → io.BytesIO → HDFS bytes

The Parquet files are written with daily partitions:
    {base_path}/dt=YYYY-MM-DD/part-00000.parquet

No Parquet file is written to local disk — the buffer stays in-memory.
Callers manage HdfsClient and AppInteractionRepository lifetimes via context managers.
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

# Explicit pyarrow schema — enforces correct types regardless of DataFrame inference
_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.int64()),
        pa.field("event_timestamp", pa.timestamp("us")),
        pa.field("sequence_number", pa.int32()),
        pa.field("duration_ms", pa.int32()),
        pa.field("properties", pa.string()),
        pa.field("session_id", pa.string()),
        pa.field("session_start", pa.timestamp("us")),
        pa.field("session_end", pa.timestamp("us")),
        pa.field("session_duration_seconds", pa.int32()),
        pa.field("country_code", pa.string()),
        pa.field("user_id", pa.int64()),
        pa.field("external_user_id", pa.string()),
        pa.field("username", pa.string()),
        pa.field("tier_name", pa.string()),
        pa.field("device_id", pa.int64()),
        pa.field("device_fingerprint", pa.string()),
        pa.field("device_type", pa.string()),
        pa.field("os_name", pa.string()),
        pa.field("os_version", pa.string()),
        pa.field("device_model", pa.string()),
        pa.field("screen_id", pa.int32()),
        pa.field("screen_name", pa.string()),
        pa.field("screen_category", pa.string()),
        pa.field("event_type_id", pa.int32()),
        pa.field("event_name", pa.string()),
        pa.field("event_category", pa.string()),
        pa.field("version_id", pa.int32()),
        pa.field("version_code", pa.string()),
        pa.field("platform", pa.string()),
    ]
)


class PgToParquetExporter:
    """Exports daily app interaction data from PostgreSQL to HDFS Parquet files."""

    def __init__(
        self,
        pg_repo: AppInteractionRepository,
        hdfs_client: HdfsClient,
    ) -> None:
        self._pg_repo = pg_repo
        self._hdfs_client = hdfs_client

    def export_date(self, dt: date) -> str | None:
        """Export one day's events to HDFS.

        Returns the HDFS path written, or None if there were no events for that day.
        """
        logger.info("Exporting events for %s", dt)
        df = self._pg_repo.get_events_by_date(dt)
        if df.empty:
            logger.info("No events for %s — skipping", dt)
            return None

        table = self._build_table(df)
        path = self._write_to_hdfs(table, dt)
        logger.info("Exported %d events for %s → %s", len(df), dt, path)
        return path

    def export_date_range(self, start: date, end: date) -> list[str]:
        """Export a range of dates. Returns list of HDFS paths written (skips empty days)."""
        paths: list[str] = []
        current = start
        while current <= end:
            path = self.export_date(current)
            if path is not None:
                paths.append(path)
            current += timedelta(days=1)
        logger.info("Exported %d partition(s) from %s to %s", len(paths), start, end)
        return paths

    def _build_table(self, df: pd.DataFrame) -> pa.Table:
        """Convert DataFrame to a typed pyarrow Table using the declared schema."""
        # Fill nulls for non-nullable string/int columns
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

        # Ensure timestamp columns are proper datetime
        for ts_col in ("event_timestamp", "session_start", "session_end"):
            if ts_col in df.columns:
                df[ts_col] = pd.to_datetime(df[ts_col], utc=False)

        return pa.Table.from_pandas(df, schema=_PARQUET_SCHEMA, safe=False)

    def _write_to_hdfs(self, table: pa.Table, partition_date: date) -> str:
        """Serialise as Parquet bytes in-memory, push to HDFS via HdfsClient.

        No file touches local disk — buffer lives only in RAM.
        """
        partition_dir = f"{self._hdfs_client.config.base_path}/dt={partition_date}"
        path = f"{partition_dir}/part-00000.parquet"

        self._hdfs_client.makedirs(partition_dir)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        # getvalue() returns full buffer contents without closing or seeking
        self._hdfs_client.write(path, buf.getvalue(), overwrite=True)
        return path
