"""ETL: Read Parquet files from HDFS, load into ClickHouse star schema.

Data flows:
    HDFS bytes → io.BytesIO → pa.Table → pd.DataFrame → ClickHouse (dims + fact)

Surrogate key strategy (no hash collision risk):
    - user_key      = user_id         (PostgreSQL BIGSERIAL)
    - device_key    = device_id       (PostgreSQL BIGSERIAL)
    - screen_key    = screen_id       (PostgreSQL SERIAL, 0 for events with no screen)
    - event_type_key= event_type_id   (PostgreSQL SERIAL)
    - version_key   = version_id      (PostgreSQL SERIAL)
    - tier_key      = derived from tier_name (1=free, 2=standard, 3=premium)
    - session_key   = MD5(session_id UUID) % 2^63  — deterministic across all systems
    - date_key      = YYYYMMDD integer (e.g., 20260429)

Callers manage HdfsClient and ClickHouseClient lifetimes via context managers.
"""

from __future__ import annotations

import hashlib
import io
import logging
import time
from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow.parquet as pq

from clickhouse_fundamentals.db.client import ClickHouseClient
from clickhouse_fundamentals.hdfs.client import HdfsClient

logger = logging.getLogger(__name__)

_TIER_KEY_MAP = {"free": 1, "standard": 2, "premium": 3}


def _session_key(session_id: str) -> int:
    """Deterministic surrogate key for a UUID session_id."""
    return int(hashlib.md5(session_id.encode()).hexdigest(), 16) % (2**63)


def _date_key(d: date) -> int:
    """YYYYMMDD integer date key."""
    return int(d.strftime("%Y%m%d"))


class ParquetToStarLoader:
    """Loads daily Parquet partitions from HDFS into the ClickHouse star schema."""

    def __init__(
        self,
        hdfs_client: HdfsClient,
        ch_client: ClickHouseClient,
    ) -> None:
        self._hdfs_client = hdfs_client
        self._ch_client = ch_client
        self._ver = int(time.time())  # single version stamp per loader instance

    def load_date(self, dt: date) -> dict:
        """Load one day's Parquet partition into the star schema.

        Returns row counts per table inserted.
        """
        path = f"{self._hdfs_client.config.base_path}/silver/dt={dt}/part-00000.parquet"
        if not self._hdfs_client.exists(path):
            logger.warning("Parquet file not found for %s: %s", dt, path)
            return {}

        logger.info("Loading star schema for %s from %s", dt, path)
        df = self._read_from_hdfs(dt)
        counts: dict = {}

        self._load_dim_date([dt])
        counts["dim_date"] = 1

        counts["dim_user_tier"] = self._load_dim_user_tier(df)
        counts["dim_user"] = self._load_dim_user(df)
        counts["dim_device"] = self._load_dim_device(df)
        counts["dim_screen"] = self._load_dim_screen(df)
        counts["dim_event_type"] = self._load_dim_event_type(df)
        counts["dim_app_version"] = self._load_dim_app_version(df)
        counts["dim_session"] = self._load_dim_session(df)
        counts["fact_app_interactions"] = self._load_fact(df, dt)

        logger.info("Loaded star schema for %s: %s", dt, counts)
        return counts

    def load_date_range(self, start: date, end: date) -> dict:
        """Load a range of dates. Accumulates total row counts."""
        totals: dict = {}
        current = start
        while current <= end:
            day_counts = self.load_date(current)
            for table, count in day_counts.items():
                totals[table] = totals.get(table, 0) + count
            current += timedelta(days=1)
        return totals

    # -------------------------------------------------------------------------
    # HDFS read
    # -------------------------------------------------------------------------

    def _read_from_hdfs(self, dt: date) -> pd.DataFrame:
        path = f"{self._hdfs_client.config.base_path}/silver/dt={dt}/part-00000.parquet"
        raw_bytes = self._hdfs_client.read(path)
        table = pq.read_table(io.BytesIO(raw_bytes))
        return table.to_pandas()

    # -------------------------------------------------------------------------
    # Dimension loaders
    # -------------------------------------------------------------------------

    def _load_dim_date(self, dates: list[date]) -> int:
        rows = []
        for d in dates:
            rows.append((
                _date_key(d),
                d,
                d.year,
                (d.month - 1) // 3 + 1,
                d.month,
                d.isocalendar()[1],
                d.day,
                d.isoweekday(),  # 1=Mon, 7=Sun
                1 if d.isoweekday() >= 6 else 0,
            ))
        self._ch_client.insert(
            "dim_date",
            rows,
            ["date_key", "full_date", "year", "quarter", "month",
             "week_of_year", "day_of_month", "day_of_week", "is_weekend"],
        )
        return len(rows)

    def _load_dim_user_tier(self, df: pd.DataFrame) -> int:
        tier_rows = (
            df[["tier_name"]].drop_duplicates().assign(
                tier_key=lambda x: x["tier_name"].map(_TIER_KEY_MAP).fillna(0).astype(int),
                max_monthly_events=lambda x: x["tier_name"].map(
                    {"free": 1000, "standard": 10000, "premium": -1}
                ).fillna(1000).astype(int),
                description="",
                ver=self._ver,
            )
        )
        if tier_rows.empty:
            return 0
        rows = list(
            tier_rows[["tier_key", "tier_name", "max_monthly_events", "description", "ver"]].itertuples(
                index=False, name=None
            )
        )
        self._ch_client.insert(
            "dim_user_tier",
            rows,
            ["tier_key", "tier_name", "max_monthly_events", "description", "ver"],
        )
        return len(rows)

    def _load_dim_user(self, df: pd.DataFrame) -> int:
        user_df = (
            df[["user_id", "external_user_id", "username", "tier_name"]]
            .drop_duplicates(subset=["user_id"])
            .assign(
                user_key=lambda x: x["user_id"].astype(int),
                tier_key=lambda x: x["tier_name"].map(_TIER_KEY_MAP).fillna(0).astype(int),
                registered_at=datetime(1970, 1, 1, 0, 0, 0),
                ver=self._ver,
            )
        )
        if user_df.empty:
            return 0
        rows = list(
            user_df[["user_key", "external_user_id", "username", "tier_key", "registered_at", "ver"]]
            .itertuples(index=False, name=None)
        )
        self._ch_client.insert(
            "dim_user",
            rows,
            ["user_key", "external_user_id", "username", "tier_key", "registered_at", "ver"],
        )
        return len(rows)

    def _load_dim_device(self, df: pd.DataFrame) -> int:
        device_df = (
            df[["device_id", "device_fingerprint", "device_type", "os_name",
                "os_version", "device_model"]]
            .drop_duplicates(subset=["device_id"])
            .assign(
                device_key=lambda x: x["device_id"].astype(int),
                screen_resolution="",
                ver=self._ver,
            )
        )
        if device_df.empty:
            return 0
        rows = list(
            device_df[["device_key", "device_fingerprint", "device_type", "os_name",
                        "os_version", "device_model", "screen_resolution", "ver"]]
            .itertuples(index=False, name=None)
        )
        self._ch_client.insert(
            "dim_device",
            rows,
            ["device_key", "device_fingerprint", "device_type", "os_name",
             "os_version", "device_model", "screen_resolution", "ver"],
        )
        return len(rows)

    def _load_dim_screen(self, df: pd.DataFrame) -> int:
        screen_df = (
            df[["screen_id", "screen_name", "screen_category"]]
            .dropna(subset=["screen_id"])
            .drop_duplicates(subset=["screen_id"])
            .assign(
                screen_key=lambda x: x["screen_id"].astype(int),
                ver=self._ver,
            )
        )
        if screen_df.empty:
            return 0
        rows = list(
            screen_df[["screen_key", "screen_name", "screen_category", "ver"]]
            .itertuples(index=False, name=None)
        )
        self._ch_client.insert(
            "dim_screen",
            rows,
            ["screen_key", "screen_name", "screen_category", "ver"],
        )
        return len(rows)

    def _load_dim_event_type(self, df: pd.DataFrame) -> int:
        et_df = (
            df[["event_type_id", "event_name", "event_category"]]
            .drop_duplicates(subset=["event_type_id"])
            .assign(
                event_type_key=lambda x: x["event_type_id"].astype(int),
                ver=self._ver,
            )
        )
        if et_df.empty:
            return 0
        rows = list(
            et_df[["event_type_key", "event_name", "event_category", "ver"]]
            .itertuples(index=False, name=None)
        )
        self._ch_client.insert(
            "dim_event_type",
            rows,
            ["event_type_key", "event_name", "event_category", "ver"],
        )
        return len(rows)

    def _load_dim_app_version(self, df: pd.DataFrame) -> int:
        av_df = (
            df[["version_id", "version_code", "platform"]]
            .drop_duplicates(subset=["version_id"])
            .assign(
                version_key=lambda x: x["version_id"].astype(int),
                release_date=date(1970, 1, 1),
                is_force_update=0,
                ver=self._ver,
            )
        )
        if av_df.empty:
            return 0
        rows = list(
            av_df[["version_key", "version_code", "platform", "release_date", "is_force_update", "ver"]]
            .itertuples(index=False, name=None)
        )
        self._ch_client.insert(
            "dim_app_version",
            rows,
            ["version_key", "version_code", "platform", "release_date", "is_force_update", "ver"],
        )
        return len(rows)

    def _load_dim_session(self, df: pd.DataFrame) -> int:
        session_df = (
            df[["session_id", "user_id", "device_id", "version_id",
                "session_start", "session_end", "session_duration_seconds", "country_code"]]
            .drop_duplicates(subset=["session_id"])
            .assign(
                session_key=lambda x: x["session_id"].map(_session_key),
                user_key=lambda x: x["user_id"].astype(int),
                device_key=lambda x: x["device_id"].astype(int),
                version_key=lambda x: x["version_id"].astype(int),
                ver=self._ver,
            )
        )
        if session_df.empty:
            return 0
        rows = list(
            session_df[["session_key", "session_id", "user_key", "device_key", "version_key",
                         "session_start", "session_end", "session_duration_seconds",
                         "country_code", "ver"]]
            .itertuples(index=False, name=None)
        )
        self._ch_client.insert(
            "dim_session",
            rows,
            ["session_key", "session_id", "user_key", "device_key", "version_key",
             "session_start", "session_end", "duration_seconds", "country_code", "ver"],
        )
        return len(rows)

    def _load_fact(self, df: pd.DataFrame, dt: date) -> int:
        """Load fact rows in batches of 100k."""
        dk = _date_key(dt)
        fact_df = df.assign(
            session_key=df["session_id"].map(_session_key),
            user_key=df["user_id"].astype(int),
            device_key=df["device_id"].astype(int),
            screen_key=df["screen_id"].fillna(0).astype(int),
            event_type_key=df["event_type_id"].astype(int),
            version_key=df["version_id"].astype(int),
            date_key=dk,
            country_code=df["country_code"].fillna(""),
        )

        cols = ["event_id", "session_key", "user_key", "device_key", "screen_key",
                "event_type_key", "version_key", "date_key", "event_timestamp",
                "sequence_number", "duration_ms", "country_code"]

        batch_size = 100_000
        total = 0
        for start in range(0, len(fact_df), batch_size):
            batch = fact_df.iloc[start: start + batch_size]
            rows = list(batch[cols].itertuples(index=False, name=None))
            self._ch_client.insert("fact_app_interactions", rows, cols)
            total += len(rows)

        return total
