"""PostgreSQL client with retry logic and context manager support."""

from __future__ import annotations

import logging
import time
from types import TracebackType
from typing import Any

import pandas as pd
import psycopg2
import psycopg2.extras

from clickhouse_fundamentals.config import PostgresConfig

logger = logging.getLogger(__name__)


class PostgresError(Exception):
    """Base exception for PostgreSQL client errors."""


class PostgresConnectionError(PostgresError):
    """Raised when connection cannot be established after retries."""


class PostgresQueryError(PostgresError):
    """Raised when a query fails after retries."""


class PostgresClient:
    """PostgreSQL connection wrapper with retry logic and context manager support.

    Usage:
        with PostgresClient(config) as client:
            rows = client.query("SELECT * FROM users LIMIT 10")
    """

    def __init__(
        self,
        config: PostgresConfig,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self._config = config
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._conn: psycopg2.extensions.connection | None = None

    def __enter__(self) -> "PostgresClient":
        self._connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._close()

    def _connect(self) -> None:
        for attempt in range(1, self._max_retries + 1):
            try:
                self._conn = psycopg2.connect(self._config.dsn())
                self._conn.autocommit = False
                logger.debug("PostgreSQL connected to %s:%d", self._config.host, self._config.port)
                return
            except psycopg2.OperationalError as exc:
                if attempt == self._max_retries:
                    raise PostgresConnectionError(
                        f"Cannot connect to PostgreSQL after {self._max_retries} attempts: {exc}"
                    ) from exc
                wait = self._retry_delay * (2 ** (attempt - 1))
                logger.warning("PostgreSQL connection attempt %d failed, retrying in %.1fs", attempt, wait)
                time.sleep(wait)

    def _close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _ensure_connected(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._connect()
        assert self._conn is not None
        return self._conn

    def ping(self) -> bool:
        """Return True if the connection is alive."""
        try:
            conn = self._ensure_connected()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        """Execute a DDL or DML statement (INSERT/UPDATE/DELETE/CREATE)."""
        conn = self._ensure_connected()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
        except psycopg2.Error as exc:
            conn.rollback()
            raise PostgresQueryError(f"Query failed: {exc}") from exc

    def execute_many(self, query: str, params_list: list[tuple[Any, ...]]) -> int:
        """Bulk-execute a parameterised statement. Returns number of rows affected."""
        if not params_list:
            return 0
        conn = self._ensure_connected()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, query, params_list, page_size=1000)
            conn.commit()
            return len(params_list)
        except psycopg2.Error as exc:
            conn.rollback()
            raise PostgresQueryError(f"Bulk execute failed: {exc}") from exc

    def execute_values(
        self,
        query: str,
        params_list: list[tuple[Any, ...]],
        template: str | None = None,
    ) -> int:
        """High-performance bulk insert using execute_values. Returns rows inserted."""
        if not params_list:
            return 0
        conn = self._ensure_connected()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, query, params_list, template=template, page_size=1000)
            conn.commit()
            return len(params_list)
        except psycopg2.Error as exc:
            conn.rollback()
            raise PostgresQueryError(f"execute_values failed: {exc}") from exc

    def query(self, query: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        """Execute a SELECT and return rows as a list of tuples."""
        conn = self._ensure_connected()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except psycopg2.Error as exc:
            raise PostgresQueryError(f"Query failed: {exc}") from exc

    def query_df(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> pd.DataFrame:
        """Execute a SELECT and return results as a pandas DataFrame."""
        conn = self._ensure_connected()
        try:
            return pd.read_sql_query(query, conn, params=params)
        except Exception as exc:
            raise PostgresQueryError(f"query_df failed: {exc}") from exc

    def table_exists(self, table_name: str) -> bool:
        """Return True if the table exists in the public schema."""
        rows = self.query(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table_name,),
        )
        return len(rows) > 0

    def get_row_count(self, table_name: str) -> int:
        """Return approximate row count (uses pg_class stats)."""
        rows = self.query(
            "SELECT reltuples::BIGINT FROM pg_class WHERE relname = %s",
            (table_name,),
        )
        return int(rows[0][0]) if rows else 0
