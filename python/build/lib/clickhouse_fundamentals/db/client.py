"""ClickHouse client wrapper with connection management and retry logic."""

import logging
import time

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client

from clickhouse_fundamentals.config import ClickHouseConfig

logger = logging.getLogger(__name__)


class ClickHouseError(Exception):
    """Base exception for ClickHouse operations."""

    pass


class ClickHouseConnectionError(ClickHouseError):
    """Exception for connection failures."""

    pass


class QueryError(ClickHouseError):
    """Exception for query execution failures."""

    pass


class ClickHouseClient:
    """ClickHouse client wrapper with connection management.

    Features:
    - Context manager support for automatic cleanup
    - Retry logic with exponential backoff
    - Simplified interface for common operations
    - Built-in connection pooling via clickhouse-connect

    Usage:
        with ClickHouseClient(config) as client:
            result = client.query("SELECT 1")
    """

    def __init__(
        self,
        config: ClickHouseConfig,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        """Initialize the ClickHouse client.

        Args:
            config: ClickHouse connection configuration.
            max_retries: Maximum number of retry attempts for failed operations.
            retry_delay: Initial delay between retries (exponential backoff).
        """
        self.config = config
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._client: Client | None = None

    def _connect(self) -> Client:
        """Create a new connection to ClickHouse.

        Returns:
            ClickHouse client instance.

        Raises:
            ClickHouseConnectionError: If connection fails after retries.
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                client = clickhouse_connect.get_client(
                    host=self.config.host,
                    port=self.config.port,
                    username=self.config.user,
                    password=self.config.password,
                    database=self.config.database,
                    connect_timeout=10,
                    send_receive_timeout=300,
                )
                # Test connection
                client.ping()
                logger.info(
                    f"Connected to ClickHouse at {self.config.host}:{self.config.port}"
                )
                return client

            except Exception as e:
                last_error = e
                delay = self.retry_delay * (2**attempt)
                logger.warning(
                    f"Connection attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)

        raise ClickHouseConnectionError(
            f"Failed to connect to ClickHouse after {self.max_retries} attempts: {last_error}"
        )

    @property
    def client(self) -> Client:
        """Get or create the ClickHouse client.

        Returns:
            Active ClickHouse client instance.
        """
        if self._client is None:
            self._client = self._connect()
        return self._client

    def __enter__(self) -> "ClickHouseClient":
        """Enter context manager."""
        self._client = self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and close connection."""
        self.close()

    def close(self) -> None:
        """Close the client connection."""
        if self._client is not None:
            try:
                self._client.close()
                logger.debug("ClickHouse connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._client = None

    def ping(self) -> bool:
        """Check if the connection is alive.

        Returns:
            True if connection is healthy, False otherwise.
        """
        try:
            self.client.ping()
            return True
        except Exception as e:
            logger.warning(f"Ping failed: {e}")
            return False

    def execute(self, query: str, parameters: dict | None = None) -> None:
        """Execute a query without returning results.

        Useful for DDL statements (CREATE, DROP, ALTER) and DML (INSERT, DELETE).

        Args:
            query: SQL query to execute.
            parameters: Optional query parameters.

        Raises:
            QueryError: If query execution fails.
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                self.client.command(query, parameters=parameters)
                logger.debug(f"Executed query: {query[:100]}...")
                return

            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    logger.warning(
                        f"Query attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    # Reconnect on failure
                    self._client = None

        raise QueryError(
            f"Query failed after {self.max_retries} attempts: {last_error}"
        )

    def query(
        self,
        query: str,
        parameters: dict | None = None,
    ) -> list[tuple]:
        """Execute a query and return results as a list of tuples.

        Args:
            query: SQL query to execute.
            parameters: Optional query parameters.

        Returns:
            List of result rows as tuples.

        Raises:
            QueryError: If query execution fails after retries.
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                result = self.client.query(query, parameters=parameters)
                logger.debug(
                    f"Query returned {len(result.result_rows)} rows: {query[:80]}..."
                )
                return result.result_rows

            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    logger.warning(
                        f"Query attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    self._client = None

        raise QueryError(
            f"Query failed after {self.max_retries} attempts: {last_error}"
        ) from last_error

    def query_df(
        self,
        query: str,
        parameters: dict | None = None,
    ) -> pd.DataFrame:
        """Execute a query and return results as a pandas DataFrame.

        Args:
            query: SQL query to execute.
            parameters: Optional query parameters.

        Returns:
            Query results as a DataFrame.

        Raises:
            QueryError: If query execution fails after retries.
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                df = self.client.query_df(query, parameters=parameters)
                logger.debug(f"Query returned {len(df)} rows: {query[:80]}...")
                return df

            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    logger.warning(
                        f"Query attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    self._client = None

        raise QueryError(
            f"Query failed after {self.max_retries} attempts: {last_error}"
        ) from last_error

    def insert(
        self,
        table: str,
        data: list[tuple],
        column_names: list[str],
    ) -> int:
        """Insert data into a table.

        Args:
            table: Target table name.
            data: List of rows as tuples.
            column_names: Column names matching the tuple order.

        Returns:
            Number of rows inserted.

        Raises:
            QueryError: If insert fails.
        """
        if not data:
            logger.debug("No data to insert")
            return 0

        try:
            self.client.insert(
                table=table,
                data=data,
                column_names=column_names,
            )
            row_count = len(data)
            logger.debug(f"Inserted {row_count} rows into {table}")
            return row_count

        except Exception as e:
            raise QueryError(f"Insert failed: {e}") from e

    def insert_df(self, table: str, df: pd.DataFrame) -> int:
        """Insert a DataFrame into a table.

        Args:
            table: Target table name.
            df: DataFrame to insert.

        Returns:
            Number of rows inserted.

        Raises:
            QueryError: If insert fails.
        """
        if df.empty:
            logger.debug("No data to insert")
            return 0

        try:
            self.client.insert_df(table=table, df=df)
            row_count = len(df)
            logger.debug(f"Inserted {row_count} rows into {table}")
            return row_count

        except Exception as e:
            raise QueryError(f"Insert failed: {e}") from e

    def table_exists(self, table: str) -> bool:
        """Check if a table exists.

        Args:
            table: Table name to check.

        Returns:
            True if table exists, False otherwise.
        """
        result = self.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {table:String}",
            parameters={"db": self.config.database, "table": table},
        )
        return result[0][0] > 0

    def get_row_count(self, table: str) -> int:
        """Get the approximate row count for a table.

        Uses system.tables for fast approximate count.

        Args:
            table: Table name.

        Returns:
            Approximate row count.
        """
        result = self.query(
            "SELECT total_rows FROM system.tables WHERE database = {db:String} AND name = {table:String}",
            parameters={"db": self.config.database, "table": table},
        )
        if result:
            return result[0][0] or 0
        return 0
