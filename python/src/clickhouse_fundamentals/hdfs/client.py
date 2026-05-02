"""HDFS WebHDFS client with retry logic and context manager support."""

from __future__ import annotations

import logging
import time
from types import TracebackType

import hdfs

from clickhouse_fundamentals.config import HdfsConfig

logger = logging.getLogger(__name__)


class HdfsError(Exception):
    """Base exception for HDFS client errors."""


class HdfsConnectionError(HdfsError):
    """Raised when the NameNode cannot be reached after retries."""


class HdfsOperationError(HdfsError):
    """Raised when an HDFS operation fails (path not found, permission, etc.)."""


class HdfsClient:
    """WebHDFS client wrapper with retry logic and context manager support.

    Uses the WebHDFS REST API (port 9870) via the `hdfs` library.
    All I/O methods retry on transient errors using exponential backoff.

    Usage:
        with HdfsClient(config) as client:
            client.makedirs("/data/app_interactions/dt=2026-04-29")
            client.write("/data/app_interactions/dt=2026-04-29/part-00000.parquet", data)
            raw = client.read("/data/app_interactions/dt=2026-04-29/part-00000.parquet")
    """

    def __init__(
        self,
        config: HdfsConfig,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self._config = config
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._client: hdfs.InsecureClient | None = None

    @property
    def config(self) -> HdfsConfig:
        return self._config

    def __enter__(self) -> "HdfsClient":
        self._connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # WebHDFS is stateless HTTP — no persistent connection to close.
        self._client = None

    def _connect(self) -> None:
        """Create the InsecureClient and verify connectivity."""
        for attempt in range(1, self._max_retries + 1):
            try:
                self._client = hdfs.InsecureClient(self._config.url(), user=self._config.user)
                # Probe with a lightweight status call on root
                self._client.status("/")
                logger.debug("HDFS connected to %s", self._config.url())
                return
            except hdfs.HdfsError as exc:
                if attempt == self._max_retries:
                    raise HdfsConnectionError(
                        f"Cannot connect to HDFS at {self._config.url()} "
                        f"after {self._max_retries} attempts: {exc}"
                    ) from exc
                wait = self._retry_delay * (2 ** (attempt - 1))
                logger.warning("HDFS connection attempt %d failed, retrying in %.1fs", attempt, wait)
                time.sleep(wait)

    def _get_client(self) -> hdfs.InsecureClient:
        if self._client is None:
            self._connect()
        assert self._client is not None
        return self._client

    def _retry(self, operation_name: str, fn):  # type: ignore[type-arg]
        """Execute fn with exponential backoff retries."""
        for attempt in range(1, self._max_retries + 1):
            try:
                return fn()
            except hdfs.HdfsError as exc:
                if attempt == self._max_retries:
                    raise HdfsOperationError(
                        f"HDFS {operation_name} failed after {self._max_retries} attempts: {exc}"
                    ) from exc
                wait = self._retry_delay * (2 ** (attempt - 1))
                logger.warning("HDFS %s attempt %d failed, retrying in %.1fs", operation_name, attempt, wait)
                time.sleep(wait)

    def ping(self) -> bool:
        """Return True if the NameNode is reachable."""
        try:
            self._get_client().status("/")
            return True
        except Exception:
            return False

    def makedirs(self, path: str) -> None:
        """Create directory and all parents (idempotent)."""
        def _op() -> None:
            self._get_client().makedirs(path)

        self._retry("makedirs", _op)
        logger.debug("HDFS makedirs: %s", path)

    def write(self, path: str, data: bytes, overwrite: bool = True) -> None:
        """Write bytes to an HDFS path."""
        import io

        def _op() -> None:
            self._get_client().write(path, data=io.BytesIO(data), overwrite=overwrite)

        self._retry("write", _op)
        logger.debug("HDFS write: %s (%d bytes)", path, len(data))

    def read(self, path: str) -> bytes:
        """Read file content as bytes from an HDFS path."""
        def _op() -> bytes:
            with self._get_client().read(path) as reader:
                return reader.read()

        result = self._retry("read", _op)
        logger.debug("HDFS read: %s", path)
        return result  # type: ignore[return-value]

    def list(self, path: str) -> list[str]:
        """List directory contents. Returns a list of file/directory names."""
        def _op() -> list[str]:
            return self._get_client().list(path)  # type: ignore[return-value]

        result = self._retry("list", _op)
        return result  # type: ignore[return-value]

    def exists(self, path: str) -> bool:
        """Return True if the path exists on HDFS."""
        try:
            self._get_client().status(path)
            return True
        except hdfs.HdfsError:
            return False

    def delete(self, path: str, recursive: bool = False) -> None:
        """Delete a file or directory."""
        def _op() -> None:
            self._get_client().delete(path, recursive=recursive)

        self._retry("delete", _op)
        logger.debug("HDFS delete: %s (recursive=%s)", path, recursive)

    def get_file_size(self, path: str) -> int:
        """Return the file size in bytes."""
        def _op() -> int:
            status = self._get_client().status(path)
            return int(status["length"])

        result = self._retry("status", _op)
        return result  # type: ignore[return-value]
