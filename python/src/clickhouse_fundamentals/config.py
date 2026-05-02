"""Configuration module for ClickHouse, PostgreSQL, and HDFS connection settings."""

import os
from dataclasses import dataclass, field


@dataclass
class ClickHouseConfig:
    """ClickHouse connection configuration.

    Reads from environment variables with sensible defaults.
    Works both locally (localhost) and inside Docker (clickhouse service name).
    """

    host: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("CLICKHOUSE_PORT", "8123")))
    user: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_USER", "default"))
    password: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_PASSWORD", ""))
    database: str = field(
        default_factory=lambda: os.getenv("CLICKHOUSE_DATABASE", "default")
    )

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("CLICKHOUSE_HOST must not be empty")
        if not (1 <= self.port <= 65535):
            raise ValueError(
                f"CLICKHOUSE_PORT must be between 1 and 65535, got: {self.port}"
            )
        if not self.database:
            raise ValueError("CLICKHOUSE_DATABASE must not be empty")


@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration."""

    host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("PG_PORT", "5432")))
    user: str = field(default_factory=lambda: os.getenv("PG_USER", "pguser"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "pgpassword"))
    database: str = field(default_factory=lambda: os.getenv("PG_DATABASE", "appdb"))

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("PG_HOST must not be empty")
        if not (1 <= self.port <= 65535):
            raise ValueError(f"PG_PORT must be between 1 and 65535, got: {self.port}")
        if not self.database:
            raise ValueError("PG_DATABASE must not be empty")

    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )

    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class HdfsConfig:
    """HDFS WebHDFS connection configuration."""

    host: str = field(default_factory=lambda: os.getenv("HDFS_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("HDFS_PORT", "9870")))
    user: str = field(default_factory=lambda: os.getenv("HDFS_USER", "root"))
    base_path: str = field(
        default_factory=lambda: os.getenv("HDFS_BASE_PATH", "/data/app_interactions")
    )

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("HDFS_HOST must not be empty")
        if not (1 <= self.port <= 65535):
            raise ValueError(f"HDFS_PORT must be between 1 and 65535, got: {self.port}")

    def url(self) -> str:
        return f"http://{self.host}:{self.port}"
