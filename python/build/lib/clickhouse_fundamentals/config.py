"""Configuration module for ClickHouse connection settings."""

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
