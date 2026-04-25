"""Shared pytest fixtures."""

from unittest.mock import MagicMock

import pytest

from clickhouse_fundamentals.config import ClickHouseConfig
from clickhouse_fundamentals.db.client import ClickHouseClient


@pytest.fixture
def config() -> ClickHouseConfig:
    return ClickHouseConfig(host="localhost", port=8123, database="test")


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock(spec=ClickHouseClient)
