"""Tests for ClickHouseClient (unit — no live ClickHouse required)."""

from unittest.mock import MagicMock, patch

import pytest

from clickhouse_fundamentals.config import ClickHouseConfig
from clickhouse_fundamentals.db.client import (
    ClickHouseClient,
    ClickHouseConnectionError,
    QueryError,
)


@pytest.fixture
def config():
    return ClickHouseConfig(host="localhost", port=8123, database="test")


def _client_with_mock_inner(
    config: ClickHouseConfig,
) -> tuple[ClickHouseClient, MagicMock]:
    client = ClickHouseClient(config)
    inner = MagicMock()
    client._client = inner
    return client, inner


def test_ping_returns_true_when_healthy(config):
    client, inner = _client_with_mock_inner(config)
    assert client.ping() is True
    inner.ping.assert_called_once()


def test_ping_returns_false_on_exception(config):
    client, inner = _client_with_mock_inner(config)
    inner.ping.side_effect = Exception("refused")
    assert client.ping() is False


def test_context_manager_closes_connection(config):
    with patch.object(ClickHouseClient, "_connect", return_value=MagicMock()):
        with ClickHouseClient(config) as client:
            assert client._client is not None
        assert client._client is None


def test_insert_empty_list_returns_zero(config):
    client, _ = _client_with_mock_inner(config)
    assert client.insert("t", [], ["col"]) == 0


def test_insert_raises_query_error_on_failure(config):
    client, inner = _client_with_mock_inner(config)
    inner.insert.side_effect = Exception("disk full")
    with pytest.raises(QueryError, match="Insert failed"):
        client.insert("t", [(1,)], ["col"])


def test_insert_df_empty_returns_zero(config):
    import pandas as pd

    client, _ = _client_with_mock_inner(config)
    assert client.insert_df("t", pd.DataFrame()) == 0


def test_table_exists_true(config):
    client, inner = _client_with_mock_inner(config)
    inner.query.return_value.result_rows = [(1,)]
    assert client.table_exists("transactions") is True


def test_table_exists_false(config):
    client, inner = _client_with_mock_inner(config)
    inner.query.return_value.result_rows = [(0,)]
    assert client.table_exists("nonexistent") is False


def test_connect_raises_clickhouse_connection_error_after_retries(config):
    with patch("clickhouse_connect.get_client", side_effect=Exception("timeout")):
        client = ClickHouseClient(config, max_retries=1, retry_delay=0)
        with pytest.raises(ClickHouseConnectionError):
            client._connect()


def test_clickhouse_connection_error_is_subclass_of_clickhouse_error(config):
    from clickhouse_fundamentals.db.client import ClickHouseError

    assert issubclass(ClickHouseConnectionError, ClickHouseError)
