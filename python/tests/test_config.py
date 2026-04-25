"""Tests for ClickHouseConfig."""

import pytest

from clickhouse_fundamentals.config import ClickHouseConfig


def test_default_values(monkeypatch):
    for var in (
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_PORT",
        "CLICKHOUSE_USER",
        "CLICKHOUSE_PASSWORD",
        "CLICKHOUSE_DATABASE",
    ):
        monkeypatch.delenv(var, raising=False)

    cfg = ClickHouseConfig()
    assert cfg.host == "localhost"
    assert cfg.port == 8123
    assert cfg.user == "default"
    assert cfg.password == ""
    assert cfg.database == "default"


def test_env_override(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HOST", "ch-server")
    monkeypatch.setenv("CLICKHOUSE_PORT", "9000")
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "mydb")

    cfg = ClickHouseConfig()
    assert cfg.host == "ch-server"
    assert cfg.port == 9000
    assert cfg.database == "mydb"


def test_empty_host_raises():
    with pytest.raises(ValueError, match="CLICKHOUSE_HOST"):
        ClickHouseConfig(host="")


def test_port_zero_raises():
    with pytest.raises(ValueError, match="CLICKHOUSE_PORT"):
        ClickHouseConfig(port=0)


def test_port_too_large_raises():
    with pytest.raises(ValueError, match="CLICKHOUSE_PORT"):
        ClickHouseConfig(port=65536)


def test_port_boundary_values():
    assert ClickHouseConfig(port=1).port == 1
    assert ClickHouseConfig(port=65535).port == 65535


def test_empty_database_raises():
    with pytest.raises(ValueError, match="CLICKHOUSE_DATABASE"):
        ClickHouseConfig(database="")
