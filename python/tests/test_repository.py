"""Tests for TransactionRepository (unit — uses a mock ClickHouseClient)."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from clickhouse_fundamentals.db.client import ClickHouseClient
from clickhouse_fundamentals.db.repository import TransactionRepository


@pytest.fixture
def mock_client():
    return MagicMock(spec=ClickHouseClient)


@pytest.fixture
def repo(mock_client):
    return TransactionRepository(mock_client)


# --- insert_batch ---


def test_insert_batch_empty_returns_zero(repo, mock_client):
    assert repo.insert_batch([]) == 0
    mock_client.insert.assert_not_called()


# --- get_by_user validation ---


def test_get_by_user_invalid_user_id_raises(repo):
    with pytest.raises(ValueError, match="user_id"):
        repo.get_by_user(0)


def test_get_by_user_invalid_limit_raises(repo):
    with pytest.raises(ValueError, match="limit"):
        repo.get_by_user(1, limit=0)


def test_get_by_user_invalid_offset_raises(repo):
    with pytest.raises(ValueError, match="offset"):
        repo.get_by_user(1, offset=-1)


# --- get_revenue_by_merchant validation ---


def test_get_revenue_by_merchant_invalid_limit_raises(repo):
    with pytest.raises(ValueError, match="limit"):
        repo.get_revenue_by_merchant(limit=0)


# --- get_user_spending_summary ---


def test_get_user_spending_summary_invalid_user_id_raises(repo):
    with pytest.raises(ValueError, match="user_id"):
        repo.get_user_spending_summary(0)


def test_get_user_spending_summary_returns_typed_dict_on_empty(repo, mock_client):
    mock_client.query.return_value = [(0, None, None, None, None, 0, 0, None, None)]
    result = repo.get_user_spending_summary(1)
    assert result["user_id"] == 1
    assert result["total_transactions"] == 0
    assert result["total_spent"] == Decimal("0.00")
    assert result["days_active"] == 0
    assert result["favorite_merchant"] == 0


def test_get_user_spending_summary_no_rows_returns_zeros(repo, mock_client):
    mock_client.query.return_value = []
    result = repo.get_user_spending_summary(42)
    assert result["total_transactions"] == 0


# --- get_total_stats ---


def test_get_total_stats_empty_table(repo, mock_client):
    mock_client.query.return_value = []
    stats = repo.get_total_stats()
    assert stats["total_transactions"] == 0
    assert stats["total_volume"] == Decimal("0.00")
    assert stats["earliest_transaction"] is None


# --- _to_decimal ---


def test_to_decimal_none_returns_fallback():
    assert TransactionRepository._to_decimal(None) == Decimal("0.00")


def test_to_decimal_string_value():
    assert TransactionRepository._to_decimal("123.45") == Decimal("123.45")


def test_to_decimal_custom_fallback():
    assert TransactionRepository._to_decimal(None, Decimal("1.00")) == Decimal("1.00")


def test_to_decimal_invalid_returns_fallback():
    assert TransactionRepository._to_decimal("not-a-number") == Decimal("0.00")
