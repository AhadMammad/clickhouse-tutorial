"""Tests for the Transaction model."""

from decimal import Decimal
from uuid import UUID

import pytest

from clickhouse_fundamentals.models.transaction import (
    CURRENCIES,
    PAYMENT_CATEGORIES,
    PAYMENT_METHODS,
    PaymentStatus,
    Transaction,
)


def test_default_transaction_fields():
    txn = Transaction()
    assert txn.user_id == 0
    assert txn.merchant_id == 0
    assert txn.amount == Decimal("0.00")
    assert txn.currency == "USD"
    assert txn.status == PaymentStatus.PENDING
    assert isinstance(txn.transaction_id, UUID)


def test_column_names_length_matches_tuple():
    txn = Transaction(user_id=1, merchant_id=2)
    assert len(txn.to_tuple()) == len(Transaction.column_names())


def test_to_tuple_field_positions():
    txn = Transaction(user_id=42, merchant_id=7, amount=Decimal("9.99"))
    tup = txn.to_tuple()
    assert tup[1] == 42
    assert tup[2] == 7
    assert tup[3] == Decimal("9.99")
    assert tup[5] == "pending"  # status as ClickHouse name


def test_to_dict_has_all_keys():
    txn = Transaction(user_id=1)
    keys = set(txn.to_dict().keys())
    assert keys == {
        "transaction_id",
        "user_id",
        "merchant_id",
        "amount",
        "currency",
        "status",
        "category",
        "payment_method",
        "created_at",
        "processed_at",
    }


def test_status_coercion_from_string():
    txn = Transaction(status="completed")
    assert txn.status == PaymentStatus.COMPLETED


def test_status_coercion_from_int():
    txn = Transaction(status=3)
    assert txn.status == PaymentStatus.COMPLETED


def test_invalid_status_raises():
    with pytest.raises(ValueError, match="Invalid status"):
        Transaction(status="not_a_status")


def test_amount_coerced_to_decimal():
    txn = Transaction(amount=10.5)
    assert isinstance(txn.amount, Decimal)


def test_random_transaction_valid():
    txn = Transaction.random()
    assert txn.user_id >= 1
    assert txn.merchant_id >= 1
    assert txn.amount > 0
    assert txn.currency in CURRENCIES
    assert txn.category in PAYMENT_CATEGORIES
    assert txn.payment_method in PAYMENT_METHODS
    assert isinstance(txn.status, PaymentStatus)


def test_random_transaction_with_fixed_pools():
    txn = Transaction.random(user_pool=[5], merchant_pool=[99])
    assert txn.user_id == 5
    assert txn.merchant_id == 99


def test_payment_status_random_weighted_returns_valid():
    for _ in range(30):
        status = PaymentStatus.random_weighted()
        assert isinstance(status, PaymentStatus)


def test_processed_at_set_for_terminal_statuses():
    for _ in range(50):
        txn = Transaction.random()
        if txn.status in (
            PaymentStatus.COMPLETED,
            PaymentStatus.FAILED,
            PaymentStatus.REFUNDED,
        ):
            assert txn.processed_at is not None
        elif txn.status in (
            PaymentStatus.PENDING,
            PaymentStatus.PROCESSING,
            PaymentStatus.CANCELLED,
        ):
            assert txn.processed_at is None
