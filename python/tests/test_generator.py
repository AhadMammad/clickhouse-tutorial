"""Tests for TransactionGenerator."""

import pytest

from clickhouse_fundamentals.generators.transaction_generator import (
    TransactionGenerator,
)


def test_invalid_user_count_raises():
    with pytest.raises(ValueError, match="user_count"):
        TransactionGenerator(user_count=0)


def test_invalid_merchant_count_raises():
    with pytest.raises(ValueError, match="merchant_count"):
        TransactionGenerator(merchant_count=0)


def test_invalid_date_range_raises():
    with pytest.raises(ValueError, match="date_range_days"):
        TransactionGenerator(date_range_days=0)


def test_generate_batch_invalid_size_raises():
    gen = TransactionGenerator(user_count=5, merchant_count=5, date_range_days=7)
    with pytest.raises(ValueError, match="size"):
        gen.generate_batch(0)


def test_generate_batch_returns_correct_count():
    gen = TransactionGenerator(
        user_count=10, merchant_count=5, date_range_days=7, seed=42
    )
    batch = gen.generate_batch(50)
    assert len(batch) == 50


def test_generate_batches_total_matches():
    gen = TransactionGenerator(
        user_count=10, merchant_count=5, date_range_days=7, seed=0
    )
    total = sum(len(b) for b in gen.generate_batches(250, batch_size=100))
    assert total == 250


def test_seeded_generator_is_reproducible():
    # Generate from gen1 first, then construct gen2 (which re-seeds random),
    # so both batches start from the same random state.
    gen1 = TransactionGenerator(seed=99)
    ids1 = [t.user_id for t in gen1.generate_batch(10)]

    gen2 = TransactionGenerator(seed=99)
    ids2 = [t.user_id for t in gen2.generate_batch(10)]

    assert ids1 == ids2


def test_estimate_data_size_zero_rows():
    gen = TransactionGenerator()
    result = gen.estimate_data_size(0)
    assert result["estimated_uncompressed_bytes"] == 0
    assert result["estimated_compressed_mb_low"] == 0


def test_estimate_data_size_expected_keys():
    gen = TransactionGenerator()
    result = gen.estimate_data_size(1000)
    assert result["row_count"] == 1000
    assert "estimated_uncompressed_mb" in result
    assert "estimated_compressed_mb_low" in result
    assert "estimated_compressed_mb_high" in result
    assert (
        result["estimated_compressed_mb_low"] < result["estimated_compressed_mb_high"]
    )


def test_generate_user_transactions_uses_correct_user():
    gen = TransactionGenerator(
        user_count=100, merchant_count=10, date_range_days=7, seed=1
    )
    txns = gen.generate_user_transactions(user_id=42, count=20)
    assert all(t.user_id == 42 for t in txns)


def test_generate_merchant_transactions_uses_correct_merchant():
    gen = TransactionGenerator(
        user_count=100, merchant_count=10, date_range_days=7, seed=1
    )
    txns = gen.generate_merchant_transactions(merchant_id=7, count=20)
    assert all(t.merchant_id == 7 for t in txns)
