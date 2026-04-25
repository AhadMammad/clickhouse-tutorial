"""Transaction data generator with realistic distributions."""

import logging
from collections.abc import Iterator
from datetime import datetime, timedelta

from faker import Faker

from clickhouse_fundamentals.models.transaction import Transaction

logger = logging.getLogger(__name__)


class TransactionGenerator:
    """Generates realistic transaction data for testing and demos.

    Features:
    - Configurable user and merchant pools
    - Realistic date distributions
    - Weighted status and category distributions
    - Batch generation for efficiency
    """

    def __init__(
        self,
        user_count: int = 10000,
        merchant_count: int = 1000,
        date_range_days: int = 90,
        seed: int | None = None,
    ) -> None:
        """Initialize the generator.

        Args:
            user_count: Number of unique users in the pool.
            merchant_count: Number of unique merchants in the pool.
            date_range_days: Days of historical data to generate.
            seed: Random seed for reproducibility.
        """
        if user_count < 1:
            raise ValueError(f"user_count must be >= 1, got {user_count}")
        if merchant_count < 1:
            raise ValueError(f"merchant_count must be >= 1, got {merchant_count}")
        if date_range_days < 1:
            raise ValueError(f"date_range_days must be >= 1, got {date_range_days}")

        self.user_pool = list(range(1, user_count + 1))
        self.merchant_pool = list(range(1, merchant_count + 1))
        self.date_range_days = date_range_days

        self.faker = Faker()
        if seed is not None:
            if not isinstance(seed, int):
                raise ValueError(f"seed must be an integer, got {type(seed).__name__}")
            Faker.seed(seed)
            import random

            random.seed(seed)

        # Calculate date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=date_range_days)

        logger.info(
            f"TransactionGenerator initialized: "
            f"{user_count} users, {merchant_count} merchants, "
            f"{date_range_days} days of data"
        )

    def generate_batch(self, size: int) -> list[Transaction]:
        """Generate a batch of random transactions.

        Args:
            size: Number of transactions to generate.

        Returns:
            List of Transaction objects.
        """
        if size < 1:
            raise ValueError(f"size must be >= 1, got {size}")

        transactions = []
        for i in range(size):
            try:
                txn = Transaction.random(
                    user_pool=self.user_pool,
                    merchant_pool=self.merchant_pool,
                    date_range=(self.start_date, self.end_date),
                )
                transactions.append(txn)
            except Exception as e:
                logger.warning(f"Failed to generate transaction {i + 1}/{size}: {e}")

        logger.debug(f"Generated batch of {len(transactions)}/{size} transactions")
        return transactions

    def generate_batches(
        self,
        total_rows: int,
        batch_size: int = 10000,
    ) -> Iterator[list[Transaction]]:
        """Generate transactions in batches (generator).

        Yields batches for memory-efficient processing.

        Args:
            total_rows: Total number of transactions to generate.
            batch_size: Size of each batch.

        Yields:
            Lists of Transaction objects.
        """
        remaining = total_rows
        batch_num = 0

        while remaining > 0:
            current_batch_size = min(batch_size, remaining)
            batch = self.generate_batch(current_batch_size)
            batch_num += 1
            remaining -= current_batch_size

            logger.info(
                f"Generated batch {batch_num}: {current_batch_size} transactions "
                f"({total_rows - remaining}/{total_rows})"
            )

            yield batch

    def generate_user_transactions(
        self,
        user_id: int,
        count: int = 50,
    ) -> list[Transaction]:
        """Generate transactions for a specific user.

        Useful for creating consistent test data for a user.

        Args:
            user_id: Target user ID.
            count: Number of transactions to generate.

        Returns:
            List of Transaction objects for the user.
        """
        transactions = []
        for _ in range(count):
            txn = Transaction.random(
                user_pool=[user_id],  # Force specific user
                merchant_pool=self.merchant_pool,
                date_range=(self.start_date, self.end_date),
            )
            transactions.append(txn)

        return transactions

    def generate_merchant_transactions(
        self,
        merchant_id: int,
        count: int = 100,
    ) -> list[Transaction]:
        """Generate transactions for a specific merchant.

        Useful for testing merchant-specific queries.

        Args:
            merchant_id: Target merchant ID.
            count: Number of transactions to generate.

        Returns:
            List of Transaction objects for the merchant.
        """
        transactions = []
        for _ in range(count):
            txn = Transaction.random(
                user_pool=self.user_pool,
                merchant_pool=[merchant_id],  # Force specific merchant
                date_range=(self.start_date, self.end_date),
            )
            transactions.append(txn)

        return transactions

    def estimate_data_size(self, row_count: int) -> dict:
        """Estimate the data size for a given row count.

        Args:
            row_count: Number of rows.

        Returns:
            Dictionary with size estimates.
        """
        if row_count < 0:
            raise ValueError(f"row_count must be >= 0, got {row_count}")
        # Approximate bytes per row (uncompressed)
        # UUID: 16, UInt64: 8x2, Decimal: 16, strings: ~50, timestamps: 8x2
        bytes_per_row = 120

        uncompressed = row_count * bytes_per_row
        # ClickHouse typically achieves 5-10x compression
        compressed_low = uncompressed / 10
        compressed_high = uncompressed / 5

        return {
            "row_count": row_count,
            "estimated_uncompressed_bytes": uncompressed,
            "estimated_uncompressed_mb": round(uncompressed / 1024 / 1024, 2),
            "estimated_compressed_bytes_low": compressed_low,
            "estimated_compressed_bytes_high": compressed_high,
            "estimated_compressed_mb_low": round(compressed_low / 1024 / 1024, 2),
            "estimated_compressed_mb_high": round(compressed_high / 1024 / 1024, 2),
        }
