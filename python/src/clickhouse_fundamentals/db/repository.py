"""Repository pattern for transaction data access."""

import logging
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import pandas as pd

from clickhouse_fundamentals.db.client import ClickHouseClient
from clickhouse_fundamentals.models.payment_metric import UserSpendingSummary
from clickhouse_fundamentals.models.transaction import PaymentStatus, Transaction

logger = logging.getLogger(__name__)


class TransactionRepository:
    """Repository for transaction data access.

    Provides a clean interface for CRUD operations and analytics queries
    on the transactions table. Uses dependency injection for the client.
    """

    def __init__(self, client: ClickHouseClient) -> None:
        """Initialize the repository.

        Args:
            client: ClickHouse client instance.
        """
        self.client = client

    @staticmethod
    def _to_decimal(value: object, fallback: Decimal = Decimal("0.00")) -> Decimal:
        """Safely convert a DB value to Decimal, returning fallback on None or error."""
        if value is None:
            return fallback
        try:
            return Decimal(str(value))
        except InvalidOperation:
            logger.warning(f"Could not convert {value!r} to Decimal, using {fallback}")
            return fallback

    def insert_batch(self, transactions: list[Transaction]) -> int:
        """Insert a batch of transactions.

        Args:
            transactions: List of Transaction objects to insert.

        Returns:
            Number of rows inserted.
        """
        if not transactions:
            return 0

        data = [txn.to_tuple() for txn in transactions]
        column_names = Transaction.column_names()

        logger.debug(f"Inserting batch of {len(transactions)} transactions")
        start = time.monotonic()
        try:
            result = self.client.insert(
                table="transactions",
                data=data,
                column_names=column_names,
            )
            logger.debug(
                f"insert_batch completed in {time.monotonic() - start:.3f}s "
                f"({len(transactions)} rows)"
            )
            return result
        except Exception as e:
            logger.error(
                f"insert_batch failed after {time.monotonic() - start:.3f}s "
                f"({len(transactions)} rows): {e}"
            )
            raise

    def get_by_user(
        self,
        user_id: int,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Transaction]:
        """Get transactions for a specific user.

        Args:
            user_id: User ID to filter by.
            limit: Maximum number of results.
            offset: Number of rows to skip.

        Returns:
            List of Transaction objects.
        """
        if user_id < 1:
            raise ValueError(f"user_id must be >= 1, got {user_id}")
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        if offset < 0:
            raise ValueError(f"offset must be >= 0, got {offset}")

        query = """
            SELECT
                transaction_id,
                user_id,
                merchant_id,
                amount,
                currency,
                status,
                category,
                payment_method,
                created_at,
                processed_at
            FROM transactions
            WHERE user_id = {user_id:UInt64}
            ORDER BY created_at DESC
            LIMIT {limit:UInt32}
            OFFSET {offset:UInt32}
        """

        start = time.monotonic()
        try:
            rows = self.client.query(
                query,
                parameters={"user_id": user_id, "limit": limit, "offset": offset},
            )
            logger.debug(
                f"get_by_user(user_id={user_id}) returned {len(rows)} rows "
                f"in {time.monotonic() - start:.3f}s"
            )
        except Exception as e:
            logger.error(
                f"get_by_user(user_id={user_id}) failed after "
                f"{time.monotonic() - start:.3f}s: {e}"
            )
            raise

        return [
            Transaction(
                transaction_id=row[0],
                user_id=row[1],
                merchant_id=row[2],
                amount=self._to_decimal(row[3]),
                currency=row[4],
                status=PaymentStatus[row[5].upper()],
                category=row[6],
                payment_method=row[7],
                created_at=row[8],
                processed_at=row[9],
            )
            for row in rows
        ]

    def get_revenue_by_merchant(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 20,
    ) -> pd.DataFrame:
        """Get revenue breakdown by merchant.

        Args:
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).
            limit: Maximum number of merchants to return.

        Returns:
            DataFrame with merchant revenue statistics.
        """
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        if start_date is None:
            start_date = datetime.now() - timedelta(days=30)
        if end_date is None:
            end_date = datetime.now()

        query = """
            SELECT
                merchant_id,
                count() AS transaction_count,
                sum(amount) AS total_revenue,
                avg(amount) AS avg_transaction,
                min(amount) AS min_transaction,
                max(amount) AS max_transaction,
                uniq(user_id) AS unique_customers
            FROM transactions
            WHERE created_at >= {start:DateTime}
              AND created_at <= {end:DateTime}
              AND status = 'completed'
            GROUP BY merchant_id
            ORDER BY total_revenue DESC
            LIMIT {limit:UInt32}
        """

        start = time.monotonic()
        try:
            result = self.client.query_df(
                query,
                parameters={"start": start_date, "end": end_date, "limit": limit},
            )
            logger.debug(
                f"get_revenue_by_merchant(limit={limit}) returned {len(result)} rows "
                f"in {time.monotonic() - start:.3f}s"
            )
            return result
        except Exception as e:
            logger.error(
                f"get_revenue_by_merchant(limit={limit}) failed after "
                f"{time.monotonic() - start:.3f}s: {e}"
            )
            raise

    def get_hourly_stats(
        self,
        days: int = 7,
    ) -> pd.DataFrame:
        """Get hourly transaction statistics.

        Args:
            days: Number of days to look back.

        Returns:
            DataFrame with hourly statistics.
        """
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")

        query = """
            SELECT
                toStartOfHour(created_at) AS hour,
                count() AS transactions,
                sum(amount) AS revenue,
                avg(amount) AS avg_amount,
                uniq(user_id) AS unique_users,
                uniq(merchant_id) AS unique_merchants
            FROM transactions
            WHERE created_at >= now() - INTERVAL {days:UInt32} DAY
            GROUP BY hour
            ORDER BY hour DESC
        """

        start = time.monotonic()
        try:
            result = self.client.query_df(query, parameters={"days": days})
            logger.debug(
                f"get_hourly_stats(days={days}) returned {len(result)} rows "
                f"in {time.monotonic() - start:.3f}s"
            )
            return result
        except Exception as e:
            logger.error(
                f"get_hourly_stats(days={days}) failed after "
                f"{time.monotonic() - start:.3f}s: {e}"
            )
            raise

    def get_user_spending_summary(self, user_id: int) -> UserSpendingSummary:
        """Get spending summary for a user.

        Args:
            user_id: User ID to analyze.

        Returns:
            UserSpendingSummary with spending statistics.
        """
        if user_id < 1:
            raise ValueError(f"user_id must be >= 1, got {user_id}")

        query = """
            SELECT
                count() AS total_transactions,
                sum(amount) AS total_spent,
                avg(amount) AS avg_transaction,
                min(created_at) AS first_transaction,
                max(created_at) AS last_transaction,
                uniq(merchant_id) AS merchants_used,
                uniq(category) AS categories_used,
                topK(1)(category)[1] AS favorite_category,
                topK(1)(merchant_id)[1] AS favorite_merchant
            FROM transactions
            WHERE user_id = {user_id:UInt64}
        """

        start = time.monotonic()
        try:
            rows = self.client.query(query, parameters={"user_id": user_id})
            logger.debug(
                f"get_user_spending_summary(user_id={user_id}) completed in "
                f"{time.monotonic() - start:.3f}s"
            )
        except Exception as e:
            logger.error(
                f"get_user_spending_summary(user_id={user_id}) failed after "
                f"{time.monotonic() - start:.3f}s: {e}"
            )
            raise

        if not rows or rows[0][0] == 0:
            return UserSpendingSummary(
                user_id=user_id,
                total_transactions=0,
                total_spent=Decimal("0.00"),
                avg_transaction=Decimal("0.00"),
                first_transaction=None,
                last_transaction=None,
                days_active=0,
                merchants_used=0,
                categories_used=0,
                favorite_category=None,
                favorite_merchant=0,
            )

        row = rows[0]
        first_txn = row[3]
        last_txn = row[4]
        days_active = (last_txn - first_txn).days if first_txn and last_txn else 0

        return UserSpendingSummary(
            user_id=user_id,
            total_transactions=row[0],
            total_spent=self._to_decimal(row[1]),
            avg_transaction=self._to_decimal(row[2]),
            first_transaction=first_txn,
            last_transaction=last_txn,
            days_active=days_active,
            merchants_used=row[5],
            categories_used=row[6],
            favorite_category=row[7] if row[7] else None,
            favorite_merchant=row[8] if row[8] else 0,
        )

    def get_daily_revenue(self, days: int = 30) -> pd.DataFrame:
        """Get daily revenue trends.

        Args:
            days: Number of days to look back.

        Returns:
            DataFrame with daily revenue data.
        """
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")

        query = """
            SELECT
                toDate(created_at) AS txn_date,
                count() AS total_transactions,
                sum(amount) AS total_revenue,
                avg(amount) AS avg_transaction,
                uniq(user_id) AS unique_customers,
                uniq(merchant_id) AS unique_merchants
            FROM transactions
            WHERE created_at >= today() - {days:UInt32}
            GROUP BY txn_date
            ORDER BY txn_date DESC
        """

        start = time.monotonic()
        try:
            result = self.client.query_df(query, parameters={"days": days})
            logger.debug(
                f"get_daily_revenue(days={days}) returned {len(result)} rows "
                f"in {time.monotonic() - start:.3f}s"
            )
            return result
        except Exception as e:
            logger.error(
                f"get_daily_revenue(days={days}) failed after "
                f"{time.monotonic() - start:.3f}s: {e}"
            )
            raise

    def get_category_breakdown(self, days: int = 30) -> pd.DataFrame:
        """Get transaction breakdown by category.

        Args:
            days: Number of days to look back.

        Returns:
            DataFrame with category statistics.
        """
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")

        query = """
            SELECT
                category,
                count() AS transactions,
                sum(amount) AS revenue,
                avg(amount) AS avg_transaction,
                uniq(user_id) AS unique_users,
                uniq(merchant_id) AS unique_merchants,
                round(count() * 100.0 / sum(count()) OVER (), 2) AS pct_transactions,
                round(sum(amount) * 100.0 / sum(sum(amount)) OVER (), 2) AS pct_revenue
            FROM transactions
            WHERE created_at >= today() - {days:UInt32}
            GROUP BY category
            ORDER BY revenue DESC
        """

        start = time.monotonic()
        try:
            result = self.client.query_df(query, parameters={"days": days})
            logger.debug(
                f"get_category_breakdown(days={days}) returned {len(result)} rows "
                f"in {time.monotonic() - start:.3f}s"
            )
            return result
        except Exception as e:
            logger.error(
                f"get_category_breakdown(days={days}) failed after "
                f"{time.monotonic() - start:.3f}s: {e}"
            )
            raise

    def get_status_distribution(self, days: int = 7) -> pd.DataFrame:
        """Get transaction status distribution.

        Args:
            days: Number of days to look back.

        Returns:
            DataFrame with status counts.
        """
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")

        query = """
            SELECT
                status,
                count() AS transactions,
                sum(amount) AS total_amount,
                round(count() * 100.0 / sum(count()) OVER (), 2) AS pct_of_total
            FROM transactions
            WHERE created_at >= today() - {days:UInt32}
            GROUP BY status
            ORDER BY transactions DESC
        """

        start = time.monotonic()
        try:
            result = self.client.query_df(query, parameters={"days": days})
            logger.debug(
                f"get_status_distribution(days={days}) returned {len(result)} rows "
                f"in {time.monotonic() - start:.3f}s"
            )
            return result
        except Exception as e:
            logger.error(
                f"get_status_distribution(days={days}) failed after "
                f"{time.monotonic() - start:.3f}s: {e}"
            )
            raise

    def get_top_merchants(self, limit: int = 10, days: int = 30) -> pd.DataFrame:
        """Get top merchants by revenue.

        Args:
            limit: Number of merchants to return.
            days: Number of days to look back.

        Returns:
            DataFrame with top merchant data.
        """
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")

        query = """
            SELECT
                merchant_id,
                count() AS transaction_count,
                sum(amount) AS total_revenue,
                avg(amount) AS avg_transaction,
                uniq(user_id) AS unique_customers,
                row_number() OVER (ORDER BY sum(amount) DESC) AS revenue_rank
            FROM transactions
            WHERE created_at >= today() - {days:UInt32}
              AND status = 'completed'
            GROUP BY merchant_id
            ORDER BY total_revenue DESC
            LIMIT {limit:UInt32}
        """

        start = time.monotonic()
        try:
            result = self.client.query_df(
                query,
                parameters={"limit": limit, "days": days},
            )
            logger.debug(
                f"get_top_merchants(limit={limit}, days={days}) returned "
                f"{len(result)} rows in {time.monotonic() - start:.3f}s"
            )
            return result
        except Exception as e:
            logger.error(
                f"get_top_merchants(limit={limit}, days={days}) failed after "
                f"{time.monotonic() - start:.3f}s: {e}"
            )
            raise

    def get_total_stats(self) -> dict:
        """Get overall statistics for the transactions table.

        Returns:
            Dictionary with total statistics.
        """
        query = """
            SELECT
                count() AS total_transactions,
                sum(amount) AS total_volume,
                uniq(user_id) AS total_users,
                uniq(merchant_id) AS total_merchants,
                min(created_at) AS earliest_transaction,
                max(created_at) AS latest_transaction
            FROM transactions
        """

        start = time.monotonic()
        try:
            rows = self.client.query(query)
            logger.debug(
                f"get_total_stats() completed in {time.monotonic() - start:.3f}s"
            )
        except Exception as e:
            logger.error(
                f"get_total_stats() failed after {time.monotonic() - start:.3f}s: {e}"
            )
            raise

        if not rows:
            return {
                "total_transactions": 0,
                "total_volume": Decimal("0.00"),
                "total_users": 0,
                "total_merchants": 0,
                "earliest_transaction": None,
                "latest_transaction": None,
            }

        row = rows[0]
        return {
            "total_transactions": row[0],
            "total_volume": self._to_decimal(row[1]),
            "total_users": row[2],
            "total_merchants": row[3],
            "earliest_transaction": row[4],
            "latest_transaction": row[5],
        }
