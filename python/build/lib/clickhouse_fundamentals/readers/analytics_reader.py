"""Analytics reader for formatted report output."""

import logging
from decimal import Decimal

from tabulate import tabulate

from clickhouse_fundamentals.db.repository import TransactionRepository

logger = logging.getLogger(__name__)


class AnalyticsReader:
    """Reads and formats analytics data for display.

    Provides human-readable reports from the transaction repository.
    Uses tabulate for consistent table formatting.
    """

    def __init__(self, repository: TransactionRepository) -> None:
        """Initialize the reader.

        Args:
            repository: Transaction repository instance.
        """
        self.repository = repository

    def _format_currency(self, amount: Decimal | float) -> str:
        """Format amount as currency."""
        return f"${float(amount):,.2f}"

    def _format_number(self, num: int) -> str:
        """Format number with thousands separator."""
        return f"{num:,}"

    def print_revenue_report(self, days: int = 7) -> None:
        """Print daily revenue report.

        Args:
            days: Number of days to include in report.
        """
        logger.info(f"Generating daily revenue report (last {days} days)")
        print("\n" + "=" * 70)
        print(f"DAILY REVENUE REPORT (Last {days} Days)")
        print("=" * 70)

        try:
            df = self.repository.get_daily_revenue(days=days)
        except Exception as e:
            logger.error(f"Failed to fetch daily revenue data: {e}")
            print("Error: Could not retrieve revenue data.")
            return

        if df.empty:
            logger.warning(f"No daily revenue data available for last {days} days")
            print("No data available for the specified period.")
            return

        # Format the dataframe for display
        table_data = []
        for _, row in df.iterrows():
            try:
                table_data.append(
                    [
                        row["txn_date"].strftime("%Y-%m-%d")
                        if hasattr(row["txn_date"], "strftime")
                        else row["txn_date"],
                        self._format_number(int(row["total_transactions"])),
                        self._format_currency(row["total_revenue"]),
                        self._format_currency(row["avg_transaction"]),
                        self._format_number(int(row["unique_customers"])),
                        self._format_number(int(row["unique_merchants"])),
                    ]
                )
            except Exception as e:
                logger.warning(f"Skipping malformed revenue row: {e}")

        headers = [
            "Date",
            "Transactions",
            "Revenue",
            "Avg Txn",
            "Customers",
            "Merchants",
        ]
        print(tabulate(table_data, headers=headers, tablefmt="simple"))

        # Summary
        total_revenue = df["total_revenue"].sum()
        total_txns = df["total_transactions"].sum()
        print(f"\nTotal Revenue: {self._format_currency(total_revenue)}")
        print(f"Total Transactions: {self._format_number(int(total_txns))}")
        logger.info(f"Daily revenue report complete ({len(df)} days)")

    def print_user_profile(self, user_id: int) -> None:
        """Print user spending profile.

        Args:
            user_id: User ID to analyze.
        """
        logger.info(f"Generating user profile for user_id={user_id}")
        print("\n" + "=" * 70)
        print(f"USER PROFILE: User #{user_id}")
        print("=" * 70)

        try:
            profile = self.repository.get_user_spending_summary(user_id)
        except Exception as e:
            logger.error(f"Failed to fetch profile for user_id={user_id}: {e}")
            print(f"Error: Could not retrieve profile for user {user_id}.")
            return

        if profile["total_transactions"] == 0:
            logger.warning(f"No transactions found for user_id={user_id}")
            print(f"No transactions found for user {user_id}")
            return

        print(
            f"\nTotal Transactions:  {self._format_number(profile['total_transactions'])}"
        )
        print(f"Total Spent:         {self._format_currency(profile['total_spent'])}")
        print(
            f"Average Transaction: {self._format_currency(profile['avg_transaction'])}"
        )
        print(f"Days Active:         {profile['days_active']}")
        print(f"Merchants Used:      {profile['merchants_used']}")
        print(f"Categories Used:     {profile['categories_used']}")
        print(f"Favorite Category:   {profile['favorite_category'] or 'N/A'}")
        print(f"Favorite Merchant:   #{profile['favorite_merchant']}")

        first_txn = profile.get("first_transaction")
        last_txn = profile.get("last_transaction")
        if first_txn and hasattr(first_txn, "strftime"):
            print(
                f"\nFirst Transaction:   {first_txn.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        if last_txn and hasattr(last_txn, "strftime"):
            print(
                f"Last Transaction:    {last_txn.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        logger.info(f"User profile complete for user_id={user_id}")

    def print_top_merchants(self, limit: int = 10, days: int = 30) -> None:
        """Print top merchants by revenue.

        Args:
            limit: Number of merchants to show.
            days: Number of days to analyze.
        """
        logger.info(f"Generating top {limit} merchants report (last {days} days)")
        print("\n" + "=" * 70)
        print(f"TOP {limit} MERCHANTS BY REVENUE (Last {days} Days)")
        print("=" * 70)

        try:
            df = self.repository.get_top_merchants(limit=limit, days=days)
        except Exception as e:
            logger.error(f"Failed to fetch top merchants data: {e}")
            print("Error: Could not retrieve merchant data.")
            return

        if df.empty:
            logger.warning(f"No merchant data available for last {days} days")
            print("No merchant data available.")
            return

        table_data = []
        for _, row in df.iterrows():
            try:
                table_data.append(
                    [
                        int(row["revenue_rank"]),
                        f"#{int(row['merchant_id'])}",
                        self._format_number(int(row["transaction_count"])),
                        self._format_currency(row["total_revenue"]),
                        self._format_currency(row["avg_transaction"]),
                        self._format_number(int(row["unique_customers"])),
                    ]
                )
            except Exception as e:
                logger.warning(f"Skipping malformed merchant row: {e}")

        headers = [
            "Rank",
            "Merchant",
            "Transactions",
            "Revenue",
            "Avg Txn",
            "Customers",
        ]
        print(tabulate(table_data, headers=headers, tablefmt="simple"))
        logger.info(f"Top merchants report complete ({len(df)} merchants)")

    def print_category_breakdown(self, days: int = 30) -> None:
        """Print transaction breakdown by category.

        Args:
            days: Number of days to analyze.
        """
        logger.info(f"Generating category breakdown report (last {days} days)")
        print("\n" + "=" * 70)
        print(f"CATEGORY BREAKDOWN (Last {days} Days)")
        print("=" * 70)

        try:
            df = self.repository.get_category_breakdown(days=days)
        except Exception as e:
            logger.error(f"Failed to fetch category breakdown data: {e}")
            print("Error: Could not retrieve category data.")
            return

        if df.empty:
            logger.warning(f"No category data available for last {days} days")
            print("No category data available.")
            return

        table_data = []
        for _, row in df.iterrows():
            try:
                table_data.append(
                    [
                        row["category"],
                        self._format_number(int(row["transactions"])),
                        f"{row['pct_transactions']:.1f}%",
                        self._format_currency(row["revenue"]),
                        f"{row['pct_revenue']:.1f}%",
                        self._format_currency(row["avg_transaction"]),
                    ]
                )
            except Exception as e:
                logger.warning(f"Skipping malformed category row: {e}")

        headers = ["Category", "Transactions", "% Txns", "Revenue", "% Rev", "Avg Txn"]
        print(tabulate(table_data, headers=headers, tablefmt="simple"))
        logger.info(f"Category breakdown report complete ({len(df)} categories)")

    def print_status_distribution(self, days: int = 7) -> None:
        """Print transaction status distribution.

        Args:
            days: Number of days to analyze.
        """
        logger.info(f"Generating status distribution report (last {days} days)")
        print("\n" + "=" * 70)
        print(f"STATUS DISTRIBUTION (Last {days} Days)")
        print("=" * 70)

        try:
            df = self.repository.get_status_distribution(days=days)
        except Exception as e:
            logger.error(f"Failed to fetch status distribution data: {e}")
            print("Error: Could not retrieve status data.")
            return

        if df.empty:
            logger.warning(f"No status data available for last {days} days")
            print("No status data available.")
            return

        table_data = []
        for _, row in df.iterrows():
            try:
                table_data.append(
                    [
                        str(row["status"]).upper(),
                        self._format_number(int(row["transactions"])),
                        f"{row['pct_of_total']:.1f}%",
                        self._format_currency(row["total_amount"]),
                    ]
                )
            except Exception as e:
                logger.warning(f"Skipping malformed status row: {e}")

        headers = ["Status", "Transactions", "% of Total", "Total Amount"]
        print(tabulate(table_data, headers=headers, tablefmt="simple"))
        logger.info(f"Status distribution report complete ({len(df)} statuses)")

    def print_hourly_stats(self, days: int = 7) -> None:
        """Print hourly transaction statistics.

        Args:
            days: Number of days to analyze.
        """
        logger.info(f"Generating hourly stats report (last {days} days)")
        print("\n" + "=" * 70)
        print(f"HOURLY STATISTICS (Last {days} Days)")
        print("=" * 70)

        try:
            df = self.repository.get_hourly_stats(days=days)
        except Exception as e:
            logger.error(f"Failed to fetch hourly stats data: {e}")
            print("Error: Could not retrieve hourly data.")
            return

        if df.empty:
            logger.warning(f"No hourly data available for last {days} days")
            print("No hourly data available.")
            return

        # Show last 24 hours
        df_recent = df.head(24)

        table_data = []
        for _, row in df_recent.iterrows():
            try:
                hour_str = (
                    row["hour"].strftime("%Y-%m-%d %H:00")
                    if hasattr(row["hour"], "strftime")
                    else str(row["hour"])
                )
                table_data.append(
                    [
                        hour_str,
                        self._format_number(int(row["transactions"])),
                        self._format_currency(row["revenue"]),
                        self._format_currency(row["avg_amount"]),
                        self._format_number(int(row["unique_users"])),
                    ]
                )
            except Exception as e:
                logger.warning(f"Skipping malformed hourly row: {e}")

        headers = ["Hour", "Transactions", "Revenue", "Avg Txn", "Users"]
        print(tabulate(table_data, headers=headers, tablefmt="simple"))
        logger.info(f"Hourly stats report complete ({len(df)} hours)")

    def print_summary(self) -> None:
        """Print overall database summary."""
        logger.info("Generating database summary")
        print("\n" + "=" * 70)
        print("DATABASE SUMMARY")
        print("=" * 70)

        try:
            stats = self.repository.get_total_stats()
        except Exception as e:
            logger.error(f"Failed to fetch database summary: {e}")
            print("Error: Could not retrieve database summary.")
            return

        print(
            f"\nTotal Transactions:  {self._format_number(stats['total_transactions'])}"
        )
        print(f"Total Volume:        {self._format_currency(stats['total_volume'])}")
        print(f"Total Users:         {self._format_number(stats['total_users'])}")
        print(f"Total Merchants:     {self._format_number(stats['total_merchants'])}")

        earliest = stats.get("earliest_transaction")
        latest = stats.get("latest_transaction")
        if earliest and hasattr(earliest, "strftime"):
            print("\nData Range:")
            print(f"  From: {earliest.strftime('%Y-%m-%d %H:%M:%S')}")
        if latest and hasattr(latest, "strftime"):
            print(f"  To:   {latest.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(
            f"Database summary complete: {stats['total_transactions']} transactions"
        )

    def print_full_report(self, days: int = 30) -> None:
        """Print a comprehensive analytics report.

        Args:
            days: Number of days to analyze.
        """
        self.print_summary()
        self.print_revenue_report(days=min(days, 14))
        self.print_top_merchants(limit=10, days=days)
        self.print_category_breakdown(days=days)
        self.print_status_distribution(days=min(days, 7))
