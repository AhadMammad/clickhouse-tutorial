"""Payment metric model for aggregated analytics."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import TypedDict


class UserSpendingSummary(TypedDict):
    """Typed shape returned by TransactionRepository.get_user_spending_summary."""

    user_id: int
    total_transactions: int
    total_spent: Decimal
    avg_transaction: Decimal
    first_transaction: datetime | None
    last_transaction: datetime | None
    days_active: int
    merchants_used: int
    categories_used: int
    favorite_category: str | None
    favorite_merchant: int


@dataclass
class PaymentMetric:
    """Represents aggregated payment metrics.

    This model is used for analytics results and pre-aggregated data
    from AggregatingMergeTree tables.
    """

    merchant_id: int
    category: str
    currency: str
    metric_date: date
    total_amount: Decimal
    transaction_count: int
    avg_amount: Decimal
    min_amount: Decimal
    max_amount: Decimal
    unique_users: int

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "merchant_id": self.merchant_id,
            "category": self.category,
            "currency": self.currency,
            "metric_date": self.metric_date.isoformat(),
            "total_amount": str(self.total_amount),
            "transaction_count": self.transaction_count,
            "avg_amount": str(self.avg_amount),
            "min_amount": str(self.min_amount),
            "max_amount": str(self.max_amount),
            "unique_users": self.unique_users,
        }


@dataclass
class HourlyRevenue:
    """Hourly revenue metric for time-series analysis."""

    merchant_id: int
    category: str
    hour: datetime
    total_amount: Decimal
    transaction_count: int
    unique_users: int
    avg_amount: Decimal


@dataclass
class UserSpending:
    """User spending summary."""

    user_id: int
    spending_date: date
    currency: str
    total_spent: Decimal
    transaction_count: int


@dataclass
class CategoryStats:
    """Category-level statistics."""

    category: str
    status: str
    stat_date: date
    total_amount: Decimal
    transaction_count: int
    unique_merchants: int
    unique_users: int
    median_amount: Decimal | None = None
    p95_amount: Decimal | None = None


@dataclass
class MerchantSummary:
    """Merchant analytics summary."""

    merchant_id: int
    merchant_name: str | None
    total_revenue: Decimal
    total_transactions: int
    unique_customers: int
    avg_transaction: Decimal
    revenue_rank: int

    def format_currency(self, amount: Decimal) -> str:
        """Format amount as currency string."""
        return f"${amount:,.2f}"


@dataclass
class UserProfile:
    """User profile with spending analytics."""

    user_id: int
    total_transactions: int
    total_spent: Decimal
    avg_transaction: Decimal
    first_transaction: datetime
    last_transaction: datetime
    favorite_category: str
    favorite_merchant_id: int
    days_active: int

    @property
    def daily_average(self) -> Decimal:
        """Calculate daily spending average."""
        if self.days_active == 0:
            return Decimal("0.00")
        return self.total_spent / self.days_active


@dataclass
class DailyRevenue:
    """Daily revenue for trend analysis."""

    txn_date: date
    total_transactions: int
    total_revenue: Decimal
    avg_transaction: Decimal
    unique_customers: int
    unique_merchants: int

    def to_row(self) -> list:
        """Convert to row for tabular display."""
        return [
            self.txn_date.isoformat(),
            self.total_transactions,
            f"${self.total_revenue:,.2f}",
            f"${self.avg_transaction:,.2f}",
            self.unique_customers,
            self.unique_merchants,
        ]
