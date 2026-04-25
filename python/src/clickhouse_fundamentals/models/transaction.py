"""Transaction model matching the ClickHouse schema."""

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import IntEnum
from uuid import UUID, uuid4


class PaymentStatus(IntEnum):
    """Payment status enum matching ClickHouse Enum8 definition.

    Values must match the SQL schema:
    Enum8('pending'=1, 'processing'=2, 'completed'=3,
          'failed'=4, 'refunded'=5, 'cancelled'=6)
    """

    PENDING = 1
    PROCESSING = 2
    COMPLETED = 3
    FAILED = 4
    REFUNDED = 5
    CANCELLED = 6

    @classmethod
    def random_weighted(cls) -> "PaymentStatus":
        """Return a random status with realistic weights.

        Most transactions complete successfully.
        """
        weights = {
            cls.PENDING: 5,
            cls.PROCESSING: 3,
            cls.COMPLETED: 80,
            cls.FAILED: 5,
            cls.REFUNDED: 4,
            cls.CANCELLED: 3,
        }
        statuses = list(weights.keys())
        probs = [weights[s] for s in statuses]
        return random.choices(statuses, weights=probs, k=1)[0]

    def to_clickhouse_name(self) -> str:
        """Return the ClickHouse enum string name."""
        return self.name.lower()


# Payment categories matching realistic merchant types
PAYMENT_CATEGORIES = [
    "retail",
    "groceries",
    "restaurants",
    "travel",
    "entertainment",
    "utilities",
    "healthcare",
    "education",
    "subscriptions",
    "transportation",
]

# Supported currencies
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD", "JPY"]

# Payment methods
PAYMENT_METHODS = ["card", "bank_transfer", "wallet", "crypto"]


@dataclass
class Transaction:
    """Represents a payment transaction.

    This model matches the ClickHouse transactions table schema.
    All fields use appropriate types for efficient serialization.
    """

    transaction_id: UUID = field(default_factory=uuid4)
    user_id: int = 0
    merchant_id: int = 0
    amount: Decimal = Decimal("0.00")
    currency: str = "USD"
    status: PaymentStatus = PaymentStatus.PENDING
    category: str = "retail"
    payment_method: str = "card"
    created_at: datetime = field(default_factory=datetime.now)
    processed_at: datetime | None = None

    def __post_init__(self) -> None:
        """Validate and convert fields after initialization."""
        # Ensure amount is Decimal
        if not isinstance(self.amount, Decimal):
            self.amount = Decimal(str(self.amount))

        # Ensure status is PaymentStatus enum
        if isinstance(self.status, int):
            self.status = PaymentStatus(self.status)
        elif isinstance(self.status, str):
            try:
                self.status = PaymentStatus[self.status.upper()]
            except KeyError:
                valid = [s.name.lower() for s in PaymentStatus]
                raise ValueError(
                    f"Invalid status {self.status!r}. Must be one of: {valid}"
                ) from None

    def to_tuple(self) -> tuple:
        """Convert to tuple for batch insertion.

        Order matches the column order expected by ClickHouse INSERT.
        """
        return (
            self.transaction_id,
            self.user_id,
            self.merchant_id,
            self.amount,
            self.currency,
            self.status.to_clickhouse_name(),
            self.category,
            self.payment_method,
            self.created_at,
            self.processed_at,
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for logging/debugging."""
        return {
            "transaction_id": str(self.transaction_id),
            "user_id": self.user_id,
            "merchant_id": self.merchant_id,
            "amount": str(self.amount),
            "currency": self.currency,
            "status": self.status.to_clickhouse_name(),
            "category": self.category,
            "payment_method": self.payment_method,
            "created_at": self.created_at.isoformat(),
            "processed_at": self.processed_at.isoformat()
            if self.processed_at
            else None,
        }

    @classmethod
    def random(
        cls,
        user_pool: list[int] | None = None,
        merchant_pool: list[int] | None = None,
        date_range: tuple[datetime, datetime] | None = None,
    ) -> "Transaction":
        """Generate a random transaction for testing.

        Args:
            user_pool: Optional list of user IDs to choose from.
            merchant_pool: Optional list of merchant IDs to choose from.
            date_range: Optional (start, end) datetime range.

        Returns:
            A new Transaction with random but realistic data.
        """
        # Default pools if not provided
        if user_pool is None:
            user_pool = list(range(1, 10001))  # 10,000 users
        if merchant_pool is None:
            merchant_pool = list(range(1, 1001))  # 1,000 merchants

        # Random timestamp
        if date_range:
            start, end = date_range
            delta = end - start
            random_offset = random.random() * delta.total_seconds()
            created_at = start + timedelta(seconds=random_offset)
        else:
            # Default: last 90 days
            now = datetime.now()
            days_ago = random.randint(0, 90)
            hours = random.randint(0, 23)
            minutes = random.randint(0, 59)
            created_at = now.replace(
                hour=hours,
                minute=minutes,
                second=random.randint(0, 59),
            ) - timedelta(days=days_ago)

        status = PaymentStatus.random_weighted()

        # Processed timestamp for completed/failed/refunded
        processed_at = None
        if status in (
            PaymentStatus.COMPLETED,
            PaymentStatus.FAILED,
            PaymentStatus.REFUNDED,
        ):
            # Processed 0-60 seconds after creation
            processed_at = created_at + timedelta(seconds=random.randint(1, 60))

        # Generate realistic amount based on category
        category = random.choice(PAYMENT_CATEGORIES)
        amount_ranges = {
            "retail": (5, 500),
            "groceries": (10, 300),
            "restaurants": (15, 150),
            "travel": (50, 2000),
            "entertainment": (10, 200),
            "utilities": (50, 500),
            "healthcare": (20, 1000),
            "education": (100, 5000),
            "subscriptions": (5, 100),
            "transportation": (5, 200),
        }
        min_amount, max_amount = amount_ranges.get(category, (10, 500))
        amount = Decimal(str(round(random.uniform(min_amount, max_amount), 2)))

        return cls(
            transaction_id=uuid4(),
            user_id=random.choice(user_pool),
            merchant_id=random.choice(merchant_pool),
            amount=amount,
            currency=random.choice(CURRENCIES),
            status=status,
            category=category,
            payment_method=random.choice(PAYMENT_METHODS),
            created_at=created_at,
            processed_at=processed_at,
        )

    @staticmethod
    def column_names() -> list[str]:
        """Return column names for INSERT operations."""
        return [
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
        ]
