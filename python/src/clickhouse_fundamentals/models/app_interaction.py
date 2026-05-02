"""Data models for mobile app interaction pipeline (PostgreSQL 3NF schema)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass
class UserTier:
    """Lookup table for user subscription tiers (prevents 3NF violation on users.tier)."""

    tier_id: int
    tier_name: str           # free | standard | premium
    max_monthly_events: int  # -1 = unlimited
    description: str


@dataclass
class AppUser:
    """Registered app user.

    country_code is intentionally absent — it belongs to sessions (session context),
    as a user may access the app from different countries.
    """

    user_id: int
    external_user_id: str  # UUID string used by external systems
    username: str
    email: str
    tier_id: int           # FK to user_tiers
    registered_at: datetime = field(default_factory=datetime.now)


@dataclass
class Device:
    """Physical device that runs the app."""

    device_id: int
    device_fingerprint: str  # deterministic hash of device attributes
    device_type: str         # mobile | tablet
    os_name: str             # iOS | Android
    os_version: str
    device_model: str
    screen_resolution: str
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class AppVersion:
    """Published app version for a platform."""

    version_id: int
    version_code: str    # e.g., "2.5.0-ios"
    platform: str        # ios | android
    release_date: date
    is_force_update: bool = False


@dataclass
class Screen:
    """Named screen/page within the app."""

    screen_id: int
    screen_name: str      # e.g., "product_detail"
    screen_category: str  # navigation | commerce | account | support | auth
    description: str = ""


@dataclass
class EventType:
    """Catalogue of all possible event types."""

    event_type_id: int
    event_name: str       # e.g., "add_to_cart"
    event_category: str   # navigation | interaction | system | commerce | account
    description: str = ""


@dataclass
class Session:
    """A single continuous app session (open → background/close).

    duration_seconds is NOT stored — always calculated as
    EXTRACT(EPOCH FROM session_end - session_start) in queries.
    """

    session_id: str          # UUID string (gen_random_uuid() in PostgreSQL)
    user_id: int
    device_id: int
    version_id: int
    session_start: datetime
    session_end: datetime | None = None
    ip_address: str | None = None
    country_code: str = ""   # session location, not user home country


@dataclass
class Event:
    """Single interaction event within a session.

    event_id = 0 means not yet inserted (assigned by PostgreSQL BIGSERIAL).
    properties stores semi-structured event-specific data as JSONB.
    """

    event_id: int
    session_id: str
    event_type_id: int
    event_timestamp: datetime
    sequence_number: int
    duration_ms: int
    screen_id: int | None = None
    properties: dict = field(default_factory=dict)


@dataclass
class UserDevice:
    """Associative table tracking which devices a user has used."""

    user_id: int
    device_id: int
    first_seen: datetime
    last_seen: datetime
