"""Generator for realistic mobile app interaction data."""

from __future__ import annotations

import hashlib
import random
import uuid
from datetime import date, datetime, timedelta
from typing import Generator

from faker import Faker

from clickhouse_fundamentals.models.app_interaction import (
    AppUser,
    AppVersion,
    Device,
    Event,
    EventType,
    Screen,
    Session,
    UserDevice,
    UserTier,
)

_SCREENS: list[tuple[str, str, str]] = [
    # (name, category, description)
    ("home", "navigation", "Main home feed"),
    ("search", "navigation", "Search screen"),
    ("explore", "navigation", "Browse categories"),
    ("notifications", "navigation", "Push notification inbox"),
    ("product_list", "commerce", "Category product listing"),
    ("product_detail", "commerce", "Single product view"),
    ("cart", "commerce", "Shopping cart"),
    ("checkout", "commerce", "Checkout flow"),
    ("order_confirmation", "commerce", "Order placed confirmation"),
    ("order_history", "commerce", "Past orders"),
    ("profile", "account", "User profile page"),
    ("settings", "account", "App settings"),
    ("payment_methods", "account", "Saved payment methods"),
    ("addresses", "account", "Saved delivery addresses"),
    ("help", "support", "Help centre"),
    ("faq", "support", "FAQ"),
    ("live_chat", "support", "Live support chat"),
    ("login", "auth", "Login screen"),
    ("register", "auth", "Registration screen"),
    ("forgot_password", "auth", "Password reset"),
]

_EVENT_TYPES: list[tuple[str, str, str]] = [
    # (name, category, description)
    ("screen_view", "navigation", "User viewed a screen"),
    ("back_press", "navigation", "User pressed back"),
    ("tab_switch", "navigation", "User switched bottom tab"),
    ("deep_link_open", "navigation", "App opened via deep link"),
    ("button_click", "interaction", "Generic button tap"),
    ("swipe", "interaction", "Swipe gesture"),
    ("scroll_end", "interaction", "Reached end of list"),
    ("search_query", "interaction", "Search term submitted"),
    ("filter_apply", "interaction", "Search/list filter applied"),
    ("sort_change", "interaction", "Sort order changed"),
    ("product_view", "commerce", "Product detail viewed"),
    ("add_to_cart", "commerce", "Item added to cart"),
    ("remove_from_cart", "commerce", "Item removed from cart"),
    ("wishlist_add", "commerce", "Item added to wishlist"),
    ("checkout_start", "commerce", "Checkout flow started"),
    ("payment_initiated", "commerce", "Payment attempt started"),
    ("purchase_complete", "commerce", "Purchase succeeded"),
    ("purchase_failed", "commerce", "Payment declined or error"),
    ("login", "account", "User logged in"),
    ("logout", "account", "User logged out"),
    ("profile_update", "account", "Profile data changed"),
    ("password_change", "account", "Password updated"),
    ("notification_toggle", "account", "Push notifications enabled/disabled"),
    ("app_open", "system", "App brought to foreground"),
    ("app_background", "system", "App sent to background"),
    ("app_crash", "system", "Unhandled exception"),
    ("push_notification_received", "system", "Push notification delivered"),
    ("push_notification_tapped", "system", "User tapped push notification"),
]

_APP_VERSIONS: list[tuple[str, str, int]] = [
    # (version_code, platform, days_ago_released)
    ("2.1.0-ios", "ios", 300),
    ("2.2.0-ios", "ios", 240),
    ("2.3.0-ios", "ios", 180),
    ("2.4.0-ios", "ios", 90),
    ("2.5.0-ios", "ios", 30),
    ("2.1.0-android", "android", 300),
    ("2.2.0-android", "android", 240),
    ("2.3.0-android", "android", 180),
    ("2.4.0-android", "android", 90),
    ("2.5.0-android", "android", 30),
]

_DEVICE_MODELS: list[tuple[str, str, str]] = [
    # (model, os_name, device_type)
    ("iPhone 14", "iOS", "mobile"),
    ("iPhone 14 Pro", "iOS", "mobile"),
    ("iPhone 15", "iOS", "mobile"),
    ("iPhone 15 Pro", "iOS", "mobile"),
    ("iPad Pro 12.9", "iOS", "tablet"),
    ("Samsung Galaxy S23", "Android", "mobile"),
    ("Samsung Galaxy S23 Ultra", "Android", "mobile"),
    ("Google Pixel 7", "Android", "mobile"),
    ("Google Pixel 8", "Android", "mobile"),
    ("OnePlus 11", "Android", "mobile"),
    ("Xiaomi 13", "Android", "mobile"),
    ("Oppo Find X6", "Android", "mobile"),
    ("Samsung Galaxy A54", "Android", "mobile"),
    ("Realme 11 Pro", "Android", "mobile"),
    ("Samsung Galaxy Tab S8", "Android", "tablet"),
]

_OS_VERSIONS: dict[str, list[str]] = {
    "iOS": ["16.0", "16.5", "17.0", "17.2", "17.4"],
    "Android": ["12", "13", "14"],
}

_SCREEN_RESOLUTIONS: dict[str, list[str]] = {
    "mobile": ["390x844", "393x852", "412x915", "360x800", "1080x2400"],
    "tablet": ["1024x1366", "800x1280"],
}

_COUNTRIES: list[str] = ["US", "GB", "DE", "FR", "CA", "AU", "TR", "AZ", "NL", "SE"]

# Screen weights by category for realistic navigation
_SCREEN_WEIGHTS: dict[str, int] = {
    "home": 25,
    "product_list": 20,
    "product_detail": 18,
    "search": 15,
    "cart": 8,
    "profile": 5,
    "notifications": 5,
    "checkout": 3,
    "order_confirmation": 1,
    "order_history": 2,
    "explore": 6,
    "settings": 3,
    "payment_methods": 1,
    "addresses": 1,
    "help": 1,
    "faq": 1,
    "live_chat": 0,
    "login": 2,
    "register": 0,
    "forgot_password": 0,
}


class AppInteractionGenerator:
    """Generates realistic mobile app interaction data for a fintech/commerce app.

    Volumes (defaults):
        - 5000 users × 3 tiers (free 60%, standard 30%, premium 10%)
        - 2000 unique devices (~1.5 devices per user)
        - 10 app versions (5 iOS + 5 Android)
        - 20 screens, 28 event types
        - ~90k sessions over date_range_days (avg ~0.6 sessions/user/day)
        - ~1.2M events (avg ~13 events/session)
    """

    def __init__(
        self,
        user_count: int = 5000,
        date_range_days: int = 30,
        seed: int | None = None,
    ) -> None:
        self.user_count = user_count
        self.date_range_days = date_range_days
        self._faker = Faker()
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)
        self._now = datetime.now()
        self._start_date = self._now - timedelta(days=date_range_days)

    def generate_user_tiers(self) -> list[UserTier]:
        return [
            UserTier(1, "free", 1000, "Free tier with basic features"),
            UserTier(2, "standard", 10000, "Standard tier with extended features"),
            UserTier(3, "premium", -1, "Premium tier with unlimited access"),
        ]

    def generate_screens(self) -> list[Screen]:
        return [
            Screen(screen_id=i + 1, screen_name=name, screen_category=cat, description=desc)
            for i, (name, cat, desc) in enumerate(_SCREENS)
        ]

    def generate_event_types(self) -> list[EventType]:
        return [
            EventType(event_type_id=i + 1, event_name=name, event_category=cat, description=desc)
            for i, (name, cat, desc) in enumerate(_EVENT_TYPES)
        ]

    def generate_app_versions(self) -> list[AppVersion]:
        versions = []
        for i, (code, platform, days_ago) in enumerate(_APP_VERSIONS):
            versions.append(
                AppVersion(
                    version_id=i + 1,
                    version_code=code,
                    platform=platform,
                    release_date=(self._now - timedelta(days=days_ago)).date(),
                    is_force_update=(days_ago > 200),
                )
            )
        return versions

    def generate_devices(self, count: int = 2000) -> list[Device]:
        devices = []
        for i in range(count):
            model, os_name, device_type = random.choice(_DEVICE_MODELS)
            os_version = random.choice(_OS_VERSIONS[os_name])
            resolution = random.choice(_SCREEN_RESOLUTIONS[device_type])
            fingerprint = hashlib.md5(f"{model}-{os_version}-{i}".encode()).hexdigest()
            devices.append(
                Device(
                    device_id=i + 1,
                    device_fingerprint=fingerprint,
                    device_type=device_type,
                    os_name=os_name,
                    os_version=os_version,
                    device_model=model,
                    screen_resolution=resolution,
                    created_at=self._faker.date_time_between(
                        start_date="-2y", end_date="now"
                    ),
                )
            )
        return devices

    def generate_users(self, tier_ids: list[int] | None = None) -> list[AppUser]:
        if tier_ids is None:
            tier_ids = [1, 1, 1, 2, 2, 3]  # 50% free, 33% standard, 17% premium
        users = []
        for i in range(self.user_count):
            users.append(
                AppUser(
                    user_id=i + 1,
                    external_user_id=str(uuid.uuid4()),
                    username=self._faker.user_name(),
                    email=self._faker.unique.email(),
                    tier_id=random.choice(tier_ids),
                    registered_at=self._faker.date_time_between(
                        start_date="-2y", end_date="-30d"
                    ),
                )
            )
        return users

    def generate_user_devices(
        self, users: list[AppUser], devices: list[Device]
    ) -> list[UserDevice]:
        """Assign 1–3 devices per user, creating user_device associations."""
        user_devices = []
        for user in users:
            num_devices = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
            assigned = random.sample(devices, min(num_devices, len(devices)))
            for device in assigned:
                first = self._faker.date_time_between(
                    start_date=user.registered_at, end_date="now"
                )
                last = self._faker.date_time_between(start_date=first, end_date="now")
                user_devices.append(
                    UserDevice(
                        user_id=user.user_id,
                        device_id=device.device_id,
                        first_seen=first,
                        last_seen=last,
                    )
                )
        return user_devices

    def generate_sessions_for_date(
        self,
        target_date: date,
        users: list[AppUser],
        devices: list[Device],
        app_versions: list[AppVersion],
        user_devices: list[UserDevice],
    ) -> list[Session]:
        """Generate sessions for a single calendar day (~0.6 sessions/user/day)."""
        # Build user → device mapping
        user_device_map: dict[int, list[int]] = {}
        for ud in user_devices:
            user_device_map.setdefault(ud.user_id, []).append(ud.device_id)

        # Platform-filtered version pools
        ios_versions = [v for v in app_versions if v.platform == "ios"]
        android_versions = [v for v in app_versions if v.platform == "android"]

        sessions: list[Session] = []
        for user in users:
            if random.random() > 0.6:
                continue  # ~40% of users inactive on a given day
            num_sessions = random.choices([1, 2, 3, 4], weights=[55, 30, 12, 3])[0]
            device_ids = user_device_map.get(user.user_id, [devices[0].device_id])
            device_id = random.choice(device_ids)
            device = next(d for d in devices if d.device_id == device_id)

            version_pool = ios_versions if device.os_name == "iOS" else android_versions
            version = random.choices(
                version_pool,
                weights=[1, 2, 4, 8, 15][: len(version_pool)],  # newer = more popular
            )[0]

            day_start = datetime.combine(target_date, datetime.min.time())
            for _ in range(num_sessions):
                start_offset = random.randint(0, 86400 - 60)
                session_start = day_start + timedelta(seconds=start_offset)
                duration = random.randint(30, 1800)  # 30s – 30min
                session_end = session_start + timedelta(seconds=duration)
                sessions.append(
                    Session(
                        session_id=str(uuid.uuid4()),
                        user_id=user.user_id,
                        device_id=device_id,
                        version_id=version.version_id,
                        session_start=session_start,
                        session_end=session_end,
                        ip_address=self._faker.ipv4_public(),
                        country_code=random.choice(_COUNTRIES),
                    )
                )
        return sessions

    def generate_events_for_sessions(
        self,
        sessions: list[Session],
        screens: list[Screen],
        event_types: list[EventType],
    ) -> list[Event]:
        """Generate realistic event sequences for a list of sessions."""
        screen_by_name = {s.screen_name: s for s in screens}
        event_by_name = {e.event_name: e for e in event_types}

        screen_names = list(_SCREEN_WEIGHTS.keys())
        screen_weights = [_SCREEN_WEIGHTS[n] for n in screen_names]

        events: list[Event] = []
        for session in sessions:
            if session.session_end is None:
                continue
            total_duration_s = int(
                (session.session_end - session.session_start).total_seconds()
            )
            num_events = max(3, min(50, total_duration_s // 15))
            current_ts = session.session_start
            time_slice = total_duration_s / num_events

            # Always start with app_open
            events.append(
                Event(
                    event_id=0,
                    session_id=session.session_id,
                    event_type_id=event_by_name["app_open"].event_type_id,
                    event_timestamp=current_ts,
                    sequence_number=1,
                    duration_ms=random.randint(100, 500),
                    screen_id=None,
                    properties={"source": random.choice(["icon", "push", "deeplink"])},
                )
            )
            current_ts += timedelta(seconds=time_slice)

            # Mid-session: screen views + interactions
            for seq in range(2, num_events):
                screen_name = random.choices(screen_names, weights=screen_weights)[0]
                screen = screen_by_name[screen_name]
                event_ts = current_ts + timedelta(
                    seconds=random.uniform(0, time_slice * 0.8)
                )

                # screen_view first
                events.append(
                    Event(
                        event_id=0,
                        session_id=session.session_id,
                        event_type_id=event_by_name["screen_view"].event_type_id,
                        event_timestamp=event_ts,
                        sequence_number=seq,
                        duration_ms=random.randint(200, 3000),
                        screen_id=screen.screen_id,
                        properties={"screen_name": screen_name},
                    )
                )

                # Follow-up interaction based on screen category
                interaction = self._pick_interaction(screen_name, event_by_name)
                if interaction:
                    events.append(
                        Event(
                            event_id=0,
                            session_id=session.session_id,
                            event_type_id=interaction.event_type_id,
                            event_timestamp=event_ts + timedelta(milliseconds=random.randint(300, 2000)),
                            sequence_number=seq,
                            duration_ms=random.randint(50, 500),
                            screen_id=screen.screen_id,
                            properties=self._make_properties(interaction.event_name),
                        )
                    )
                current_ts += timedelta(seconds=time_slice)

            # Always end with app_background (or rare crash)
            end_event_name = "app_crash" if random.random() < 0.005 else "app_background"
            events.append(
                Event(
                    event_id=0,
                    session_id=session.session_id,
                    event_type_id=event_by_name[end_event_name].event_type_id,
                    event_timestamp=session.session_end,
                    sequence_number=num_events,
                    duration_ms=0,
                    screen_id=None,
                    properties={},
                )
            )
        return events

    def generate_sessions_and_events_by_date(
        self,
        users: list[AppUser],
        devices: list[Device],
        app_versions: list[AppVersion],
        user_devices: list[UserDevice],
        screens: list[Screen],
        event_types: list[EventType],
    ) -> Generator[tuple[date, list[Session], list[Event]], None, None]:
        """Yield (date, sessions, events) for each day in the configured date range."""
        current = self._start_date.date()
        end = self._now.date()
        while current <= end:
            sessions = self.generate_sessions_for_date(
                current, users, devices, app_versions, user_devices
            )
            events = self.generate_events_for_sessions(sessions, screens, event_types)
            yield current, sessions, events
            current += timedelta(days=1)

    def _pick_interaction(
        self,
        screen_name: str,
        event_by_name: dict[str, EventType],
    ) -> EventType | None:
        commerce_funnel = {
            "product_detail": ("add_to_cart", 0.35),
            "product_list": ("product_view", 0.70),
            "cart": ("checkout_start", 0.40),
            "checkout": ("payment_initiated", 0.60),
        }
        if screen_name in commerce_funnel:
            event_name, prob = commerce_funnel[screen_name]
            if random.random() < prob:
                return event_by_name.get(event_name)
        if screen_name == "search":
            return event_by_name.get("search_query")
        if random.random() < 0.5:
            return event_by_name.get("button_click")
        return None

    def _make_properties(self, event_name: str) -> dict:
        if event_name == "add_to_cart":
            return {
                "product_id": str(random.randint(1000, 9999)),
                "price": str(round(random.uniform(5, 500), 2)),
                "quantity": str(random.randint(1, 3)),
            }
        if event_name == "purchase_complete":
            return {
                "order_id": str(uuid.uuid4())[:8],
                "total": str(round(random.uniform(10, 1000), 2)),
                "currency": random.choice(["USD", "EUR", "GBP"]),
            }
        if event_name == "search_query":
            return {"query": self._faker.word(), "results_count": str(random.randint(0, 200))}
        if event_name == "button_click":
            return {"button_id": self._faker.slug()}
        return {}
