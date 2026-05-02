"""PostgreSQL data access layer for the mobile app interaction pipeline."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime

import pandas as pd

from clickhouse_fundamentals.db.pg_client import PostgresClient
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

logger = logging.getLogger(__name__)


class AppInteractionRepository:
    """Repository for all mobile app interaction tables in PostgreSQL."""

    def __init__(self, client: PostgresClient) -> None:
        self._client = client

    # -------------------------------------------------------------------------
    # Reference / lookup tables
    # -------------------------------------------------------------------------

    def insert_user_tiers(self, tiers: list[UserTier]) -> int:
        rows = [(t.tier_name, t.max_monthly_events, t.description) for t in tiers]
        n = self._client.execute_values(
            "INSERT INTO user_tiers (tier_name, max_monthly_events, description) "
            "VALUES %s ON CONFLICT (tier_name) DO NOTHING",
            rows,
        )
        logger.info("Upserted %d user tiers", len(tiers))
        return n

    def insert_screens(self, screens: list[Screen]) -> int:
        rows = [(s.screen_name, s.screen_category, s.description) for s in screens]
        n = self._client.execute_values(
            "INSERT INTO screens (screen_name, screen_category, description) "
            "VALUES %s ON CONFLICT (screen_name) DO NOTHING",
            rows,
        )
        logger.info("Upserted %d screens", len(screens))
        return n

    def insert_event_types(self, event_types: list[EventType]) -> int:
        rows = [(e.event_name, e.event_category, e.description) for e in event_types]
        n = self._client.execute_values(
            "INSERT INTO event_types (event_name, event_category, description) "
            "VALUES %s ON CONFLICT (event_name) DO NOTHING",
            rows,
        )
        logger.info("Upserted %d event types", len(event_types))
        return n

    def insert_app_versions(self, versions: list[AppVersion]) -> int:
        rows = [
            (v.version_code, v.platform, v.release_date, v.is_force_update)
            for v in versions
        ]
        n = self._client.execute_values(
            "INSERT INTO app_versions (version_code, platform, release_date, is_force_update) "
            "VALUES %s ON CONFLICT (version_code) DO NOTHING",
            rows,
        )
        logger.info("Upserted %d app versions", len(versions))
        return n

    # -------------------------------------------------------------------------
    # User / device tables
    # -------------------------------------------------------------------------

    def insert_users(self, users: list[AppUser]) -> int:
        rows = [
            (u.external_user_id, u.username, u.email, u.tier_id, u.registered_at)
            for u in users
        ]
        n = self._client.execute_values(
            "INSERT INTO users (external_user_id, username, email, tier_id, registered_at) "
            "VALUES %s ON CONFLICT (external_user_id) DO NOTHING",
            rows,
        )
        logger.info("Upserted %d users", len(users))
        return n

    def insert_devices(self, devices: list[Device]) -> int:
        rows = [
            (
                d.device_fingerprint,
                d.device_type,
                d.os_name,
                d.os_version,
                d.device_model,
                d.screen_resolution,
                d.created_at,
            )
            for d in devices
        ]
        n = self._client.execute_values(
            "INSERT INTO devices "
            "(device_fingerprint, device_type, os_name, os_version, device_model, screen_resolution, created_at) "
            "VALUES %s ON CONFLICT (device_fingerprint) DO NOTHING",
            rows,
        )
        logger.info("Upserted %d devices", len(devices))
        return n

    def insert_user_devices(self, user_devices: list[UserDevice]) -> int:
        rows = [(ud.user_id, ud.device_id, ud.first_seen, ud.last_seen) for ud in user_devices]
        n = self._client.execute_values(
            "INSERT INTO user_devices (user_id, device_id, first_seen, last_seen) "
            "VALUES %s ON CONFLICT (user_id, device_id) DO UPDATE SET last_seen = EXCLUDED.last_seen",
            rows,
        )
        logger.info("Upserted %d user-device links", len(user_devices))
        return n

    # -------------------------------------------------------------------------
    # Session / event tables
    # -------------------------------------------------------------------------

    def insert_sessions(self, sessions: list[Session]) -> int:
        rows = [
            (
                s.session_id,
                s.user_id,
                s.device_id,
                s.version_id,
                s.session_start,
                s.session_end,
                s.ip_address,
                s.country_code or None,
            )
            for s in sessions
        ]
        n = self._client.execute_values(
            "INSERT INTO sessions "
            "(session_id, user_id, device_id, version_id, session_start, session_end, ip_address, country_code) "
            "VALUES %s ON CONFLICT (session_id) DO NOTHING",
            rows,
        )
        logger.info("Inserted %d sessions", len(sessions))
        return n

    def insert_events(self, events: list[Event]) -> int:
        rows = [
            (
                e.session_id,
                e.screen_id,
                e.event_type_id,
                e.event_timestamp,
                e.sequence_number,
                e.duration_ms,
                json.dumps(e.properties),
            )
            for e in events
        ]
        n = self._client.execute_values(
            "INSERT INTO events "
            "(session_id, screen_id, event_type_id, event_timestamp, sequence_number, duration_ms, properties) "
            "VALUES %s",
            rows,
        )
        logger.info("Inserted %d events", len(events))
        return n

    # -------------------------------------------------------------------------
    # Read / analytics methods
    # -------------------------------------------------------------------------

    def get_events_by_date(self, dt: date) -> pd.DataFrame:
        """Return fully joined event data for a single day (used by ETL exporter).

        duration_seconds is calculated (session_end - session_start), not stored.
        """
        query = """
            SELECT
                e.event_id,
                e.event_timestamp,
                e.sequence_number,
                e.duration_ms,
                e.properties::text                                          AS properties,
                s.session_id,
                s.session_start,
                s.session_end,
                EXTRACT(EPOCH FROM (s.session_end - s.session_start))::INT  AS session_duration_seconds,
                s.country_code,
                s.user_id,
                u.external_user_id,
                u.username,
                ut.tier_name,
                d.device_id,
                d.device_fingerprint,
                d.device_type,
                d.os_name,
                d.os_version,
                d.device_model,
                sc.screen_id,
                sc.screen_name,
                sc.screen_category,
                et.event_type_id,
                et.event_name,
                et.event_category,
                av.version_id,
                av.version_code,
                av.platform
            FROM events e
            JOIN sessions s   ON e.session_id    = s.session_id
            JOIN users u      ON s.user_id        = u.user_id
            JOIN user_tiers ut ON u.tier_id       = ut.tier_id
            JOIN devices d    ON s.device_id      = d.device_id
            LEFT JOIN screens sc ON e.screen_id   = sc.screen_id
            JOIN event_types et ON e.event_type_id = et.event_type_id
            JOIN app_versions av ON s.version_id  = av.version_id
            WHERE DATE(e.event_timestamp) = %s
            ORDER BY e.event_id
        """
        return self._client.query_df(query, (dt,))

    def get_date_range(self) -> tuple[date, date]:
        """Return (min_date, max_date) of event_timestamp in the events table."""
        rows = self._client.query(
            "SELECT MIN(DATE(event_timestamp)), MAX(DATE(event_timestamp)) FROM events"
        )
        if rows and rows[0][0] is not None:
            return rows[0][0], rows[0][1]
        today = datetime.now().date()
        return today, today

    def get_stats(self) -> dict:
        """Return summary statistics for the pg-report command."""
        stats: dict = {}
        for table in ("users", "devices", "sessions", "events"):
            rows = self._client.query(f"SELECT COUNT(*) FROM {table}")
            stats[f"{table}_count"] = rows[0][0] if rows else 0
        date_range = self.get_date_range()
        stats["min_date"] = str(date_range[0])
        stats["max_date"] = str(date_range[1])
        return stats

    def get_daily_sessions(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT DATE(session_start) AS session_date, COUNT(*) AS session_count "
            "FROM sessions GROUP BY session_date ORDER BY session_date"
        )

    def get_top_screens(self, limit: int = 10) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT sc.screen_name, sc.screen_category, COUNT(*) AS event_count "
            "FROM events e JOIN screens sc ON e.screen_id = sc.screen_id "
            "GROUP BY sc.screen_name, sc.screen_category "
            "ORDER BY event_count DESC LIMIT %s",
            (limit,),
        )

    def get_event_category_distribution(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT et.event_category, COUNT(*) AS event_count "
            "FROM events e JOIN event_types et ON e.event_type_id = et.event_type_id "
            "GROUP BY et.event_category ORDER BY event_count DESC"
        )

    def get_platform_breakdown(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT av.platform, COUNT(DISTINCT s.session_id) AS sessions, "
            "COUNT(DISTINCT s.user_id) AS unique_users "
            "FROM sessions s JOIN app_versions av ON s.version_id = av.version_id "
            "GROUP BY av.platform ORDER BY sessions DESC"
        )

    # -------------------------------------------------------------------------
    # Raw extract — per-table reads (ELT layer)
    # -------------------------------------------------------------------------

    def get_events_raw(self, dt: date) -> pd.DataFrame:
        """Return raw events rows for a single day with no JOINs."""
        return self._client.query_df(
            "SELECT event_id, session_id::text, screen_id, event_type_id, "
            "event_timestamp, sequence_number, duration_ms, properties::text AS properties "
            "FROM events WHERE DATE(event_timestamp) = %s ORDER BY event_id",
            (dt,),
        )

    def get_sessions_raw(self, dt: date) -> pd.DataFrame:
        """Return raw sessions rows whose session_start falls on dt with no JOINs."""
        return self._client.query_df(
            "SELECT session_id::text, user_id, device_id, version_id, "
            "session_start, session_end, ip_address::text AS ip_address, country_code "
            "FROM sessions WHERE DATE(session_start) = %s ORDER BY session_start",
            (dt,),
        )

    def get_users_snapshot(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT user_id, external_user_id, username, email, tier_id, registered_at "
            "FROM users ORDER BY user_id"
        )

    def get_devices_snapshot(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT device_id, device_fingerprint, device_type, os_name, os_version, "
            "device_model, screen_resolution, created_at FROM devices ORDER BY device_id"
        )

    def get_user_tiers_snapshot(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT tier_id, tier_name, max_monthly_events, description "
            "FROM user_tiers ORDER BY tier_id"
        )

    def get_screens_snapshot(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT screen_id, screen_name, screen_category, description "
            "FROM screens ORDER BY screen_id"
        )

    def get_event_types_snapshot(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT event_type_id, event_name, event_category, description "
            "FROM event_types ORDER BY event_type_id"
        )

    def get_app_versions_snapshot(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT version_id, version_code, platform, release_date, is_force_update "
            "FROM app_versions ORDER BY version_id"
        )

    def get_user_devices_snapshot(self) -> pd.DataFrame:
        return self._client.query_df(
            "SELECT user_id, device_id, first_seen, last_seen "
            "FROM user_devices ORDER BY user_id, device_id"
        )
