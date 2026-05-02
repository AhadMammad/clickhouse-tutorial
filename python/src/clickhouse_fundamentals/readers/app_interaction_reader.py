"""Console report formatter for mobile app interaction data (PostgreSQL)."""

from __future__ import annotations

import logging

from tabulate import tabulate

from clickhouse_fundamentals.db.pg_repository import AppInteractionRepository

logger = logging.getLogger(__name__)


class AppInteractionReader:
    """Formats and prints analytics reports from the PostgreSQL interaction tables."""

    def __init__(self, pg_repo: AppInteractionRepository) -> None:
        self._repo = pg_repo

    def print_summary(self) -> None:
        """Print total sessions, events, users, and date range."""
        stats = self._repo.get_stats()
        print("\n=== Mobile App Interaction Summary ===")
        rows = [
            ["Users", f"{stats.get('users_count', 0):,}"],
            ["Devices", f"{stats.get('devices_count', 0):,}"],
            ["Sessions", f"{stats.get('sessions_count', 0):,}"],
            ["Events", f"{stats.get('events_count', 0):,}"],
            ["Date range", f"{stats.get('min_date', 'N/A')}  →  {stats.get('max_date', 'N/A')}"],
        ]
        print(tabulate(rows, headers=["Metric", "Value"], tablefmt="rounded_outline"))

    def print_daily_sessions(self) -> None:
        """Print sessions-per-day table."""
        df = self._repo.get_daily_sessions()
        if df.empty:
            print("\nNo session data found.")
            return
        print("\n=== Daily Sessions ===")
        print(tabulate(df, headers=list(df.columns), tablefmt="rounded_outline", showindex=False))

    def print_top_screens(self, limit: int = 10) -> None:
        """Print most-visited screens."""
        df = self._repo.get_top_screens(limit=limit)
        if df.empty:
            print("\nNo screen data found.")
            return
        print(f"\n=== Top {limit} Screens by Event Count ===")
        print(tabulate(df, headers=list(df.columns), tablefmt="rounded_outline", showindex=False))

    def print_event_distribution(self) -> None:
        """Print event count per category."""
        df = self._repo.get_event_category_distribution()
        if df.empty:
            print("\nNo event data found.")
            return
        total = df["event_count"].sum()
        df["pct"] = (df["event_count"] / total * 100).round(1).astype(str) + "%"
        print("\n=== Event Category Distribution ===")
        print(tabulate(df, headers=list(df.columns), tablefmt="rounded_outline", showindex=False))

    def print_platform_breakdown(self) -> None:
        """Print iOS vs Android session and user counts."""
        df = self._repo.get_platform_breakdown()
        if df.empty:
            print("\nNo platform data found.")
            return
        print("\n=== Sessions by Platform ===")
        print(tabulate(df, headers=list(df.columns), tablefmt="rounded_outline", showindex=False))

    def print_full_report(self) -> None:
        """Print all available reports."""
        self.print_summary()
        self.print_daily_sessions()
        self.print_top_screens()
        self.print_event_distribution()
        self.print_platform_breakdown()
