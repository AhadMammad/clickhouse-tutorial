#!/usr/bin/env python3
"""ClickHouse Fundamentals — Main CLI Application.

This is the main entry point for the ClickHouse learning project.
It provides commands for ClickHouse fundamentals AND the mobile app interaction
data pipeline (PostgreSQL → HDFS Parquet → ClickHouse Star Schema).

Usage:
    # ClickHouse fundamentals (original)
    python main.py setup                        Create ClickHouse schema
    python main.py generate --rows N            Generate payment transactions
    python main.py report                       Print analytics reports
    python main.py demo                         Full fundamentals demo

    # Mobile app interaction pipeline (new)
    python main.py pg-generate --users 5000     Generate & write app interaction data to PostgreSQL
    python main.py pg-report                    Print analytics from PostgreSQL
    python main.py export-raw --days 30         Export each PG table as raw Parquet to HDFS
    python main.py raw-to-silver --days 30      Join raw tables into silver Parquet on HDFS
    python main.py star-setup                   Create ClickHouse star schema tables
    python main.py star-load --days 30          Load silver Parquet → ClickHouse star
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from clickhouse_fundamentals.config import ClickHouseConfig, HdfsConfig, PostgresConfig
from clickhouse_fundamentals.db.client import ClickHouseClient, ClickHouseError
from clickhouse_fundamentals.db.pg_client import PostgresClient
from clickhouse_fundamentals.db.pg_repository import AppInteractionRepository
from clickhouse_fundamentals.db.repository import TransactionRepository
from clickhouse_fundamentals.etl.parquet_to_star import ParquetToStarLoader
from clickhouse_fundamentals.etl.pg_to_parquet import PgToParquetExporter
from clickhouse_fundamentals.etl.pg_to_raw import PgToRawExporter
from clickhouse_fundamentals.etl.raw_to_silver import RawToSilverTransformer
from clickhouse_fundamentals.generators.app_interaction_generator import (
    AppInteractionGenerator,
)
from clickhouse_fundamentals.generators.transaction_generator import (
    TransactionGenerator,
)
from clickhouse_fundamentals.hdfs.client import HdfsClient
from clickhouse_fundamentals.readers.analytics_reader import AnalyticsReader
from clickhouse_fundamentals.readers.app_interaction_reader import AppInteractionReader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# SQL directories
_docker_sql = Path("/sql")
SQL_DIR = _docker_sql if _docker_sql.exists() else Path(__file__).parent.parent / "sql"

_docker_sql_star = Path("/sql_star")
SQL_STAR_DIR = (
    _docker_sql_star
    if _docker_sql_star.exists()
    else Path(__file__).parent.parent / "sql_star"
)


# =============================================================================
# Helpers
# =============================================================================

def run_sql_file(client: ClickHouseClient, filepath: Path) -> None:
    """Execute a SQL file, skipping comments and empty lines."""
    logger.info("Executing %s...", filepath.name)

    content = filepath.read_text()
    statements = []
    current_statement: list[str] = []

    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        if "--" in line:
            line = line[: line.index("--")]
        stripped = line.strip()
        current_statement.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current_statement).strip()
            if statement and statement != ";":
                statements.append(statement)
            current_statement = []

    executed = 0
    for statement in statements:
        clean = statement.strip().rstrip(";")
        if not clean or clean.startswith("--"):
            continue
        try:
            client.execute(statement)
            executed += 1
        except Exception as exc:
            if "already exists" in str(exc).lower():
                logger.debug("Object already exists, skipping: %s", str(exc)[:100])
            else:
                logger.warning("Statement warning: %s", str(exc)[:200])

    logger.info("Executed %d statements from %s", executed, filepath.name)


def _parse_date_arg(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format '{value}'. Use YYYY-MM-DD.")


# =============================================================================
# ClickHouse fundamentals commands (unchanged)
# =============================================================================

def cmd_setup(config: ClickHouseConfig) -> int:
    logger.info("Setting up ClickHouse schema...")
    try:
        with ClickHouseClient(config) as client:
            if not client.ping():
                logger.error("Cannot connect to ClickHouse")
                return 1
            sql_files = sorted(SQL_DIR.glob("*.sql"))
            if not sql_files:
                logger.warning("No SQL files found in %s", SQL_DIR)
                return 1
            for sql_file in sql_files:
                run_sql_file(client, sql_file)
            logger.info("Schema setup complete!")
            return 0
    except ClickHouseError as exc:
        logger.error("Database error: %s", exc)
        return 1
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        return 1


def cmd_generate(config: ClickHouseConfig, rows: int, batch_size: int = 10000) -> int:
    logger.info("Generating %d sample transactions...", rows)
    try:
        with ClickHouseClient(config) as client:
            repository = TransactionRepository(client)
            generator = TransactionGenerator(
                user_count=10000, merchant_count=1000, date_range_days=90
            )
            estimate = generator.estimate_data_size(rows)
            logger.info(
                "Estimated size: %.1f MB uncompressed, %.1f-%.1f MB compressed",
                estimate["estimated_uncompressed_mb"],
                estimate["estimated_compressed_mb_low"],
                estimate["estimated_compressed_mb_high"],
            )
            total_inserted = 0
            for batch in generator.generate_batches(rows, batch_size=batch_size):
                inserted = repository.insert_batch(batch)
                total_inserted += inserted
                logger.info("Progress: %d/%d rows inserted", total_inserted, rows)
            logger.info("Data generation complete! Inserted %d transactions.", total_inserted)
            return 0
    except ClickHouseError as exc:
        logger.error("Database error: %s", exc)
        return 1
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        return 1


def cmd_report(config: ClickHouseConfig, days: int = 30) -> int:
    logger.info("Generating analytics reports...")
    try:
        with ClickHouseClient(config) as client:
            repository = TransactionRepository(client)
            reader = AnalyticsReader(repository)
            reader.print_full_report(days=days)
            return 0
    except ClickHouseError as exc:
        logger.error("Database error: %s", exc)
        return 1
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        return 1


def cmd_user(config: ClickHouseConfig, user_id: int) -> int:
    logger.info("Fetching profile for user_id=%d", user_id)
    try:
        with ClickHouseClient(config) as client:
            repository = TransactionRepository(client)
            reader = AnalyticsReader(repository)
            reader.print_user_profile(user_id)
            return 0
    except ClickHouseError as exc:
        logger.error("Database error: %s", exc)
        return 1
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        return 1


def cmd_demo(config: ClickHouseConfig, rows: int = 100000) -> int:
    print("\n" + "=" * 70)
    print("CLICKHOUSE FUNDAMENTALS — DEMO")
    print("=" * 70 + "\n")
    result = cmd_setup(config)
    if result != 0:
        return result
    print(f"\nStep 2: Generating {rows:,} transactions...")
    result = cmd_generate(config, rows=rows)
    if result != 0:
        return result
    print("\nStep 3: Running analytics reports...")
    result = cmd_report(config)
    if result != 0:
        return result
    print("\n" + "=" * 70)
    print("DEMO COMPLETE!")
    print("=" * 70 + "\n")
    return 0


# =============================================================================
# Mobile app interaction pipeline commands (new)
# =============================================================================

def cmd_pg_generate(pg_config: PostgresConfig, user_count: int, date_range_days: int) -> int:
    """Generate mobile app interaction data and write to PostgreSQL."""
    logger.info(
        "Generating app interaction data: %d users, %d days", user_count, date_range_days
    )
    try:
        gen = AppInteractionGenerator(user_count=user_count, date_range_days=date_range_days)

        tiers = gen.generate_user_tiers()
        screens = gen.generate_screens()
        event_types = gen.generate_event_types()
        app_versions = gen.generate_app_versions()

        logger.info("Generating users and devices...")
        users = gen.generate_users()
        devices = gen.generate_devices(count=min(user_count * 2 // 3 + 500, 3000))
        user_devices = gen.generate_user_devices(users, devices)

        with PostgresClient(pg_config) as pg_client:
            repo = AppInteractionRepository(pg_client)

            logger.info("Writing reference data...")
            repo.insert_user_tiers(tiers)
            repo.insert_screens(screens)
            repo.insert_event_types(event_types)
            repo.insert_app_versions(app_versions)

            logger.info("Writing %d users and %d devices...", len(users), len(devices))
            repo.insert_users(users)
            repo.insert_devices(devices)
            repo.insert_user_devices(user_devices)

            total_sessions = 0
            total_events = 0
            for dt, sessions, events in gen.generate_sessions_and_events_by_date(
                users, devices, app_versions, user_devices, screens, event_types
            ):
                repo.insert_sessions(sessions)
                repo.insert_events(events)
                total_sessions += len(sessions)
                total_events += len(events)
                logger.info(
                    "Date %s: %d sessions, %d events (totals: %d sessions, %d events)",
                    dt, len(sessions), len(events), total_sessions, total_events,
                )

        logger.info(
            "pg-generate complete: %d sessions, %d events written to PostgreSQL",
            total_sessions, total_events,
        )
        return 0

    except Exception as exc:
        logger.error("pg-generate failed: %s", exc, exc_info=True)
        return 1


def cmd_pg_report(pg_config: PostgresConfig) -> int:
    """Print analytics from the PostgreSQL interaction tables."""
    try:
        with PostgresClient(pg_config) as pg_client:
            repo = AppInteractionRepository(pg_client)
            reader = AppInteractionReader(repo)
            reader.print_full_report()
        return 0
    except Exception as exc:
        logger.error("pg-report failed: %s", exc)
        return 1


def cmd_export_parquet(
    pg_config: PostgresConfig, hdfs_config: HdfsConfig, start: date, end: date
) -> int:
    """Export PostgreSQL app interaction data to HDFS Parquet with daily partitions."""
    logger.info("Exporting Parquet from %s to %s", start, end)
    try:
        with PostgresClient(pg_config) as pg_client, HdfsClient(hdfs_config) as hdfs_client:
            repo = AppInteractionRepository(pg_client)
            exporter = PgToParquetExporter(repo, hdfs_client)
            paths = exporter.export_date_range(start, end)
        logger.info("export-parquet complete: %d partition(s) written", len(paths))
        for p in paths:
            print(f"  {p}")
        return 0
    except Exception as exc:
        logger.error("export-parquet failed: %s", exc, exc_info=True)
        return 1


def cmd_export_raw(
    pg_config: PostgresConfig, hdfs_config: HdfsConfig, start: date, end: date
) -> int:
    """Export each PostgreSQL table as raw Parquet to HDFS (no JOINs)."""
    logger.info("Exporting raw tables from %s to %s", start, end)
    try:
        with PostgresClient(pg_config) as pg_client, HdfsClient(hdfs_config) as hdfs_client:
            repo = AppInteractionRepository(pg_client)
            exporter = PgToRawExporter(repo, hdfs_client)
            paths = exporter.export_date_range(start, end)
        logger.info("export-raw complete: %d file(s) written", len(paths))
        for p in paths:
            print(f"  {p}")
        return 0
    except Exception as exc:
        logger.error("export-raw failed: %s", exc, exc_info=True)
        return 1


def cmd_raw_to_silver(hdfs_config: HdfsConfig, start: date, end: date) -> int:
    """Join raw HDFS Parquet files into the denormalized silver layer."""
    logger.info("Transforming raw → silver from %s to %s", start, end)
    try:
        with HdfsClient(hdfs_config) as hdfs_client:
            transformer = RawToSilverTransformer(hdfs_client)
            paths = transformer.transform_date_range(start, end)
        logger.info("raw-to-silver complete: %d partition(s) written", len(paths))
        for p in paths:
            print(f"  {p}")
        return 0
    except Exception as exc:
        logger.error("raw-to-silver failed: %s", exc, exc_info=True)
        return 1


def cmd_star_setup(ch_config: ClickHouseConfig) -> int:
    """Create ClickHouse star schema dimension and fact tables."""
    logger.info("Setting up ClickHouse star schema...")
    try:
        with ClickHouseClient(ch_config) as client:
            if not client.ping():
                logger.error("Cannot connect to ClickHouse")
                return 1
            sql_files = sorted(SQL_STAR_DIR.glob("0[12]_*.sql"))
            if not sql_files:
                logger.warning("No star schema SQL files found in %s", SQL_STAR_DIR)
                return 1
            for sql_file in sql_files:
                run_sql_file(client, sql_file)
            logger.info("Star schema setup complete (%d files executed).", len(sql_files))
        return 0
    except ClickHouseError as exc:
        logger.error("Database error: %s", exc)
        return 1
    except Exception as exc:
        logger.error("Unexpected error: %s", exc)
        return 1


def cmd_star_load(
    ch_config: ClickHouseConfig, hdfs_config: HdfsConfig, start: date, end: date
) -> int:
    """Load HDFS Parquet files into the ClickHouse star schema."""
    logger.info("Loading star schema from %s to %s", start, end)
    try:
        with HdfsClient(hdfs_config) as hdfs_client, ClickHouseClient(ch_config) as ch_client:
            loader = ParquetToStarLoader(hdfs_client, ch_client)
            totals = loader.load_date_range(start, end)
        logger.info("star-load complete: %s", totals)
        return 0
    except Exception as exc:
        logger.error("star-load failed: %s", exc, exc_info=True)
        return 1


# =============================================================================
# CLI entry point
# =============================================================================

def main() -> int:
    load_dotenv()

    env_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.getLogger().setLevel(getattr(logging, env_level, logging.INFO))

    parser = argparse.ArgumentParser(
        description="ClickHouse Fundamentals + Mobile App Interaction Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
ClickHouse fundamentals:
  python main.py setup                          Create ClickHouse schema
  python main.py generate --rows 100000         Generate payment transactions
  python main.py report --days 30               Print analytics reports
  python main.py user --id 1001                 Show user profile
  python main.py demo                           Full fundamentals demo

Mobile app interaction pipeline:
  python main.py pg-generate --users 5000       Generate & write to PostgreSQL
  python main.py pg-report                      Print PostgreSQL analytics
  python main.py export-raw --days 30           Export each PG table as raw Parquet (ELT)
  python main.py raw-to-silver --days 30        Join raw tables into silver Parquet
  python main.py star-setup                     Create ClickHouse star schema
  python main.py star-load --days 30            Load silver Parquet into ClickHouse star
        """,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- Original commands ---
    subparsers.add_parser("setup", help="Set up ClickHouse schema")

    gen_parser = subparsers.add_parser("generate", help="Generate sample payment data")
    gen_parser.add_argument("--rows", type=int, default=100000)
    gen_parser.add_argument("--batch-size", type=int, default=10000)

    report_parser = subparsers.add_parser("report", help="Print ClickHouse analytics")
    report_parser.add_argument("--days", type=int, default=30)

    user_parser = subparsers.add_parser("user", help="Show user profile")
    user_parser.add_argument("--id", type=int, required=True)

    demo_parser = subparsers.add_parser("demo", help="Run full fundamentals demo")
    demo_parser.add_argument("--rows", type=int, default=100000)

    # --- New pipeline commands ---
    pg_gen_parser = subparsers.add_parser(
        "pg-generate", help="Generate mobile app data and write to PostgreSQL"
    )
    pg_gen_parser.add_argument(
        "--users", type=int, default=5000, help="Number of users to generate (default: 5000)"
    )
    pg_gen_parser.add_argument(
        "--days", type=int, default=30, help="Number of days of history (default: 30)"
    )

    subparsers.add_parser("pg-report", help="Print mobile app analytics from PostgreSQL")

    ep_parser = subparsers.add_parser(
        "export-parquet", help="Export PostgreSQL data to HDFS Parquet (daily partitions)"
    )
    ep_date_group = ep_parser.add_mutually_exclusive_group()
    ep_date_group.add_argument(
        "--date", type=_parse_date_arg, help="Single date to export (YYYY-MM-DD)"
    )
    ep_date_group.add_argument(
        "--days", type=int, default=30, help="Export last N days (default: 30)"
    )

    er_parser = subparsers.add_parser(
        "export-raw", help="Export each PostgreSQL table as raw Parquet to HDFS (no JOINs)"
    )
    er_date_group = er_parser.add_mutually_exclusive_group()
    er_date_group.add_argument(
        "--date", type=_parse_date_arg, help="Single date to export (YYYY-MM-DD)"
    )
    er_date_group.add_argument(
        "--days", type=int, default=30, help="Export last N days (default: 30)"
    )

    rts_parser = subparsers.add_parser(
        "raw-to-silver", help="Join raw HDFS Parquet tables into silver denormalized layer"
    )
    rts_date_group = rts_parser.add_mutually_exclusive_group()
    rts_date_group.add_argument(
        "--date", type=_parse_date_arg, help="Single date to transform (YYYY-MM-DD)"
    )
    rts_date_group.add_argument(
        "--days", type=int, default=30, help="Transform last N days (default: 30)"
    )

    subparsers.add_parser("star-setup", help="Create ClickHouse star schema tables")

    sl_parser = subparsers.add_parser(
        "star-load", help="Load HDFS Parquet files into ClickHouse star schema"
    )
    sl_date_group = sl_parser.add_mutually_exclusive_group()
    sl_date_group.add_argument(
        "--date", type=_parse_date_arg, help="Single date to load (YYYY-MM-DD)"
    )
    sl_date_group.add_argument(
        "--days", type=int, default=30, help="Load last N days (default: 30)"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.command is None:
        parser.print_help()
        return 1

    try:
        ch_config = ClickHouseConfig()
        pg_config = PostgresConfig()
        hdfs_config = HdfsConfig()
    except ValueError as exc:
        logger.critical("Invalid configuration: %s", exc)
        return 1

    # --- Dispatch ---
    if args.command == "setup":
        return cmd_setup(ch_config)

    elif args.command == "generate":
        return cmd_generate(ch_config, rows=args.rows, batch_size=args.batch_size)

    elif args.command == "report":
        return cmd_report(ch_config, days=args.days)

    elif args.command == "user":
        return cmd_user(ch_config, user_id=args.id)

    elif args.command == "demo":
        return cmd_demo(ch_config, rows=args.rows)

    elif args.command == "pg-generate":
        return cmd_pg_generate(pg_config, user_count=args.users, date_range_days=args.days)

    elif args.command == "pg-report":
        return cmd_pg_report(pg_config)

    elif args.command == "export-parquet":
        if args.date:
            start_date = end_date = args.date
        else:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=args.days - 1)
        return cmd_export_parquet(pg_config, hdfs_config, start_date, end_date)

    elif args.command == "export-raw":
        if args.date:
            start_date = end_date = args.date
        else:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=args.days - 1)
        return cmd_export_raw(pg_config, hdfs_config, start_date, end_date)

    elif args.command == "raw-to-silver":
        if args.date:
            start_date = end_date = args.date
        else:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=args.days - 1)
        return cmd_raw_to_silver(hdfs_config, start_date, end_date)

    elif args.command == "star-setup":
        return cmd_star_setup(ch_config)

    elif args.command == "star-load":
        if args.date:
            start_date = end_date = args.date
        else:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=args.days - 1)
        return cmd_star_load(ch_config, hdfs_config, start_date, end_date)

    parser.print_help()
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        logging.getLogger(__name__).critical(
            "Unhandled exception — process exiting: %s", exc, exc_info=True
        )
        sys.exit(1)
