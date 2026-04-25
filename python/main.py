#!/usr/bin/env python3
"""ClickHouse Fundamentals — Main CLI Application.

This is the main entry point for the ClickHouse learning project.
It provides commands for setup, data generation, and analytics.

Usage:
    python main.py setup              # Create database schema
    python main.py generate --rows N  # Generate sample data
    python main.py report             # Print analytics reports
    python main.py demo               # Run full demo
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from clickhouse_fundamentals.config import ClickHouseConfig
from clickhouse_fundamentals.db.client import ClickHouseClient, ClickHouseError
from clickhouse_fundamentals.db.repository import TransactionRepository
from clickhouse_fundamentals.generators.transaction_generator import (
    TransactionGenerator,
)
from clickhouse_fundamentals.readers.analytics_reader import AnalyticsReader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# SQL files directory
# In Docker, sql is mounted at /sql; locally it's relative to this file
_docker_sql = Path("/sql")
SQL_DIR = _docker_sql if _docker_sql.exists() else Path(__file__).parent.parent / "sql"


def run_sql_file(client: ClickHouseClient, filepath: Path) -> None:
    """Execute a SQL file, skipping comments and empty lines.

    Args:
        client: ClickHouse client.
        filepath: Path to the SQL file.
    """
    logger.info(f"Executing {filepath.name}...")

    content = filepath.read_text()

    # Split by semicolons, handling multiline statements
    statements = []
    current_statement = []

    for line in content.split("\n"):
        # Skip comment-only lines
        stripped = line.strip()
        if stripped.startswith("--"):
            continue

        # Remove inline comments (but be careful with strings)
        if "--" in line:
            line = line[: line.index("--")]

        # Re-strip after comment removal
        stripped = line.strip()

        current_statement.append(line)

        if stripped.endswith(";"):
            statement = "\n".join(current_statement).strip()
            if statement and statement != ";":
                statements.append(statement)
            current_statement = []

    # Execute each statement
    executed = 0
    for statement in statements:
        # Skip empty or comment-only statements
        clean = statement.strip().rstrip(";")
        if not clean or clean.startswith("--"):
            continue

        try:
            client.execute(statement)
            executed += 1
        except Exception as e:
            # Log but continue for idempotent operations
            if "already exists" in str(e).lower():
                logger.debug(f"Object already exists, skipping: {str(e)[:100]}")
            else:
                logger.warning(f"Statement warning: {str(e)[:200]}")

    logger.info(f"Executed {executed} statements from {filepath.name}")


def cmd_setup(config: ClickHouseConfig) -> int:
    """Set up the database schema by running all SQL files.

    Args:
        config: ClickHouse configuration.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    logger.info("Setting up ClickHouse schema...")

    try:
        with ClickHouseClient(config) as client:
            # Check connection
            if not client.ping():
                logger.error("Cannot connect to ClickHouse")
                return 1

            # Run SQL files in order
            sql_files = sorted(SQL_DIR.glob("*.sql"))

            if not sql_files:
                logger.warning(f"No SQL files found in {SQL_DIR}")
                return 1

            for sql_file in sql_files:
                run_sql_file(client, sql_file)

            logger.info("Schema setup complete!")
            return 0

    except ClickHouseError as e:
        logger.error(f"Database error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def cmd_generate(config: ClickHouseConfig, rows: int, batch_size: int = 10000) -> int:
    """Generate and insert sample transaction data.

    Args:
        config: ClickHouse configuration.
        rows: Number of rows to generate.
        batch_size: Batch size for inserts.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    logger.info(f"Generating {rows:,} sample transactions...")

    try:
        with ClickHouseClient(config) as client:
            repository = TransactionRepository(client)
            generator = TransactionGenerator(
                user_count=10000,
                merchant_count=1000,
                date_range_days=90,
            )

            # Show estimate
            estimate = generator.estimate_data_size(rows)
            uncompressed = estimate["estimated_uncompressed_mb"]
            low = estimate["estimated_compressed_mb_low"]
            high = estimate["estimated_compressed_mb_high"]
            logger.info(
                f"Estimated size: {uncompressed:.1f} MB uncompressed, "
                f"{low:.1f}-{high:.1f} MB compressed"
            )

            total_inserted = 0
            for batch in generator.generate_batches(rows, batch_size=batch_size):
                inserted = repository.insert_batch(batch)
                total_inserted += inserted
                logger.info(f"Progress: {total_inserted:,}/{rows:,} rows inserted")

            logger.info(
                f"Data generation complete! Inserted {total_inserted:,} transactions."
            )
            return 0

    except ClickHouseError as e:
        logger.error(f"Database error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def cmd_report(config: ClickHouseConfig, days: int = 30) -> int:
    """Print analytics reports.

    Args:
        config: ClickHouse configuration.
        days: Number of days to analyze.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    logger.info("Generating analytics reports...")

    try:
        with ClickHouseClient(config) as client:
            repository = TransactionRepository(client)
            reader = AnalyticsReader(repository)

            reader.print_full_report(days=days)
            return 0

    except ClickHouseError as e:
        logger.error(f"Database error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def cmd_user(config: ClickHouseConfig, user_id: int) -> int:
    """Print user profile.

    Args:
        config: ClickHouse configuration.
        user_id: User ID to analyze.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    logger.info(f"Fetching profile for user_id={user_id}")
    try:
        with ClickHouseClient(config) as client:
            repository = TransactionRepository(client)
            reader = AnalyticsReader(repository)

            reader.print_user_profile(user_id)
            return 0

    except ClickHouseError as e:
        logger.error(f"Database error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


def cmd_demo(config: ClickHouseConfig, rows: int = 100000) -> int:
    """Run full demo: setup, generate, and report.

    Args:
        config: ClickHouse configuration.
        rows: Number of rows to generate.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    print("\n" + "=" * 70)
    print("CLICKHOUSE FUNDAMENTALS — DEMO")
    print("=" * 70 + "\n")

    # Step 1: Setup
    print("Step 1: Setting up schema...")
    result = cmd_setup(config)
    if result != 0:
        return result

    # Step 2: Generate data
    print(f"\nStep 2: Generating {rows:,} transactions...")
    result = cmd_generate(config, rows=rows)
    if result != 0:
        return result

    # Step 3: Reports
    print("\nStep 3: Running analytics reports...")
    result = cmd_report(config)
    if result != 0:
        return result

    print("\n" + "=" * 70)
    print("DEMO COMPLETE!")
    print("=" * 70 + "\n")

    return 0


def main() -> int:
    """Main entry point."""
    # Load environment variables before anything reads them
    load_dotenv()

    # Honour LOG_LEVEL env var; --verbose flag overrides it later
    env_level = os.getenv("LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, env_level, logging.INFO)
    logging.getLogger().setLevel(numeric_level)

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="ClickHouse Fundamentals CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py setup                    Set up database schema
  python main.py generate --rows 100000   Generate 100k transactions
  python main.py report --days 30         Print 30-day analytics
  python main.py user --id 1001           Show user profile
  python main.py demo                     Run full demo
        """,
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # setup command
    subparsers.add_parser("setup", help="Set up database schema")

    # generate command
    gen_parser = subparsers.add_parser("generate", help="Generate sample data")
    gen_parser.add_argument(
        "--rows",
        type=int,
        default=100000,
        help="Number of rows to generate (default: 100000)",
    )
    gen_parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for inserts (default: 10000)",
    )

    # report command
    report_parser = subparsers.add_parser("report", help="Print analytics reports")
    report_parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to analyze (default: 30)",
    )

    # user command
    user_parser = subparsers.add_parser("user", help="Show user profile")
    user_parser.add_argument(
        "--id",
        type=int,
        required=True,
        help="User ID to analyze",
    )

    # demo command
    demo_parser = subparsers.add_parser("demo", help="Run full demo")
    demo_parser.add_argument(
        "--rows",
        type=int,
        default=100000,
        help="Number of rows to generate (default: 100000)",
    )

    args = parser.parse_args()

    # --verbose overrides LOG_LEVEL
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load config (raises ValueError on bad env vars)
    try:
        config = ClickHouseConfig()
    except ValueError as e:
        logger.critical(f"Invalid configuration: {e}")
        return 1

    logger.info(
        f"Starting ClickHouse CLI: command={args.command} "
        f"host={config.host}:{config.port} db={config.database}"
    )

    # Execute command
    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "setup":
        return cmd_setup(config)
    elif args.command == "generate":
        return cmd_generate(config, rows=args.rows, batch_size=args.batch_size)
    elif args.command == "report":
        return cmd_report(config, days=args.days)
    elif args.command == "user":
        return cmd_user(config, user_id=args.id)
    elif args.command == "demo":
        return cmd_demo(config, rows=args.rows)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        logging.getLogger(__name__).critical(
            f"Unhandled exception — process exiting: {e}", exc_info=True
        )
        sys.exit(1)
