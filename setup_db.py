#!/usr/bin/env python3
"""Setup script for creating tracking tables in the database."""

import sys
import argparse
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from cdc_dependency_tracker.db_client import DatabaseClient
from cdc_dependency_tracker.schema import create_schema, drop_schema
from cdc_dependency_tracker.config import Config
from cdc_dependency_tracker.utils import derive_id_column_name
from cdc_dependency_tracker.sql_parser import SQLParser


def main():
    parser = argparse.ArgumentParser(description="Setup CDC dependency tracker database")
    parser.add_argument(
        "--config", "-c", default="examples/config.yaml", help="Path to config file"
    )
    parser.add_argument("--drop", action="store_true", help="Drop existing tables before creating")
    parser.add_argument(
        "--id-column",
        default=None,
        help="Override ID column name (default: derived from base table or SQL)",
    )

    args = parser.parse_args()

    try:
        print(f"Loading configuration from: {args.config}")
        cfg = Config.from_yaml(args.config)

        # Try to extract ID column from SQL query first
        base_id_column = None
        if cfg.tracker.sql_query:
            parser_obj = SQLParser(cfg.tracker.sql_query, cfg.tracker.base_table)
            base_id_column = parser_obj.get_base_id_column()
            if base_id_column:
                print(f"Extracted ID column from SQL: {base_id_column}")

        # Fall back to CLI override or derive from table name
        if args.id_column:
            base_id_column = args.id_column
            print(f"Using ID column from CLI: {base_id_column}")
        elif not base_id_column:
            base_id_column = derive_id_column_name(cfg.tracker.base_table)
            print(f"Derived ID column from table name: {base_id_column}")

        tracking_table = cfg.tracker.tracking_table

        print(
            f"Connecting to database: {cfg.database.host}:{cfg.database.port}/{cfg.database.dbname}"
        )
        print(f"Tracking table: {tracking_table}, ID column: {base_id_column}")

        db = DatabaseClient(cfg.database.to_connection_params(), tracking_table=tracking_table)

        with db.transaction() as cur:
            if args.drop:
                print("Dropping existing tables...")
                drop_schema(cur, tracking_table=tracking_table)
                print("Tables dropped")

            print("Creating tracking tables...")
            create_schema(cur, tracking_table=tracking_table, id_column=base_id_column)
            print("Tables created successfully!")

        # Verify tables exist
        with db.transaction() as cur:
            cur.execute(
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name IN ('intermediate_to_track', %s)
                ORDER BY table_name
            """,
                (tracking_table,),
            )
            tables = cur.fetchall()

            print("\nCreated tables:")
            for (table_name,) in tables:
                print(f"  ✓ {table_name}")

        db.close()
        print("\nSetup complete!")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
