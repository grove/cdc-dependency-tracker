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


def main():
    parser = argparse.ArgumentParser(description="Setup CDC dependency tracker database")
    parser.add_argument(
        "--config",
        "-c",
        default="examples/config.yaml",
        help="Path to config file"
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop existing tables before creating"
    )
    
    args = parser.parse_args()
    
    try:
        print(f"Loading configuration from: {args.config}")
        cfg = Config.from_yaml(args.config)
        
        # Derive ID column from base table
        base_table = cfg.tracker.base_table
        if base_table.endswith('s'):
            base_id_column = base_table[:-1] + "_id"  # customers → customer_id
        else:
            base_id_column = base_table + "_id"  # user → user_id
        
        tracking_table = cfg.tracker.tracking_table
        
        print(f"Connecting to database: {cfg.database.host}:{cfg.database.port}/{cfg.database.dbname}")
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
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name IN ('intermediate_to_track', %s)
                ORDER BY table_name
            """, (tracking_table,))
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
