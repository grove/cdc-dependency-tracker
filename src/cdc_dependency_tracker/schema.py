"""Database schema definitions and migrations."""

# SQL for creating tracking tables

INTERMEDIATE_TRACKING_TABLE = """
-- Intermediate staging for expensive resolutions
CREATE TABLE IF NOT EXISTS intermediate_to_track (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    depth INTEGER NOT NULL,
    tracked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    percolated BOOLEAN DEFAULT FALSE,
    UNIQUE(table_name, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_intermediate_pending 
ON intermediate_to_track (percolated, depth) 
WHERE percolated = FALSE;

CREATE INDEX IF NOT EXISTS idx_intermediate_tracked_at
ON intermediate_to_track (tracked_at)
WHERE percolated = TRUE;
"""

# Template for base table tracking (to be formatted with table name and id column)
BASE_TRACKING_TABLE_TEMPLATE = """
-- Final destination for affected base entities
CREATE TABLE IF NOT EXISTS {tracking_table} (
    {id_column} VARCHAR PRIMARY KEY,
    last_tracked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_{tracking_table}_tracked_at
ON {tracking_table} (last_tracked_at);
"""

ARCHIVE_TABLE = """
-- Optional: Archive table for historical tracking
CREATE TABLE IF NOT EXISTS intermediate_to_track_archive (
    id INTEGER,
    table_name VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    depth INTEGER NOT NULL,
    tracked_at TIMESTAMP,
    percolated BOOLEAN,
    percolated_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_archive_tracked_at
ON intermediate_to_track_archive (tracked_at);
"""


def create_schema(cursor, tracking_table: str = "customers_to_reprocess", id_column: str = "customer_id") -> None:
    """
    Create all tracking tables and indexes.
    
    Args:
        cursor: Database cursor
        tracking_table: Name of the base entity tracking table
        id_column: Name of the ID column in tracking table
    """
    cursor.execute(INTERMEDIATE_TRACKING_TABLE)
    cursor.execute(BASE_TRACKING_TABLE_TEMPLATE.format(
        tracking_table=tracking_table,
        id_column=id_column
    ))
    cursor.execute(ARCHIVE_TABLE)


def drop_schema(cursor, tracking_table: str = "customers_to_reprocess") -> None:
    """
    Drop all tracking tables (for testing).
    
    Args:
        cursor: Database cursor
        tracking_table: Name of the base entity tracking table
    """
    cursor.execute("DROP TABLE IF EXISTS intermediate_to_track_archive CASCADE")
    cursor.execute(f"DROP TABLE IF EXISTS {tracking_table} CASCADE")
    cursor.execute("DROP TABLE IF EXISTS intermediate_to_track CASCADE")
