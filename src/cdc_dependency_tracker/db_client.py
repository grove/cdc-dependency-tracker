"""Database client for connection pooling and query execution."""

from typing import Any, Dict, List, Optional, Set
from contextlib import contextmanager
from psycopg2 import pool
from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ, ISOLATION_LEVEL_READ_COMMITTED
import logging

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Database client with connection pooling."""

    def __init__(
        self,
        connection_params: Dict[str, Any],
        tracking_table: str = "entities_to_reprocess",
        pool_size: int = 10,
    ):
        """
        Initialize database client with connection pool.

        Args:
            connection_params: psycopg2 connection parameters
            tracking_table: Name of the tracking table for base entities
            pool_size: Maximum number of connections in pool
        """
        self.connection_params = connection_params
        self.tracking_table = tracking_table
        self.pool = pool.ThreadedConnectionPool(minconn=1, maxconn=pool_size, **connection_params)
        logger.info(f"Created connection pool with max {pool_size} connections")

    @contextmanager
    def get_connection(self, isolation_level=ISOLATION_LEVEL_READ_COMMITTED):
        """
        Get a database connection from the pool.

        Args:
            isolation_level: Transaction isolation level

        Yields:
            Database connection
        """
        conn = self.pool.getconn()
        try:
            conn.set_isolation_level(isolation_level)
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    @contextmanager
    def transaction(self, isolation_level=ISOLATION_LEVEL_REPEATABLE_READ):
        """
        Execute statements in a transaction with specified isolation level.

        Args:
            isolation_level: Transaction isolation level

        Yields:
            Database cursor
        """
        with self.get_connection(isolation_level) as conn:
            with conn.cursor() as cur:
                yield cur

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query
            params: Query parameters

        Returns:
            List of result tuples
        """
        with self.transaction() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def execute_insert(self, query: str, params: Optional[tuple] = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE and return affected row count.

        Args:
            query: SQL statement
            params: Statement parameters

        Returns:
            Number of affected rows
        """
        with self.transaction() as cur:
            cur.execute(query, params)
            return cur.rowcount

    def track_entity(self, entity_id: str, id_column: str = "entity_id") -> None:
        """
        Track a single base entity for reprocessing.

        Args:
            entity_id: Base entity ID to track
            id_column: Name of the ID column in tracking table
        """
        query = f"""
            INSERT INTO {self.tracking_table} ({id_column})
            VALUES (%s)
            ON CONFLICT ({id_column}) 
            DO UPDATE SET last_tracked_at = CURRENT_TIMESTAMP
        """
        self.execute_insert(query, (entity_id,))

    def track_entities(self, entity_ids: Set[str], id_column: str = "entity_id") -> int:
        """
        Track multiple base entities for reprocessing.

        Args:
            entity_ids: Set of base entity IDs
            id_column: Name of the ID column in tracking table

        Returns:
            Number of entities tracked
        """
        if not entity_ids:
            return 0

        # Batch insert with parameterized query for safety
        placeholders = ",".join(["(%s)"] * len(entity_ids))
        query = f"""
            INSERT INTO {self.tracking_table} ({id_column})
            VALUES {placeholders}
            ON CONFLICT ({id_column})
            DO UPDATE SET last_tracked_at = CURRENT_TIMESTAMP
        """
        return self.execute_insert(query, tuple(entity_ids))

    def insert_intermediate(self, table_name: str, entity_id: str, depth: int) -> None:
        """
        Insert item into intermediate tracking table.

        Args:
            table_name: Source table name
            entity_id: Entity ID
            depth: Hop distance from base table
        """
        query = """
            INSERT INTO intermediate_to_track (table_name, entity_id, depth)
            VALUES (%s, %s, %s)
            ON CONFLICT (table_name, entity_id) 
            DO UPDATE SET tracked_at = CURRENT_TIMESTAMP
        """
        self.execute_insert(query, (table_name, entity_id, depth))

    def query_parent_entity(
        self, parent_table: str, parent_col: str, child_col_value: str, filters: str = ""
    ) -> Optional[str]:
        """
        Query parent entity ID by traversing a join relationship.

        Args:
            parent_table: Name of parent table
            parent_col: Column name containing parent ID
            child_col_value: Value to match
            filters: Optional WHERE clause filters (e.g., "_deleted = FALSE")

        Returns:
            Parent entity ID or None
        """
        where_clause = "WHERE _id = %s"
        if filters:
            where_clause += f" AND {filters}"

        query = f"""
            SELECT {parent_col} FROM {parent_table} 
            {where_clause}
        """
        results = self.execute_query(query, (child_col_value,))
        return results[0][0] if results else None

    def close(self) -> None:
        """Close all connections in the pool."""
        self.pool.closeall()
        logger.info("Closed connection pool")
