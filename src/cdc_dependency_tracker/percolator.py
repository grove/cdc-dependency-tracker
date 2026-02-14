"""Percolation engine for batch resolution of intermediate items."""

from typing import Dict, Any
import logging

from .db_client import DatabaseClient
from .dependency_graph import DependencyGraph

logger = logging.getLogger(__name__)


class PercolationEngine:
    """Batch resolves intermediate tracking items to base entities."""
    
    def __init__(
        self, 
        db_client: DatabaseClient, 
        dependency_graph: DependencyGraph,
        base_id_column: str = "entity_id",
        batch_size: int = 1000
    ):
        """
        Initialize percolation engine.
        
        Args:
            db_client: Database client
            dependency_graph: Dependency graph for join traversal
            base_id_column: Column name for base entity ID in tracking table
            batch_size: Maximum items to process per batch
        """
        self.db = db_client
        self.graph = dependency_graph
        self.base_id_column = base_id_column
        self.batch_size = batch_size
        self._build_percolation_query()
    
    def _build_percolation_query(self) -> None:
        """Build dynamic percolation query based on dependency graph."""
        # Get all non-base tables that might be in intermediate tracking
        tables = [t for t in self.graph._depths.keys() if t != self.graph.base_table]
        
        if not tables:
            logger.warning("No dependent tables found in dependency graph")
            self.percolation_query = None
            return
        
        # Build joins dynamically by following the path from each table to base
        # We'll create a UNION query for each table type
        union_parts = []
        
        for table in tables:
            depth = self.graph.get_depth(table)
            if depth <= 0:
                continue
            
            # Build join path to base table
            try:
                path = self.graph.get_path_to_base(table)
            except Exception as e:
                logger.warning(f"Cannot build path from {table} to {self.graph.base_table}: {e}")
                continue
            
            if not path:
                continue
            
            # Start with intermediate_to_track
            from_clause = "intermediate_to_track it"
            current_alias = "t0"
            
            # First join to the table itself
            from_clause += f"\n    JOIN {table} AS {current_alias} ON it.table_name = '{table}' AND {current_alias}._id = it.entity_id"
            
            # Add _deleted filter for first table if it exists
            from_clause += f" AND {current_alias}._deleted = FALSE"
            
            # Follow the join path to base table
            for i, join_step in enumerate(path):
                next_alias = f"t{i+1}"
                from_clause += f"\n    JOIN {join_step.to_table} AS {next_alias} ON {current_alias}.{join_step.from_col} = {next_alias}.{join_step.to_col}"
                
                # Add _deleted filter if the table has it
                from_clause += f" AND {next_alias}._deleted = FALSE"
                
                current_alias = next_alias
            
            # The last alias is the base table
            base_alias = current_alias
            union_parts.append((from_clause, base_alias))
        
        if not union_parts:
            logger.warning("Could not build percolation paths")
            self.percolation_query = None
            return
        
        # Combine all paths with UNION
        # Since they all resolve to base table ID, we can UNION them
        select_parts = []
        for from_clause, base_alias in union_parts:
            select_parts.append(f"""
            SELECT DISTINCT {base_alias}._id, CURRENT_TIMESTAMP
            FROM {from_clause}
            WHERE it.percolated = FALSE
            """)
        
        # Combine with UNION ALL (faster than UNION if duplicates are ok, ON CONFLICT handles them)
        combined_select = "\nUNION ALL\n".join(select_parts)
        
        self.percolation_query = f"""
        INSERT INTO {self.db.tracking_table} ({self.base_id_column}, last_tracked_at)
        {combined_select}
        LIMIT %s
        ON CONFLICT ({self.base_id_column}) 
        DO UPDATE SET last_tracked_at = CURRENT_TIMESTAMP
        """
        
        logger.info(f"Built percolation query for {len(union_parts)} table paths")
        logger.debug(f"Percolation query: {self.percolation_query}")
    
    def percolate_batch(self) -> Dict[str, Any]:
        """
        Percolate one batch of intermediate items to base entities.
        
        Returns:
            Dictionary with metrics: entities_tracked, items_percolated
        """
        if not self.percolation_query:
            logger.error("Percolation query not built, cannot percolate")
            return {"entities_tracked": 0, "items_percolated": 0}
        
        with self.db.transaction() as cur:
            # Execute the dynamically built percolation query
            cur.execute(self.percolation_query, (self.batch_size,))
            
            affected_entities = cur.rowcount
            
            # Mark items as percolated
            cur.execute("""
                UPDATE intermediate_to_track
                SET percolated = TRUE
                WHERE id IN (
                    SELECT id FROM intermediate_to_track
                    WHERE percolated = FALSE
                    ORDER BY tracked_at ASC
                    LIMIT %s
                )
            """, (self.batch_size,))
            
            percolated_items = cur.rowcount
            
            return {
                'entities_tracked': affected_entities,
                'items_percolated': percolated_items
            }
    
    def get_pending_count(self) -> int:
        """Get count of pending intermediate items."""
        result = self.db.execute_query(
            "SELECT COUNT(*) FROM intermediate_to_track WHERE percolated = FALSE"
        )
        return result[0][0] if result else 0
    
    def get_percolation_lag(self) -> int:
        """
        Get age of oldest unpercolated item in seconds.
        
        Returns:
            Age in seconds, or 0 if no pending items
        """
        result = self.db.execute_query("""
            SELECT EXTRACT(EPOCH FROM (NOW() - MIN(tracked_at)))::INTEGER
            FROM intermediate_to_track 
            WHERE percolated = FALSE
        """)
        return result[0][0] if result and result[0][0] else 0
    
    def cleanup_old_items(self, retention_days: int = 7) -> int:
        """
        Clean up old percolated items.
        
        Args:
            retention_days: Number of days to retain
            
        Returns:
            Number of items deleted
        """
        return self.db.execute_insert("""
            DELETE FROM intermediate_to_track
            WHERE percolated = TRUE
              AND tracked_at < NOW() - INTERVAL '%s days'
        """ % retention_days)
    
    def archive_old_items(self, retention_days: int = 7) -> int:
        """
        Archive old percolated items before deletion.
        
        Args:
            retention_days: Number of days to retain
            
        Returns:
            Number of items archived
        """
        return self.db.execute_insert("""
            INSERT INTO intermediate_to_track_archive 
                (id, table_name, entity_id, depth, tracked_at, percolated, percolated_at)
            SELECT 
                id, table_name, entity_id, depth, tracked_at, percolated, NOW()
            FROM intermediate_to_track
            WHERE percolated = TRUE
              AND tracked_at < NOW() - INTERVAL '%s days'
        """ % retention_days)
