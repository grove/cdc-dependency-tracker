"""Adaptive routing engine for CDC events."""

from typing import Set
from dataclasses import dataclass
import logging

from .cdc_handler import CDCEvent
from .dependency_graph import DependencyGraph
from .db_client import DatabaseClient

logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    """Result of processing a CDC event."""

    immediate: int = 0  # Number of base entities tracked immediately
    deferred: int = 0  # Number of items deferred to intermediate
    queries: int = 0  # Number of database queries executed
    skipped: bool = False  # Whether event was skipped

    def __repr__(self) -> str:
        if self.skipped:
            return "ProcessingResult(skipped=True)"
        return f"ProcessingResult(immediate={self.immediate}, deferred={self.deferred}, queries={self.queries})"


class RoutingEngine:
    """Routes CDC events to immediate or deferred processing."""

    def __init__(
        self,
        db_client: DatabaseClient,
        dependency_graph: DependencyGraph,
        base_table_id_column: str = "entity_id",
        immediate_threshold: int = 100,
    ):
        """
        Initialize routing engine.

        Args:
            db_client: Database client
            dependency_graph: Dependency graph
            base_table_id_column: Name of ID column in base table tracking table
            immediate_threshold: Max fanout for immediate resolution
        """
        self.db = db_client
        self.graph = dependency_graph
        self.base_table_id_column = base_table_id_column
        self.threshold = immediate_threshold

    def handle_event(self, event: CDCEvent) -> ProcessingResult:
        """
        Route CDC event based on table and estimated cost.

        Args:
            event: Parsed CDC event

        Returns:
            Processing result
        """
        table = event.table

        # Skip truncate operations
        if event.is_truncate:
            logger.warning(f"Skipping TRUNCATE operation on {table}")
            return ProcessingResult(skipped=True)

        # Route based on table
        if table == self.graph.base_table:
            return self._handle_base_table(event)

        depth = self.graph.get_depth(table)

        if depth == 1:  # 1-hop from base table
            return self._handle_depth_1(event)
        elif depth == 2:  # 2-hop from base table
            return self._handle_depth_2_adaptive(event)
        elif depth >= 3:  # 3+ hops from base table
            return self._handle_depth_3_plus(event)
        else:
            logger.warning(f"Unknown table: {table}, skipping")
            return ProcessingResult(skipped=True)

    def _handle_base_table(self, event: CDCEvent) -> ProcessingResult:
        """Handle base table changes - always immediate."""
        entity_id = event.get_id_field()

        if not entity_id:
            logger.warning(f"No ID field in base table event: {event}")
            return ProcessingResult(skipped=True)

        self.db.track_entity(entity_id, self.base_table_id_column)
        logger.info(f"Tracked {self.graph.base_table} {entity_id} (base table change)")

        return ProcessingResult(immediate=1, queries=0)

    def _handle_depth_1(self, event: CDCEvent) -> ProcessingResult:
        """Handle depth-1 tables - always immediate, parent ID in event."""
        base_entities: Set[str] = set()

        # Get parent join info to find the parent ID column
        parent_info = self.graph.get_parent_join(event.table)
        if not parent_info:
            logger.error(f"No parent join found for {event.table}")
            return ProcessingResult(skipped=True)

        # Unpack tuple: (parent_table, join_col_in_table, join_col_in_parent)
        _, parent_id_col, _ = parent_info

        if event.is_create:
            # INSERT or snapshot
            parent_id = event.after.get(parent_id_col) if event.after else None
            if parent_id:
                base_entities.add(parent_id)

        elif event.is_update:
            # Check if parent ID changed (mutable join key)
            # If no before state, just use after state
            before_parent_id = event.before.get(parent_id_col) if event.before else None
            after_parent_id = event.after.get(parent_id_col) if event.after else None

            if before_parent_id != after_parent_id:
                # Both old and new parents affected
                if before_parent_id:
                    base_entities.add(before_parent_id)
                if after_parent_id:
                    base_entities.add(after_parent_id)
            else:
                # Only current parent affected
                if after_parent_id:
                    base_entities.add(after_parent_id)

        elif event.is_delete:
            parent_id = event.before.get(parent_id_col) if event.before else None
            if parent_id:
                base_entities.add(parent_id)

        if base_entities:
            count = self.db.track_entities(base_entities, self.base_table_id_column)
            logger.info(
                f"Tracked {count} {self.graph.base_table} entities for {event.table} change (immediate)"
            )
            return ProcessingResult(immediate=count, queries=0)

        return ProcessingResult(skipped=True)

    def _handle_depth_2_adaptive(self, event: CDCEvent) -> ProcessingResult:
        """Handle depth-2 tables - adaptive based on complexity."""
        # Check if join keys changed (only if we have before state)
        if event.is_update and event.before:
            join_keys = self.graph.get_join_keys_for_table(event.table)
            keys_changed = False

            for key in join_keys:
                before_value = event.before.get(key)
                after_value = event.after.get(key) if event.after else None
                if before_value != after_value:
                    keys_changed = True
                    break

            if keys_changed:
                # Complex resolution needed, defer
                entity_id = event.get_id_field()
                if not entity_id:
                    logger.warning(f"No ID field in event: {event}")
                    return ProcessingResult(skipped=True)
                depth = self.graph.get_depth(event.table)
                self.db.insert_intermediate(event.table, entity_id, depth)
                logger.info(f"Deferred {event.table} {entity_id} (join key changed)")
                return ProcessingResult(deferred=1, queries=1)

        # Get parent join info
        parent_info = self.graph.get_parent_join(event.table)
        if not parent_info:
            logger.error(f"No parent join found for {event.table}")
            return ProcessingResult(skipped=True)

        parent_table, join_col, parent_col = parent_info

        # Get the parent row ID from event
        if event.is_delete:
            parent_row_id = event.before.get(join_col) if event.before else None
        else:
            parent_row_id = event.after.get(join_col) if event.after else None

        if not parent_row_id:
            logger.warning(f"No {join_col} in event: {event}")
            return ProcessingResult(skipped=True)

        # Simple heuristic: try immediate resolution
        try:
            # Query up one level to get the base entity ID
            # If parent is base table, we're done. Otherwise query again.
            if parent_table == self.graph.base_table:
                # Parent is base table, parent_row_id IS the base entity ID
                self.db.track_entity(parent_row_id, self.base_table_id_column)
                logger.info(
                    f"Tracked {self.graph.base_table} {parent_row_id} for {event.table} change (immediate)"
                )
                return ProcessingResult(immediate=1, queries=0)
            else:
                # Need to query parent to get base entity ID
                grandparent_info = self.graph.get_parent_join(parent_table)
                if grandparent_info:
                    _, grandparent_join_col, _ = grandparent_info
                    base_entity_id = self.db.query_parent_entity(
                        parent_table, grandparent_join_col, parent_row_id, "_deleted = FALSE"
                    )
                    if base_entity_id:
                        self.db.track_entity(base_entity_id, self.base_table_id_column)
                        logger.info(
                            f"Tracked {self.graph.base_table} {base_entity_id} for {event.table} change (immediate)"
                        )
                        return ProcessingResult(immediate=1, queries=1)
                    else:
                        logger.warning(
                            f"No {self.graph.base_table} found for {parent_table}={parent_row_id}"
                        )
                        return ProcessingResult(skipped=True, queries=1)
                else:
                    logger.error(f"Cannot traverse from {parent_table} to {self.graph.base_table}")
                    return ProcessingResult(skipped=True)
        except Exception as e:
            logger.error(f"Error in immediate resolution: {e}, deferring")
            entity_id = event.get_id_field()
            if not entity_id:
                logger.warning(f"No ID field in event: {event}")
                return ProcessingResult(skipped=True, queries=1)
            depth = self.graph.get_depth(event.table)
            self.db.insert_intermediate(event.table, entity_id, depth)
            return ProcessingResult(deferred=1, queries=1)

    def _handle_depth_3_plus(self, event: CDCEvent) -> ProcessingResult:
        """Handle depth-3+ tables - always defer."""
        entity_id = event.get_id_field()
        depth = self.graph.get_depth(event.table)

        if not entity_id:
            logger.warning(f"No ID field in event: {event}")
            return ProcessingResult(skipped=True)

        self.db.insert_intermediate(event.table, entity_id, depth)
        logger.info(f"Deferred {event.table} {entity_id} (depth-{depth}, always defer)")

        return ProcessingResult(deferred=1, queries=0)
