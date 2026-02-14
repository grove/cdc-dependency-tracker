"""Streaming module for CDC event consumption from PostgreSQL logical replication."""

import logging
import threading
from typing import Optional

from .config import Config
from .sql_parser import SQLParser
from .dependency_graph import DependencyGraph
from .db_client import DatabaseClient
from .routing import RoutingEngine
from .replication_consumer import ReplicationConsumer

logger = logging.getLogger(__name__)


class StreamingManager:
    """Manages CDC streaming lifecycle."""
    
    def __init__(
        self,
        config: Config,
        verbose: bool = False
    ):
        """
        Initialize streaming manager.
        
        Args:
            config: Full configuration object
            verbose: Enable verbose logging
        """
        self.config = config
        self.verbose = verbose
        
        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Initialize components
        self._initialize_components()
        
        # Consumer will be created when starting
        self.consumer: Optional[ReplicationConsumer] = None
        self._streaming_thread: Optional[threading.Thread] = None
    
    def _initialize_components(self):
        """Initialize all required components."""
        logger.info(f"Initializing components for base table: {self.config.tracker.base_table}")
        
        # Parse SQL to build dependency graph
        parser = SQLParser(
            self.config.tracker.sql_query,
            self.config.tracker.base_table
        )
        join_graph = parser.get_join_graph()
        self.dependency_graph = DependencyGraph(
            self.config.tracker.base_table,
            join_graph
        )
        logger.info(f"Built dependency graph: {self.dependency_graph}")
        
        # Get base ID column
        self.base_id_column = parser.get_base_id_column()
        if self.base_id_column:
            logger.info(f"Using base_id_column: {self.base_id_column} (parsed from SQL)")
        else:
            # Fall back to name-based heuristic
            base_table = self.config.tracker.base_table
            if base_table.endswith('s'):
                self.base_id_column = base_table[:-1] + "_id"
            else:
                self.base_id_column = base_table + "_id"
            logger.info(f"Using base_id_column: {self.base_id_column} (derived from table name)")
        
        # Initialize database client
        self.db_client = DatabaseClient(
            self.config.database.to_connection_params(),
            tracking_table=self.config.tracker.tracking_table
        )
        
        # Initialize routing engine
        self.routing_engine = RoutingEngine(
            self.db_client,
            self.dependency_graph,
            base_table_id_column=self.base_id_column,
            immediate_threshold=self.config.tracker.immediate_fanout_threshold
        )
    
    def start_streaming(self, blocking: bool = True) -> Optional[threading.Thread]:
        """
        Start streaming CDC events from PostgreSQL.
        
        Args:
            blocking: If True, blocks until streaming stops. 
                     If False, starts streaming in background thread.
        
        Returns:
            If blocking=False, returns the streaming thread.
            Otherwise returns None.
        
        Raises:
            ValueError: If replication is not enabled in configuration
        """
        # Validate replication is enabled
        if not self.config.replication or not self.config.replication.enabled:
            raise ValueError(
                "Replication is not enabled in configuration. "
                "Add 'replication:' section to config.yaml with 'enabled: true'"
            )
        
        logger.info("Starting replication consumer...")
        logger.info(
            f"Slot: {self.config.replication.slot_name}, "
            f"Plugin: {self.config.replication.plugin}"
        )
        
        # Get tables from dependency graph
        tables = self.dependency_graph.get_all_tables()
        logger.info(f"Tables to track: {', '.join(tables)}")
        
        # Create replication consumer
        self.consumer = ReplicationConsumer(
            self.config.replication,
            self.routing_engine,
            tables=tables
        )
        
        if blocking:
            # Start streaming (blocking)
            self.consumer.start_streaming()
            return None
        else:
            # Start streaming in background thread
            self._streaming_thread = threading.Thread(
                target=self.consumer.start_streaming,
                daemon=True,
                name="cdc-streaming"
            )
            self._streaming_thread.start()
            logger.info("Streaming started in background thread")
            return self._streaming_thread
    
    def stop_streaming(self):
        """Stop the streaming consumer."""
        if self.consumer:
            logger.info("Stopping streaming consumer...")
            self.consumer.disconnect()
            self.consumer = None
        
        if self._streaming_thread and self._streaming_thread.is_alive():
            logger.info("Waiting for streaming thread to finish...")
            self._streaming_thread.join(timeout=5.0)
    
    def close(self):
        """Cleanup resources."""
        self.stop_streaming()
        if self.db_client:
            self.db_client.close()
    
    def __enter__(self):
        """Context manager support."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()


def start_cdc_streaming(
    config: Config,
    verbose: bool = False,
    blocking: bool = True
) -> StreamingManager:
    """
    Convenience function to start CDC streaming.
    
    Args:
        config: Full configuration object
        verbose: Enable verbose logging
        blocking: If True, blocks until streaming stops
        
    Returns:
        StreamingManager instance
        
    Example:
        >>> config = Config.from_yaml('config.yaml')
        >>> manager = start_cdc_streaming(config, blocking=False)
        >>> # Do other work...
        >>> manager.stop_streaming()
    """
    manager = StreamingManager(config, verbose=verbose)
    manager.start_streaming(blocking=blocking)
    return manager
