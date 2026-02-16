"""PostgreSQL logical replication consumer using custom pgoutput decoder."""

import logging
import time
import signal
from typing import Optional, Dict, Any, List
import psycopg2

from .config import ReplicationConfig
from .cdc_handler import CDCEvent
from .routing import RoutingEngine
from .logical_replication_stream import LogicalReplicationStream

logger = logging.getLogger(__name__)

# Global shutdown flag
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown."""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


class ReplicationConsumer:
    """Consumes CDC events from PostgreSQL logical replication using custom pgoutput decoder."""

    def __init__(
        self,
        config: ReplicationConfig,
        routing_engine: RoutingEngine,
        tables: Optional[List[str]] = None,
        max_retries: int = 5,
        retry_delay: int = 5,
    ):
        """
        Initialize replication consumer.

        Args:
            config: Replication configuration
            routing_engine: Routing engine for processing events
            tables: List of tables to include in publication (if auto-creating)
            max_retries: Maximum reconnection attempts
            retry_delay: Seconds to wait between retries
        """
        self.config = config
        self.routing_engine = routing_engine
        self.tables = tables or []
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.reader: Optional[LogicalReplicationStream] = None

        # Register signal handlers
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def connect(self) -> None:
        """Establish logical replication connection using custom decoder."""
        try:
            logger.info(
                f"Connecting to {self.config.host}:{self.config.port}/{self.config.dbname} for replication"
            )

            self.reader = LogicalReplicationStream(
                publication_name=self.config.publication_name,
                slot_name=self.config.slot_name,
                host=self.config.host,
                database=self.config.dbname,
                port=str(self.config.port),
                user=self.config.user,
                password=self.config.password,
            )

            logger.info(
                f"Connected to replication slot '{self.config.slot_name}' with publication '{self.config.publication_name}'"
            )

        except Exception as e:
            logger.error(f"Failed to connect for replication: {e}")
            raise

    def disconnect(self) -> None:
        """Close replication connection."""
        if self.reader:
            try:
                self.reader.close()
                logger.info("Replication connection closed")
            except Exception as e:
                logger.warning(f"Error closing reader: {e}")
            finally:
                self.reader = None

    def _ensure_slot_exists(self) -> None:
        """Check if replication slot exists and create if needed."""
        if not self.config.auto_create_slot:
            return

        # Validate plugin
        if self.config.plugin != "pgoutput":
            raise ValueError(f"Only 'pgoutput' plugin is supported, got: {self.config.plugin}")

        try:
            # Need regular connection to check/create slot
            check_conn = psycopg2.connect(**self.config.to_connection_params())
            check_cur = check_conn.cursor()

            # Check if slot exists
            check_cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (self.config.slot_name,)
            )

            if check_cur.fetchone():
                logger.info(f"Replication slot '{self.config.slot_name}' already exists")
            else:
                # Create slot with pgoutput plugin
                logger.info(
                    f"Creating replication slot '{self.config.slot_name}' with plugin 'pgoutput'"
                )
                check_cur.execute(
                    "SELECT pg_create_logical_replication_slot(%s, %s)",
                    (self.config.slot_name, "pgoutput"),
                )
                logger.info(f"Replication slot '{self.config.slot_name}' created successfully")

            check_cur.close()
            check_conn.close()

        except psycopg2.Error as e:
            logger.error(f"Error ensuring replication slot exists: {e}")
            raise

    def _ensure_publication_exists(self) -> None:
        """Check if publication exists and create if needed."""
        if not self.config.auto_create_publication:
            return

        try:
            # Need regular connection to check/create publication
            check_conn = psycopg2.connect(**self.config.to_connection_params())
            check_cur = check_conn.cursor()

            # Check if publication exists
            check_cur.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s", (self.config.publication_name,)
            )

            if check_cur.fetchone():
                logger.info(f"Publication '{self.config.publication_name}' already exists")
            else:
                # Create publication
                if self.tables:
                    table_list = ", ".join(self.tables)
                    logger.info(
                        f"Creating publication '{self.config.publication_name}' for tables: {table_list}"
                    )
                    check_cur.execute(
                        f"CREATE PUBLICATION {self.config.publication_name} FOR TABLE {table_list}"
                    )
                else:
                    logger.info(
                        f"Creating publication '{self.config.publication_name}' for ALL TABLES"
                    )
                    check_cur.execute(
                        f"CREATE PUBLICATION {self.config.publication_name} FOR ALL TABLES"
                    )
                check_conn.commit()
                logger.info(f"Publication '{self.config.publication_name}' created successfully")

            check_cur.close()
            check_conn.close()

        except psycopg2.Error as e:
            logger.error(f"Error ensuring publication exists: {e}")
            raise

    def start_streaming(self) -> None:
        """
        Start streaming CDC events from replication slot using custom pgoutput decoder.

        This is a long-running operation that processes events until
        shutdown is requested or an unrecoverable error occurs.
        """
        retry_count = 0

        # Ensure slot and publication exist before streaming
        try:
            self._ensure_slot_exists()
            self._ensure_publication_exists()
        except Exception as e:
            logger.error(f"Failed to setup replication: {e}")
            raise

        while not shutdown_requested and retry_count < self.max_retries:
            try:
                if not self.reader:
                    self.connect()
                    retry_count = 0  # Reset on successful connection

                logger.info("Starting replication stream...")

                # Process messages using custom decoder's iterator
                self._consume_messages()

            except Exception as e:
                logger.error(f"Error during replication: {e}")
                retry_count += 1

                # Cleanup on error
                self.disconnect()

                if retry_count < self.max_retries:
                    logger.info(
                        f"Retrying in {self.retry_delay}s... (attempt {retry_count}/{self.max_retries})"
                    )
                    time.sleep(self.retry_delay)
                else:
                    logger.error("Max retries exceeded, giving up")
                    raise

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
                break

        if shutdown_requested:
            logger.info("Shutdown complete")

        self.disconnect()

    def _consume_messages(self) -> None:
        """Consume and process messages from replication stream using custom decoder."""
        message_count = 0

        if not self.reader:
            raise RuntimeError("Reader not initialized. Call connect() first.")

        try:
            for message in self.reader:
                if shutdown_requested:
                    logger.info("Shutdown requested, stopping message consumption")
                    break

                try:
                    # Convert decoded message to CDCEvent
                    event = self._parse_pgoutput_message(message)

                    # Process through routing engine
                    result = self.routing_engine.handle_event(event)

                    logger.debug(
                        f"Processed event: table={event.table}, op={event.operation} -> {result}"
                    )
                    message_count += 1

                    if message_count % 100 == 0:
                        logger.info(f"Processed {message_count} messages")

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    # Custom decoder handles acknowledgment internally
                    raise

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt during message consumption")
            raise

    def _parse_pgoutput_message(self, message) -> CDCEvent:
        """
        Convert decoded message to CDCEvent format.

        Custom decoder provides decoded messages from PostgreSQL logical replication:
        - message.op: operation type ('I' for insert, 'U' for update, 'D' for delete)
        - message.table_schema: schema name
        - message.table_name: table name
        - message.new_tuple: new tuple data (for INSERT/UPDATE)
        - message.old_tuple: old tuple data (for UPDATE/DELETE)

        Args:
            message: Decoded message object from LogicalReplicationStream

        Returns:
            Parsed CDCEvent
        """
        # Map pgoutput operation codes to Debezium-style codes
        op_map = {
            "I": "c",  # Insert -> Create
            "U": "u",  # Update -> Update
            "D": "d",  # Delete -> Delete
        }
        operation = op_map.get(message.op, "u")

        # Extract table metadata
        table = message.table_name
        schema = message.table_schema

        # Build after state from new_tuple (INSERT/UPDATE)
        after: Optional[Dict[str, Any]] = None
        if hasattr(message, "new_tuple") and message.new_tuple:
            after = {}
            for col in message.new_tuple:
                # Decoded columns have .name and .value attributes
                after[col.name] = col.value

        # Build before state from old_tuple (UPDATE/DELETE)
        before: Optional[Dict[str, Any]] = None
        if hasattr(message, "old_tuple") and message.old_tuple:
            before = {}
            for col in message.old_tuple:
                before[col.name] = col.value

        # Use current time as timestamp since pgoutput doesn't include it
        import time

        ts_ms = int(time.time() * 1000)

        return CDCEvent(
            table=table,
            schema=schema,
            operation=operation,
            before=before,
            after=after,
            source={"table": table, "schema": schema, "plugin": "pgoutput"},
            ts_ms=ts_ms,
        )

    def get_slot_lag(self) -> Optional[str]:
        """
        Get replication slot lag in human-readable format.

        Returns:
            Lag string like "16 MB" or None if unable to query
        """
        try:
            # Need a regular connection to query slot status
            import psycopg2 as regular_psycopg2

            check_conn = regular_psycopg2.connect(**self.config.to_connection_params())
            check_cur = check_conn.cursor()

            check_cur.execute(
                """
                SELECT pg_size_pretty(
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                ) as lag
                FROM pg_replication_slots 
                WHERE slot_name = %s
            """,
                (self.config.slot_name,),
            )

            result = check_cur.fetchone()
            check_cur.close()
            check_conn.close()

            return result[0] if result else None

        except Exception as e:
            logger.warning(f"Failed to get slot lag: {e}")
            return None
