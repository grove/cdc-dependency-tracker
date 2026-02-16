"""
Logical replication stream wrapper for PostgreSQL.

Provides a high-level interface for consuming CDC events from PostgreSQL
logical replication using the pgoutput-decoder library in a background thread.
"""

import asyncio
import logging
import threading
import queue
from typing import Iterator, Optional, Any
import pgoutput_decoder

logger = logging.getLogger(__name__)


class DecodedMessage:
    """
    Wrapper for decoded CDC messages with convenient attributes.
    
    Provides pypgoutput-compatible interface for backward compatibility.
    """
    
    def __init__(
        self,
        op: str,
        table_name: str,
        table_schema: str,
        new_tuple: Optional[list] = None,
        old_tuple: Optional[list] = None
    ):
        self.op = op  # 'I', 'U', 'D'
        self.table_name = table_name
        self.table_schema = table_schema
        self.new_tuple = new_tuple or []
        self.old_tuple = old_tuple or []


class Column:
    """Represents a column value in a tuple."""
    
    def __init__(self, name: str, value: any):
        self.name = name
        self.value = value
    
    def __repr__(self):
        return f"Column(name='{self.name}', value={self.value!r})"


def _async_reader_thread(
    message_queue: queue.Queue,
    stop_event: threading.Event,
    ready_event: threading.Event,
    publication_name: str,
    slot_name: str,
    host: str,
    database: str,
    port: int,
    user: str,
    password: str
):
    """
    Background thread that runs async pgoutput-decoder reader.
    
    Reads CDC messages from PostgreSQL and puts them in a queue
    for synchronous consumption.
    """
    async def async_reader():
        try:
            cdc_reader = pgoutput_decoder.LogicalReplicationReader(
                publication_name=publication_name,
                slot_name=slot_name,
                host=host,
                database=database,
                port=port,
                user=user,
                password=password,
            )
            
            logger.info(f"Started async CDC reader for slot '{slot_name}', publication '{publication_name}'")
            ready_event.set()  # Signal that we're ready
            
            async for message in cdc_reader:
                if stop_event.is_set():
                    break
                
                # Convert pgoutput-decoder message to DecodedMessage
                decoded = _convert_message(message)
                message_queue.put(decoded)
                
            await cdc_reader.stop()
            
        except Exception as e:
            logger.error(f"Error in async CDC reader: {e}", exc_info=True)
            message_queue.put(e)  # Put exception in queue
            ready_event.set()  # Signal ready even on error
    
    # Create new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(async_reader())
    finally:
        loop.close()


def _convert_message(msg) -> DecodedMessage:
    """
    Convert pgoutput-decoder message to DecodedMessage.
    
    Args:
        msg: ReplicationMessage from pgoutput-decoder
        
    Returns:
        DecodedMessage with compatible interface
    """
    # Map operation codes: "c" -> "I", "u" -> "U", "d" -> "D"
    op_map = {"c": "I", "u": "U", "d": "D"}
    op = op_map.get(msg.op, msg.op)
    
    table_name = msg.source.get("table", "")
    table_schema = msg.source.get("schema", "public")
    
    # Convert after/before dicts to list of Column objects
    new_tuple = []
    if msg.after:
        new_tuple = [Column(k, v) for k, v in msg.after.items()]
    
    old_tuple = []  
    if msg.before:
        old_tuple = [Column(k, v) for k, v in msg.before.items()]
    
    return DecodedMessage(
        op=op,
        table_name=table_name,
        table_schema=table_schema,
        new_tuple=new_tuple,
        old_tuple=old_tuple
    )


class LogicalReplicationStream:
    """
    Manages PostgreSQL logical replication connection and message decoding.
    
    Uses pgoutput-decoder library in a background thread with async event loop.
    Provides a synchronous iterator interface for consuming CDC events.
    """
    
    def __init__(
        self,
        publication_name: str,
        slot_name: str,
        host: str,
        database: str,
        port: str,
        user: str,
        password: str,
        options: Optional[dict] = None
    ):
        """
        Initialize logical replication stream.
        
        Args:
            publication_name: PostgreSQL publication name
            slot_name: Replication slot name
            host: Database host
            database: Database name
            port: Database port
            user: Database user
            password: Database password
            options: Additional replication options (unused, for compatibility)
        """
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.host = host
        self.database = database
        self.port = int(port)
        self.user = user
        self.password = password
        
        self._message_queue: queue.Queue = queue.Queue(maxsize=1000)
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None
        self._connected = False
        
    def connect(self):
        """Establish logical replication connection (no-op for compatibility)."""
        pass
        
    def start_replication(self):
        """Start consuming from the replication slot in background thread."""
        if self._connected:
            return
        
        self._reader_thread = threading.Thread(
            target=_async_reader_thread,
            args=(
                self._message_queue,
                self._stop_event,
                self._ready_event,
                self.publication_name,
                self.slot_name,
                self.host,
                self.database,
                self.port,
                self.user,
                self.password
            ),
            daemon=True
        )
        self._reader_thread.start()
        
        # Wait for reader to be ready
        if not self._ready_event.wait(timeout=10):
            raise RuntimeError("CDC reader failed to start within 10 seconds")
        
        # Check if there was an error during startup
        try:
            item = self._message_queue.get(timeout=0.1)
            if isinstance(item, Exception):
                raise item
            # Put it back if it's a valid message
            self._message_queue.put(item)
        except queue.Empty:
            pass  # No error, continue
        
        self._connected = True
        logger.info(f"Started replication from slot '{self.slot_name}'")
    
    def __iter__(self) -> Iterator[DecodedMessage]:
        """
        Iterate over CDC messages from the replication stream.
        
        Yields:
            DecodedMessage objects with operation details
        """
        if not self._connected:
            self.start_replication()
        
        try:
            while not self._stop_event.is_set():
                try:
                    # Get message from queue with timeout
                    message = self._message_queue.get(timeout=0.1)
                    
                    # Check if it's an exception from the reader thread
                    if isinstance(message, Exception):
                        raise message
                    
                    yield message
                    
                except queue.Empty:
                    # No message available, continue
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Replication interrupted")
            self.stop()
            raise
        except Exception as e:
            logger.error(f"Error during replication: {e}", exc_info=True)
            self.stop()
            raise
    
    def stop(self):
        """Stop the background reader thread."""
        if self._connected:
            logger.info("Stopping CDC reader thread...")
            self._stop_event.set()
            
            if self._reader_thread and self._reader_thread.is_alive():
                self._reader_thread.join(timeout=5)
            
            self._connected = False
            logger.info("CDC reader stopped")
    
    def close(self):
        """Close replication connection (alias for stop)."""
        self.stop()
    
    def __enter__(self):
        """Context manager entry."""
        self.start_replication()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
