"""
Logical replication stream wrapper for PostgreSQL.

Provides a high-level interface for consuming CDC events from PostgreSQL
logical replication using the custom pgoutput decoder.
"""

import logging
import struct
from typing import Iterator, Optional
import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from .pgoutput_decoder import (
    PgOutputDecoder,
    InsertMessage,
    UpdateMessage,
    DeleteMessage,
    RelationMetadata,
    BeginMessage,
    CommitMessage
)

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


class LogicalReplicationStream:
    """
    Manages PostgreSQL logical replication connection and message decoding.
    
    Provides an iterator interface for consuming CDC events.
    Compatible with pypgoutput.LogicalReplicationReader API.
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
            options: Additional replication options
        """
        self.publication_name = publication_name
        self.slot_name = slot_name
        self.host = host
        self.database = database
        self.port = int(port)
        self.user = user
        self.password = password
        self.options = options or {}
        
        self.decoder = PgOutputDecoder()
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None
        self._connected = False
        
    def connect(self):
        """Establish logical replication connection."""
        if self._connected:
            return
        
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                connection_factory=LogicalReplicationConnection
            )
            self.cursor = self.conn.cursor()
            self._connected = True
            logger.info(f"Connected to replication slot '{self.slot_name}' with publication '{self.publication_name}'")
        except Exception as e:
            logger.error(f"Failed to connect for replication: {e}")
            raise
    
    def start_replication(self):
        """Start consuming from the replication slot."""
        if not self._connected:
            self.connect()
        
        # Build options for pgoutput plugin
        options = {
            'proto_version': '1',
            'publication_names': self.publication_name,
        }
        options.update(self.options)
        
        try:
            self.cursor.start_replication(
                slot_name=self.slot_name,
                decode=False,  # We'll decode manually
                options=options  # Pass as dict
            )
            logger.info(f"Started replication from slot '{self.slot_name}'")
        except Exception as e:
            logger.error(f"Failed to start replication: {e}", exc_info=True)
            raise
    
    def __iter__(self) -> Iterator[DecodedMessage]:
        """
        Iterate over CDC messages from the replication stream.
        
        Yields:
            DecodedMessage objects with operation details
        """
        if not self._connected:
            self.start_replication()
        
        try:
            # With decode=False, we need to use read_message() in a loop
            # Use select() to implement timeout so thread can be interrupted
            import select
            
            while True:
                # Check if there's data available with timeout
                if select.select([self.cursor], [], [], 0.1) == ([], [],  []):
                    continue
                
                msg = self.cursor.read_message()
                
                if msg is None:
                    continue
                
                # msg.payload contains binary data
                # msg.data_start contains LSN for acknowledgment
                # msg.cursor is the cursor for sending feedback
                
                decoded = self.decoder.decode_message(msg.payload)
                
                if decoded is None:
                    # Non-DML message (Begin, Commit, Relation, etc.)
                    # We need Relation messages to build metadata cache
                    # But we don't yield them
                    pass
                elif isinstance(decoded, InsertMessage):
                    yield self._convert_insert(decoded)
                elif isinstance(decoded, UpdateMessage):
                    yield self._convert_update(decoded)
                elif isinstance(decoded, DeleteMessage):
                    yield self._convert_delete(decoded)
                
                # Send feedback to keep connection alive
                msg.cursor.send_feedback(flush_lsn=msg.data_start)
                
        except KeyboardInterrupt:
            logger.info("Replication interrupted")
            raise
        except Exception as e:
            logger.error(f"Error during replication: {e}", exc_info=True)
            raise
    
    def _convert_insert(self, msg: InsertMessage) -> DecodedMessage:
        """Convert InsertMessage to DecodedMessage."""
        relation = self.decoder.get_relation_info(msg.relation_id)
        if not relation:
            raise ValueError(f"Unknown relation ID: {msg.relation_id}")
        
        # Convert dict to list of Column objects
        new_tuple = [
            Column(name, msg.tuple_data.columns.get(name))
            for name in [col.name for col in relation.columns]
        ]
        
        return DecodedMessage(
            op='I',
            table_name=relation.name,
            table_schema=relation.namespace,
            new_tuple=new_tuple,
            old_tuple=[]
        )
    
    def _convert_update(self, msg: UpdateMessage) -> DecodedMessage:
        """Convert UpdateMessage to DecodedMessage."""
        relation = self.decoder.get_relation_info(msg.relation_id)
        if not relation:
            raise ValueError(f"Unknown relation ID: {msg.relation_id}")
        
        # Convert new tuple
        new_tuple = [
            Column(name, msg.new_tuple_data.columns.get(name))
            for name in [col.name for col in relation.columns]
        ]
        
        # Convert old tuple if present
        old_tuple = []
        if msg.old_tuple_data:
            old_tuple = [
                Column(name, msg.old_tuple_data.columns.get(name))
                for name in [col.name for col in relation.columns]
            ]
        
        return DecodedMessage(
            op='U',
            table_name=relation.name,
            table_schema=relation.namespace,
            new_tuple=new_tuple,
            old_tuple=old_tuple
        )
    
    def _convert_delete(self, msg: DeleteMessage) -> DecodedMessage:
        """Convert DeleteMessage to DecodedMessage."""
        relation = self.decoder.get_relation_info(msg.relation_id)
        if not relation:
            raise ValueError(f"Unknown relation ID: {msg.relation_id}")
        
        # Convert old tuple
        old_tuple = [
            Column(name, msg.old_tuple_data.columns.get(name))
            for name in [col.name for col in relation.columns]
        ]
        
        return DecodedMessage(
            op='D',
            table_name=relation.name,
            table_schema=relation.namespace,
            new_tuple=[],
            old_tuple=old_tuple
        )
    
    def close(self):
        """Close replication connection."""
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass
            self.cursor = None
        
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
        
        self._connected = False
        logger.info("Closed replication connection")
    
    def __enter__(self):
        """Context manager entry."""
        self.start_replication()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
