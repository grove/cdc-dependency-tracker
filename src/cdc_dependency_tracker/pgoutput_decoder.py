"""
Custom PostgreSQL pgoutput logical replication protocol decoder.

This module implements a pure Python decoder for PostgreSQL's pgoutput plugin,
avoiding the multiprocessing constraints of pypgoutput library.

Protocol reference:
https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""

import struct
import logging
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# PostgreSQL epoch: 2000-01-01 00:00:00 UTC
PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
PG_EPOCH_TIMESTAMP = int(PG_EPOCH.timestamp() * 1_000_000)

# Message type identifiers (first byte of each message)
MSG_BEGIN = ord('B')
MSG_COMMIT = ord('C')
MSG_ORIGIN = ord('O')
MSG_RELATION = ord('R')
MSG_TYPE = ord('Y')
MSG_INSERT = ord('I')
MSG_UPDATE = ord('U')
MSG_DELETE = ord('D')
MSG_TRUNCATE = ord('T')
MSG_STREAM_START = ord('S')
MSG_STREAM_STOP = ord('E')
MSG_STREAM_COMMIT = ord('c')
MSG_STREAM_ABORT = ord('A')

# Tuple data identifiers
TUPLE_NULL = ord('n')
TUPLE_UNCHANGED = ord('u')
TUPLE_TEXT = ord('t')


@dataclass
class ColumnMetadata:
    """Metadata for a single column."""
    name: str
    type_oid: int
    type_modifier: int
    
    
@dataclass
class RelationMetadata:
    """Metadata for a table (relation)."""
    relation_id: int
    namespace: str
    name: str
    replica_identity: str  # 'd' = default, 'n' = nothing, 'f' = full, 'i' = index
    columns: List[ColumnMetadata]


@dataclass
class TupleData:
    """Parsed tuple data (row)."""
    columns: Dict[str, Any]  # column_name -> value


@dataclass
class BeginMessage:
    """Transaction begin message."""
    final_lsn: int
    commit_time: int
    xid: int


@dataclass
class CommitMessage:
    """Transaction commit message."""
    flags: int
    commit_lsn: int
    end_lsn: int
    commit_time: int


@dataclass
class InsertMessage:
    """Insert operation message."""
    relation_id: int
    tuple_data: TupleData


@dataclass
class UpdateMessage:
    """Update operation message."""
    relation_id: int
    old_tuple_data: Optional[TupleData]
    new_tuple_data: TupleData


@dataclass
class DeleteMessage:
    """Delete operation message."""
    relation_id: int
    old_tuple_data: TupleData


class PgOutputDecoder:
    """
    Decoder for PostgreSQL pgoutput logical replication protocol.
    
    Parses binary messages from PostgreSQL logical replication stream
    and converts them to structured Python objects.
    """
    
    def __init__(self):
        self.relations: Dict[int, RelationMetadata] = {}  # Cache relation metadata
        
    def decode_message(self, data: bytes) -> Optional[Any]:
        """
        Decode a single pgoutput message.
        
        Args:
            data: Raw binary message data
            
        Returns:
            Decoded message object or None if message type is not handled
        """
        if not data:
            return None
            
        msg_type = data[0]
        payload = data[1:]
        
        try:
            if msg_type == MSG_BEGIN:
                return self._decode_begin(payload)
            elif msg_type == MSG_COMMIT:
                return self._decode_commit(payload)
            elif msg_type == MSG_RELATION:
                return self._decode_relation(payload)
            elif msg_type == MSG_INSERT:
                return self._decode_insert(payload)
            elif msg_type == MSG_UPDATE:
                return self._decode_update(payload)
            elif msg_type == MSG_DELETE:
                return self._decode_delete(payload)
            elif msg_type == MSG_ORIGIN:
                # Origin messages - can be ignored for most use cases
                return None
            elif msg_type == MSG_TYPE:
                # Type messages - can be ignored if we handle common types
                return None
            else:
                logger.debug(f"Unhandled message type: {chr(msg_type)} (0x{msg_type:02x})")
                return None
        except Exception as e:
            logger.error(f"Error decoding message type {chr(msg_type)}: {e}", exc_info=True)
            raise
    
    def _decode_begin(self, data: bytes) -> BeginMessage:
        """Decode BEGIN transaction message."""
        # BEGIN format: Lsn(8) + Timestamp(8) + Xid(4)
        final_lsn, commit_time, xid = struct.unpack('>QQI', data[:20])
        return BeginMessage(
            final_lsn=final_lsn,
            commit_time=commit_time,
            xid=xid
        )
    
    def _decode_commit(self, data: bytes) -> CommitMessage:
        """Decode COMMIT transaction message."""
        # COMMIT format: Flags(1) + LSN(8) + LSN(8) + Timestamp(8)
        flags = data[0]
        commit_lsn, end_lsn, commit_time = struct.unpack('>QQQ', data[1:25])
        return CommitMessage(
            flags=flags,
            commit_lsn=commit_lsn,
            end_lsn=end_lsn,
            commit_time=commit_time
        )
    
    def _decode_relation(self, data: bytes) -> RelationMetadata:
        """Decode RELATION metadata message."""
        pos = 0
        
        # Relation ID (4 bytes)
        relation_id = struct.unpack('>I', data[pos:pos+4])[0]
        pos += 4
        
        # Namespace (null-terminated string)
        namespace, pos = self._read_string(data, pos)
        
        # Relation name (null-terminated string)
        name, pos = self._read_string(data, pos)
        
        # Replica identity (1 byte)
        replica_identity = chr(data[pos])
        pos += 1
        
        # Number of columns (2 bytes)
        num_columns = struct.unpack('>H', data[pos:pos+2])[0]
        pos += 2
        
        # Parse column metadata
        columns = []
        for _ in range(num_columns):
            # Flags (1 byte) - indicates if column is part of key
            flags = data[pos]
            pos += 1
            
            # Column name (null-terminated string)
            col_name, pos = self._read_string(data, pos)
            
            # Type OID (4 bytes)
            type_oid = struct.unpack('>I', data[pos:pos+4])[0]
            pos += 4
            
            # Type modifier (4 bytes)
            type_modifier = struct.unpack('>i', data[pos:pos+4])[0]
            pos += 4
            
            columns.append(ColumnMetadata(
                name=col_name,
                type_oid=type_oid,
                type_modifier=type_modifier
            ))
        
        relation = RelationMetadata(
            relation_id=relation_id,
            namespace=namespace,
            name=name,
            replica_identity=replica_identity,
            columns=columns
        )
        
        # Cache the relation metadata
        self.relations[relation_id] = relation
        
        return relation
    
    def _decode_insert(self, data: bytes) -> InsertMessage:
        """Decode INSERT message."""
        pos = 0
        
        # Relation ID (4 bytes)
        relation_id = struct.unpack('>I', data[pos:pos+4])[0]
        pos += 4
        
        # Tuple identifier (1 byte) - should be 'N' for new tuple
        tuple_type = chr(data[pos])
        pos += 1
        
        if tuple_type != 'N':
            raise ValueError(f"Expected 'N' for new tuple in INSERT, got '{tuple_type}'")
        
        # Decode tuple data
        tuple_data, pos = self._decode_tuple_data(data, pos, relation_id)
        
        return InsertMessage(
            relation_id=relation_id,
            tuple_data=tuple_data
        )
    
    def _decode_update(self, data: bytes) -> UpdateMessage:
        """Decode UPDATE message."""
        pos = 0
        
        # Relation ID (4 bytes)
        relation_id = struct.unpack('>I', data[pos:pos+4])[0]
        pos += 4
        
        # Check for old tuple identifier
        old_tuple_data = None
        tuple_type = chr(data[pos])
        pos += 1
        
        if tuple_type in ['K', 'O']:  # K = old key, O = old tuple
            old_tuple_data, pos = self._decode_tuple_data(data, pos, relation_id)
            # Read next tuple type
            tuple_type = chr(data[pos])
            pos += 1
        
        # New tuple (must be present)
        if tuple_type != 'N':
            raise ValueError(f"Expected 'N' for new tuple in UPDATE, got '{tuple_type}'")
        
        new_tuple_data, pos = self._decode_tuple_data(data, pos, relation_id)
        
        return UpdateMessage(
            relation_id=relation_id,
            old_tuple_data=old_tuple_data,
            new_tuple_data=new_tuple_data
        )
    
    def _decode_delete(self, data: bytes) -> DeleteMessage:
        """Decode DELETE message."""
        pos = 0
        
        # Relation ID (4 bytes)
        relation_id = struct.unpack('>I', data[pos:pos+4])[0]
        pos += 4
        
        # Tuple identifier (1 byte) - 'K' for old key or 'O' for old tuple
        tuple_type = chr(data[pos])
        pos += 1
        
        if tuple_type not in ['K', 'O']:
            raise ValueError(f"Expected 'K' or 'O' for old tuple in DELETE, got '{tuple_type}'")
        
        # Decode tuple data
        old_tuple_data, pos = self._decode_tuple_data(data, pos, relation_id)
        
        return DeleteMessage(
            relation_id=relation_id,
            old_tuple_data=old_tuple_data
        )
    
    def _decode_tuple_data(self, data: bytes, pos: int, relation_id: int) -> Tuple[TupleData, int]:
        """
        Decode tuple data (column values).
        
        Args:
            data: Binary data
            pos: Current position in data
            relation_id: ID of the relation (to get column metadata)
            
        Returns:
            Tuple of (TupleData, new_position)
        """
        # Get relation metadata
        if relation_id not in self.relations:
            raise ValueError(f"Unknown relation ID: {relation_id}")
        
        relation = self.relations[relation_id]
        
        # Number of columns (2 bytes)
        num_columns = struct.unpack('>H', data[pos:pos+2])[0]
        pos += 2
        
        if num_columns != len(relation.columns):
            raise ValueError(
                f"Column count mismatch: expected {len(relation.columns)}, got {num_columns}"
            )
        
        # Parse each column value
        columns = {}
        for col_meta in relation.columns:
            # Column data type identifier (1 byte)
            col_type = chr(data[pos])
            pos += 1
            
            if col_type == 'n':  # NULL
                columns[col_meta.name] = None
            elif col_type == 'u':  # Unchanged (TOAST)
                # For unchanged columns, we don't have the value
                # In practice, this shouldn't happen for tracked changes
                columns[col_meta.name] = None
            elif col_type == 't':  # Text format
                # Length (4 bytes)
                value_len = struct.unpack('>I', data[pos:pos+4])[0]
                pos += 4
                
                # Value (bytes)
                value_bytes = data[pos:pos+value_len]
                pos += value_len
                
                # Decode based on type OID
                columns[col_meta.name] = self._decode_column_value(
                    value_bytes,
                    col_meta.type_oid
                )
            else:
                raise ValueError(f"Unknown column data type: '{col_type}'")
        
        return TupleData(columns=columns), pos
    
    def _decode_column_value(self, value_bytes: bytes, type_oid: int) -> Any:
        """
        Decode column value from bytes based on PostgreSQL type OID.
        
        Args:
            value_bytes: Raw bytes of the value
            type_oid: PostgreSQL type OID
            
        Returns:
            Decoded Python value
        """
        # Convert bytes to text (pgoutput sends text format for most types)
        text = value_bytes.decode('utf-8')
        
        # Common PostgreSQL type OIDs
        # Reference: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
        
        if type_oid == 16:  # bool
            return text == 't' or text == 'true'
        elif type_oid in [20, 21, 23]:  # int8, int2, int4
            return int(text)
        elif type_oid in [700, 701]:  # float4, float8
            return float(text)
        elif type_oid == 1700:  # numeric
            # Try to convert to int if possible, otherwise float
            try:
                if '.' in text:
                    return float(text)
                return int(text)
            except ValueError:
                return text
        elif type_oid == 1082:  # date
            return text  # Keep as string for simplicity
        elif type_oid in [1114, 1184]:  # timestamp, timestamptz
            return text  # Keep as string for simplicity
        elif type_oid == 1043:  # varchar
            return text
        elif type_oid in [25, 1042, 18]:  # text, char, bpchar
            return text
        elif type_oid == 17:  # bytea
            return value_bytes  # Return raw bytes
        elif type_oid == 2950:  # uuid
            return text
        elif type_oid == 114:  # json
            return text  # Let application parse JSON
        elif type_oid == 3802:  # jsonb
            return text  # Let application parse JSON
        else:
            # Unknown type - return as string
            logger.debug(f"Unknown type OID {type_oid}, returning as string")
            return text
    
    def _read_string(self, data: bytes, pos: int) -> Tuple[str, int]:
        """
        Read a null-terminated string from data.
        
        Args:
            data: Binary data
            pos: Current position in data
            
        Returns:
            Tuple of (string, new_position)
        """
        end = data.index(b'\x00', pos)
        string = data[pos:end].decode('utf-8')
        return string, end + 1
    
    def get_relation_info(self, relation_id: int) -> Optional[RelationMetadata]:
        """Get cached relation metadata by ID."""
        return self.relations.get(relation_id)
