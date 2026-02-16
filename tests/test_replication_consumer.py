"""Tests for replication consumer."""

import pytest
from unittest.mock import Mock, MagicMock, patch

from cdc_dependency_tracker.replication_consumer import ReplicationConsumer
from cdc_dependency_tracker.config import ReplicationConfig


@pytest.fixture
def replication_config():
    """Sample replication configuration."""
    return ReplicationConfig(
        enabled=True,
        slot_name="test_slot",
        plugin="pgoutput",
        host="localhost",
        port=5432,
        dbname="testdb",
        user="testuser",
        password="testpass",
        ack_interval_seconds=10,
        max_batch_size=100,
    )


@pytest.fixture
def mock_routing_engine():
    """Mock routing engine."""
    engine = Mock()
    engine.handle_event = Mock(return_value=Mock(immediate=1, deferred=0, queries=1, skipped=False))
    return engine


@pytest.fixture
def consumer(replication_config, mock_routing_engine):
    """Create replication consumer with mocks."""
    return ReplicationConsumer(replication_config, mock_routing_engine)


class TestPgoutputParsing:
    """Test pgoutput message parsing."""

    def test_parse_insert(self, consumer):
        """Test parsing INSERT operation."""
        # Mock pypgoutput message for INSERT
        mock_message = Mock()
        mock_message.op = "I"
        mock_message.table_schema = "public"
        mock_message.table_name = "customers"

        # Mock new_tuple with columns
        mock_col1 = Mock()
        mock_col1.name = "_id"
        mock_col1.value = "C1"

        mock_col2 = Mock()
        mock_col2.name = "name"
        mock_col2.value = "Acme Corp"

        mock_col3 = Mock()
        mock_col3.name = "email"
        mock_col3.value = "acme@example.com"

        mock_message.new_tuple = [mock_col1, mock_col2, mock_col3]
        mock_message.old_tuple = None

        event = consumer._parse_pgoutput_message(mock_message)

        assert event.table == "customers"
        assert event.schema == "public"
        assert event.operation == "c"  # create
        assert event.after == {"_id": "C1", "name": "Acme Corp", "email": "acme@example.com"}
        assert event.before is None
        assert event.ts_ms > 0

    def test_parse_update(self, consumer):
        """Test parsing UPDATE operation."""
        # Mock pypgoutput message for UPDATE
        mock_message = Mock()
        mock_message.op = "U"
        mock_message.table_schema = "public"
        mock_message.table_name = "orders"

        # Mock new_tuple (updated values)
        mock_col1 = Mock()
        mock_col1.name = "_id"
        mock_col1.value = "O1"

        mock_col2 = Mock()
        mock_col2.name = "status"
        mock_col2.value = "shipped"

        mock_col3 = Mock()
        mock_col3.name = "total"
        mock_col3.value = 150.00

        mock_message.new_tuple = [mock_col1, mock_col2, mock_col3]

        # Mock old_tuple (previous values)
        mock_old_col1 = Mock()
        mock_old_col1.name = "_id"
        mock_old_col1.value = "O1"

        mock_old_col2 = Mock()
        mock_old_col2.name = "status"
        mock_old_col2.value = "pending"

        mock_old_col3 = Mock()
        mock_old_col3.name = "total"
        mock_old_col3.value = 100.00

        mock_message.old_tuple = [mock_old_col1, mock_old_col2, mock_old_col3]

        event = consumer._parse_pgoutput_message(mock_message)

        assert event.table == "orders"
        assert event.operation == "u"  # update
        assert event.after == {"_id": "O1", "status": "shipped", "total": 150.00}
        assert event.before == {"_id": "O1", "status": "pending", "total": 100.00}

    def test_parse_delete(self, consumer):
        """Test parsing DELETE operation."""
        # Mock pypgoutput message for DELETE
        mock_message = Mock()
        mock_message.op = "D"
        mock_message.table_schema = "public"
        mock_message.table_name = "products"

        # Mock old_tuple (deleted values)
        mock_col = Mock()
        mock_col.name = "_id"
        mock_col.value = "P1"

        mock_message.old_tuple = [mock_col]
        mock_message.new_tuple = None

        event = consumer._parse_pgoutput_message(mock_message)

        assert event.table == "products"
        assert event.operation == "d"  # delete
        assert event.after is None  # No after state for deletes
        assert event.before == {"_id": "P1"}  # old_tuple has the deleted row

    def test_parse_complex_columns(self, consumer):
        """Test parsing with various data types."""
        # Mock pypgoutput message with various data types
        mock_message = Mock()
        mock_message.op = "I"
        mock_message.table_schema = "public"
        mock_message.table_name = "test_table"

        mock_col1 = Mock()
        mock_col1.name = "id"
        mock_col1.value = 42

        mock_col2 = Mock()
        mock_col2.name = "count"
        mock_col2.value = 10

        mock_col3 = Mock()
        mock_col3.name = "price"
        mock_col3.value = 99.99

        mock_col4 = Mock()
        mock_col4.name = "active"
        mock_col4.value = True

        mock_col5 = Mock()
        mock_col5.name = "data"
        mock_col5.value = None

        mock_col6 = Mock()
        mock_col6.name = "created_at"
        mock_col6.value = "2026-02-11"

        mock_message.new_tuple = [mock_col1, mock_col2, mock_col3, mock_col4, mock_col5, mock_col6]
        mock_message.old_tuple = None

        event = consumer._parse_pgoutput_message(mock_message)

        assert event.after["id"] == 42
        assert event.after["count"] == 10
        assert event.after["price"] == 99.99
        assert event.after["active"] is True
        assert event.after["data"] is None
        assert event.after["created_at"] == "2026-02-11"


class TestConnectionParams:
    """Test connection parameter conversion."""

    def test_to_connection_params(self, replication_config):
        """Test conversion to psycopg2 connection params."""
        params = replication_config.to_connection_params()

        assert params["host"] == "localhost"
        assert params["port"] == 5432
        assert params["dbname"] == "testdb"
        assert params["user"] == "testuser"
        assert params["password"] == "testpass"


class TestConsumerLifecycle:
    """Test consumer connection lifecycle."""

    @patch("cdc_dependency_tracker.replication_consumer.LogicalReplicationStream")
    def test_connect(self, mock_reader_class, consumer):
        """Test establishing replication connection using custom decoder."""
        mock_reader = MagicMock()
        mock_reader_class.return_value = mock_reader

        consumer.connect()

        assert consumer.reader == mock_reader
        mock_reader_class.assert_called_once()
        # Verify it was called with connection parameters
        call_kwargs = mock_reader_class.call_args[1]
        assert call_kwargs["host"] == "localhost"
        assert call_kwargs["database"] == "testdb"
        assert call_kwargs["user"] == "testuser"

    def test_disconnect(self, consumer):
        """Test closing replication connection."""
        # Setup mock reader
        mock_reader = MagicMock()
        consumer.reader = mock_reader

        consumer.disconnect()

        mock_reader.close.assert_called_once()

    def test_disconnect_when_not_connected(self, consumer):
        """Test disconnect when no connection exists."""
        # Should not raise error
        consumer.disconnect()


class TestAutoCreation:
    """Test automatic creation of slot and publication."""

    @patch("cdc_dependency_tracker.replication_consumer.psycopg2.connect")
    def test_ensure_slot_exists_when_slot_exists(
        self, mock_connect, replication_config, mock_routing_engine
    ):
        """Test slot check when slot already exists."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)  # Slot exists
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        consumer = ReplicationConsumer(replication_config, mock_routing_engine)

        # Should not raise error
        consumer._ensure_slot_exists()

        # Verify slot was checked
        mock_cursor.execute.assert_called_once()
        assert "pg_replication_slots" in mock_cursor.execute.call_args[0][0]

    @patch("cdc_dependency_tracker.replication_consumer.psycopg2.connect")
    def test_ensure_slot_created_when_missing(
        self, mock_connect, replication_config, mock_routing_engine
    ):
        """Test slot creation when slot doesn't exist."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Slot doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        consumer = ReplicationConsumer(replication_config, mock_routing_engine)

        # Should create the slot
        consumer._ensure_slot_exists()

        # Verify slot was created
        assert mock_cursor.execute.call_count == 2
        create_call = mock_cursor.execute.call_args_list[1]
        assert "pg_create_logical_replication_slot" in create_call[0][0]

    @patch("cdc_dependency_tracker.replication_consumer.psycopg2.connect")
    def test_ensure_publication_exists_when_publication_exists(
        self, mock_connect, replication_config, mock_routing_engine
    ):
        """Test publication check when publication already exists."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)  # Publication exists
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        consumer = ReplicationConsumer(replication_config, mock_routing_engine)

        # Should not raise error
        consumer._ensure_publication_exists()

        # Verify publication was checked
        mock_cursor.execute.assert_called_once()
        assert "pg_publication" in mock_cursor.execute.call_args[0][0]

    @patch("cdc_dependency_tracker.replication_consumer.psycopg2.connect")
    def test_ensure_publication_created_with_tables(
        self, mock_connect, replication_config, mock_routing_engine
    ):
        """Test publication creation with specific tables."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Publication doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        tables = ["customers", "orders", "products"]
        consumer = ReplicationConsumer(replication_config, mock_routing_engine, tables=tables)

        # Should create the publication
        consumer._ensure_publication_exists()

        # Verify publication was created with tables
        assert mock_cursor.execute.call_count == 2
        create_call = mock_cursor.execute.call_args_list[1]
        assert "CREATE PUBLICATION" in create_call[0][0]
        assert "customers, orders, products" in create_call[0][0]

    @patch("cdc_dependency_tracker.replication_consumer.psycopg2.connect")
    def test_ensure_publication_created_all_tables(
        self, mock_connect, replication_config, mock_routing_engine
    ):
        """Test publication creation for all tables when no specific tables given."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # Publication doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        consumer = ReplicationConsumer(replication_config, mock_routing_engine, tables=[])

        # Should create the publication for all tables
        consumer._ensure_publication_exists()

        # Verify publication was created for all tables
        assert mock_cursor.execute.call_count == 2
        create_call = mock_cursor.execute.call_args_list[1]
        assert "CREATE PUBLICATION" in create_call[0][0]
        assert "ALL TABLES" in create_call[0][0]

    def test_auto_create_disabled(self, mock_routing_engine):
        """Test that auto-creation can be disabled."""
        config = ReplicationConfig(
            enabled=True,
            slot_name="test_slot",
            plugin="pgoutput",
            host="localhost",
            port=5432,
            dbname="testdb",
            user="testuser",
            password="testpass",
            auto_create_slot=False,
            auto_create_publication=False,
        )

        consumer = ReplicationConsumer(config, mock_routing_engine)

        # These should return immediately without doing anything
        consumer._ensure_slot_exists()  # Should not raise
        consumer._ensure_publication_exists()  # Should not raise


# Integration test would require actual PostgreSQL with pgoutput
# For now, we rely on unit tests and manual testing
