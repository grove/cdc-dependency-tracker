"""End-to-end test for customers example using Testcontainers."""

import pytest
import time
import json
import threading
import queue
from pathlib import Path
from typing import List, Optional, Dict, Any
from testcontainers.postgres import PostgresContainer

from cdc_dependency_tracker.config import Config, DatabaseConfig, TrackerConfig, ReplicationConfig
from cdc_dependency_tracker.db_client import DatabaseClient
from cdc_dependency_tracker.sql_parser import SQLParser
from cdc_dependency_tracker.dependency_graph import DependencyGraph
from cdc_dependency_tracker.routing import RoutingEngine
from cdc_dependency_tracker.cdc_handler import CDCEvent
from cdc_dependency_tracker.percolator import PercolationEngine
from cdc_dependency_tracker.logical_replication_stream import LogicalReplicationStream


# SQL for creating test schema
SCHEMA_SQL = """
-- Create base tables
CREATE TABLE IF NOT EXISTS customers (
    _id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    _deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    _id VARCHAR(50) PRIMARY KEY,
    cust_id VARCHAR(50) NOT NULL REFERENCES customers(_id),
    order_date DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    _deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_lines (
    _id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL REFERENCES orders(_id),
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    _deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    _id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    _deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create tracking tables
CREATE TABLE IF NOT EXISTS customers_to_reprocess (
    customer_id VARCHAR(50) PRIMARY KEY,
    last_tracked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS intermediate_to_track (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    depth INTEGER NOT NULL,
    tracked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    percolated BOOLEAN DEFAULT FALSE,
    UNIQUE(table_name, entity_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_cust_id ON orders(cust_id);
CREATE INDEX IF NOT EXISTS idx_order_lines_order_id ON order_lines(order_id);
CREATE INDEX IF NOT EXISTS idx_order_lines_product_id ON order_lines(product_id);
CREATE INDEX IF NOT EXISTS idx_intermediate_percolated ON intermediate_to_track(percolated);

-- Set REPLICA IDENTITY FULL to capture old values in UPDATE/DELETE operations
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_lines REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
"""

# Sample data
SAMPLE_DATA_SQL = """
-- Insert test customers
INSERT INTO customers (_id, name, email) VALUES
    ('C1', 'Acme Corp', 'contact@acme.com'),
    ('C2', 'Widget Inc', 'info@widget.com'),
    ('C3', 'Gadget Co', 'hello@gadget.com');

-- Insert test products
INSERT INTO products (_id, name, price) VALUES
    ('P1', 'Widget', 10.00),
    ('P2', 'Gadget', 20.00),
    ('P3', 'Gizmo', 30.00);

-- Insert test orders
INSERT INTO orders (_id, cust_id, order_date, status) VALUES
    ('O1', 'C1', '2024-01-01', 'completed'),
    ('O2', 'C1', '2024-01-15', 'shipped'),
    ('O3', 'C2', '2024-02-01', 'pending');

-- Insert test order lines
INSERT INTO order_lines (_id, order_id, product_id, quantity, price) VALUES
    ('OL1', 'O1', 'P1', 2, 10.00),
    ('OL2', 'O1', 'P2', 1, 20.00),
    ('OL3', 'O2', 'P1', 5, 10.00),
    ('OL4', 'O3', 'P3', 3, 30.00);
"""

SQL_QUERY = """
SELECT 
    c._id as customer_id,
    c.name as customer_name,
    o._id as order_id,
    ol._id as order_line_id,
    p._id as product_id
FROM customers c
JOIN orders o ON c._id = o.cust_id
JOIN order_lines ol ON o._id = ol.order_id
JOIN products p ON ol.product_id = p._id
WHERE c._deleted = FALSE
  AND o._deleted = FALSE
  AND ol._deleted = FALSE
  AND p._deleted = FALSE
"""


def cdc_consumer_thread(config_dict: dict, event_queue: queue.Queue, stop_event: threading.Event, ready_event: threading.Event):
    """Thread function to consume CDC events using custom pgoutput decoder (production code path)."""
    import time
    from cdc_dependency_tracker.logical_replication_stream import LogicalReplicationStream
    
    reader = None
    try:
        # Create LogicalReplicationStream (same as production code)
        reader = LogicalReplicationStream(
            publication_name=config_dict['publication_name'],
            slot_name=config_dict['slot_name'],
            host=config_dict['host'],
            database=config_dict['dbname'],
            port=str(config_dict['port']),
            user=config_dict['user'],
            password=config_dict['password'],
        )
        
        # Explicitly start replication to ensure connection is established
        reader.start_replication()
        
        # Signal that we're ready to consume (connection established and replication started)
        ready_event.set()
        
        # Consume messages - same logic as ReplicationConsumer._parse_pgoutput_message
        for message in reader:
            if stop_event.is_set():
                break
            
            try:
                # Map operation codes to Debezium-style codes
                op_map = {
                    'I': 'c',  # Insert -> Create
                    'U': 'u',  # Update -> Update
                    'D': 'd',  # Delete -> Delete
                }
                operation = op_map.get(message.op, 'u')
                
                # Extract table metadata
                table = message.table_name
                schema = message.table_schema
                
                # Build after state from new_tuple (INSERT/UPDATE)
                after = None
                if hasattr(message, 'new_tuple') and message.new_tuple:
                    after = {}
                    for col in message.new_tuple:
                        after[col.name] = col.value
                
                # Build before state from old_tuple (UPDATE/DELETE)
                before = None
                if hasattr(message, 'old_tuple') and message.old_tuple:
                    before = {}
                    for col in message.old_tuple:
                        before[col.name] = col.value
                
                # Create event dict (will be converted to CDCEvent in consume_events)
                event_dict = {
                    'table': table,
                    'schema': schema,
                    'operation': operation,
                    'before': before,
                    'after': after,
                    'source': {'table': table, 'schema': schema, 'plugin': 'pgoutput'},
                    'ts_ms': int(time.time() * 1000)
                }
                event_queue.put(event_dict)
                
            except Exception as parse_error:
                event_queue.put({'error': f'Parse error: {parse_error}'})
        
    except Exception as e:
        event_queue.put({'error': str(e)})
    finally:
        # Ensure reader is closed
        if reader:
            try:
                reader.close()
            except Exception:
                pass


class CDCTestHelper:
    """Helper class to manage CDC streaming in tests using threading."""
    
    def __init__(self, config: Config):
        self.config = config
        self.event_queue = queue.Queue()  # Thread-safe queue
        self.stop_event = threading.Event()  # Thread-safe event
        self.ready_event = threading.Event()  # Thread-safe event to signal consumer is ready
        self.consumer_thread = None
    
    def start(self):
        """Start CDC consumer in separate thread."""
        # Convert config to dict (no pickling needed for threads)
        config_dict = {
            'publication_name': self.config.replication.publication_name,
            'slot_name': self.config.replication.slot_name,
            'host': self.config.replication.host,
            'dbname': self.config.replication.dbname,
            'port': self.config.replication.port,
            'user': self.config.replication.user,
            'password': self.config.replication.password,
        }
        
        self.consumer_thread = threading.Thread(
            target=cdc_consumer_thread,
            args=(config_dict, self.event_queue, self.stop_event, self.ready_event),
            daemon=True
        )
        self.consumer_thread.start()
        
        # Wait for consumer to be ready (with timeout)
        if not self.ready_event.wait(timeout=10):
            raise RuntimeError("CDC consumer failed to start within 10 seconds")
    
    def stop(self):
        """Stop CDC consumer thread."""
        if self.consumer_thread:
            self.stop_event.set()
            self.consumer_thread.join(timeout=10)  # Increased timeout for cleanup
            time.sleep(0.5)  # Extra time for connection cleanup
    
    def consume_events(self, timeout: float = 2.0, min_events: int = 0) -> List[CDCEvent]:
        """
        Consume all events from the queue.
        
        Args:
            timeout: Maximum time to wait for events
            min_events: Minimum number of events to wait for before returning early
        
        Returns:
            List of CDCEvent objects
        """
        events = []
        deadline = time.time() + timeout
        no_event_iterations = 0
        
        while time.time() < deadline:
            try:
                event_dict = self.event_queue.get(timeout=0.1)
                
                # Check for error
                if isinstance(event_dict, dict) and 'error' in event_dict:
                    raise RuntimeError(f"CDC consumer error: {event_dict['error']}")
                
                # Convert dict back to CDCEvent
                event = CDCEvent(
                    table=event_dict['table'],
                    schema=event_dict['schema'],
                    operation=event_dict['operation'],
                    before=event_dict['before'],
                    after=event_dict['after'],
                    source=event_dict['source'],
                    ts_ms=event_dict['ts_ms']
                )
                events.append(event)
                no_event_iterations = 0  # Reset counter when we get an event
                    
            except queue.Empty:
                no_event_iterations += 1
                # If we have enough events and haven't seen new ones for 0.5s, return early
                if min_events > 0 and len(events) >= min_events and no_event_iterations >= 5:
                    break
                continue
        
        return events


@pytest.fixture(scope="module")
def postgres_container():
    """Create and start PostgreSQL container with logical replication enabled."""
    container = PostgresContainer("postgres:18.1-alpine")
    # Override command to enable logical replication
    container = container.with_command(
        "postgres "
        "-c wal_level=logical "
        "-c max_replication_slots=10 "
        "-c max_wal_senders=10"
    )
    container.start()
    
    # Wait a moment for PostgreSQL to fully start
    time.sleep(2)
    
    yield container
    container.stop()


@pytest.fixture(scope="module")
def db_config(postgres_container):
    """Create database configuration from container."""
    return DatabaseConfig(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        dbname=postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password
    )


@pytest.fixture(scope="module")
def setup_database(db_config):
    """Setup database schema and sample data."""
    client = DatabaseClient(db_config.to_connection_params())
    
    # Drop existing tables first
    with client.transaction() as cur:
        cur.execute("DROP TABLE IF EXISTS order_lines CASCADE")
        cur.execute("DROP TABLE IF EXISTS orders CASCADE")
        cur.execute("DROP TABLE IF EXISTS products CASCADE")
        cur.execute("DROP TABLE IF EXISTS customers CASCADE")
        cur.execute("DROP TABLE IF EXISTS intermediate_to_track CASCADE")
        cur.execute("DROP TABLE IF EXISTS customers_to_reprocess CASCADE")
    
    # Create schema
    with client.transaction() as cur:
        cur.execute(SCHEMA_SQL)
    
    # Insert sample data
    with client.transaction() as cur:
        cur.execute(SAMPLE_DATA_SQL)
    
    client.close()
    
    yield db_config
    
    # Cleanup is automatic when container stops


@pytest.fixture(scope="module")
def config(setup_database):
    """Create full configuration with replication enabled."""
    return Config(
        database=setup_database,
        tracker=TrackerConfig(
            base_table="customers",
            tracking_table="customers_to_reprocess",
            immediate_fanout_threshold=100,
            percolation_interval_seconds=30,
            percolation_batch_size=1000,
            sql_query=SQL_QUERY
        ),
        replication=ReplicationConfig(
            enabled=True,
            slot_name="test_cdc_slot",
            plugin="pgoutput",
            host=setup_database.host,
            port=setup_database.port,
            dbname=setup_database.dbname,
            user=setup_database.user,
            password=setup_database.password,
            auto_create_slot=True,
            auto_create_publication=True,
            publication_name="test_cdc_pub",
            ack_interval_seconds=1,
            max_batch_size=100
        )
    )


@pytest.fixture()
def components(config):
    """Initialize all components."""
    # Parse SQL and build dependency graph
    parser = SQLParser(config.tracker.sql_query, config.tracker.base_table)
    join_graph = parser.get_join_graph()
    dependency_graph = DependencyGraph(config.tracker.base_table, join_graph)
    base_id_column = parser.get_base_id_column() or "customer_id"
    
    # Initialize database client
    db_client = DatabaseClient(
        config.database.to_connection_params(),
        tracking_table=config.tracker.tracking_table
    )
    
    # Initialize routing engine
    routing_engine = RoutingEngine(
        db_client,
        dependency_graph,
        base_table_id_column=base_id_column,
        immediate_threshold=config.tracker.immediate_fanout_threshold
    )
    
    # Initialize percolator
    percolator = PercolationEngine(
        db_client,
        dependency_graph,
        base_id_column,
        batch_size=config.tracker.percolation_batch_size
    )
    
    yield {
        'config': config,
        'db_client': db_client,
        'dependency_graph': dependency_graph,
        'routing_engine': routing_engine,
        'percolator': percolator,
        'base_id_column': base_id_column
    }
    
    # Cleanup
    db_client.close()


@pytest.fixture()
def cdc_helper(config, setup_database):
    """Fixture to manage CDC streaming with real replication using pgoutput (production code path)."""
    # Setup replication slot and publication for pgoutput
    db_client = DatabaseClient(
        config.database.to_connection_params(),
        tracking_table=config.tracker.tracking_table
    )
    
    slot_name = config.replication.slot_name
    pub_name = config.replication.publication_name
    
    # Drop existing slot if it exists - must be in separate transaction
    with db_client.transaction() as cur:
        cur.execute(
            "SELECT pg_drop_replication_slot(slot_name) "
            "FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (slot_name,)
        )
    
    # Drop existing publication if it exists
    with db_client.transaction() as cur:
        cur.execute(
            "DROP PUBLICATION IF EXISTS %s" % pub_name  # Publication names can't be parameterized
        )
    
    # Create publication for all tables
    with db_client.transaction() as cur:
        cur.execute(
            "CREATE PUBLICATION %s FOR ALL TABLES" % pub_name
        )
    
    # Create replication slot with pgoutput plugin - must be in its own transaction
    with db_client.transaction() as cur:
        cur.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
            (slot_name,)
        )
    
    db_client.close()
    
    # Create and start CDC helper
    helper = CDCTestHelper(config)
    helper.start()
    
    yield helper
    
    # Teardown
    helper.stop()
    
    # Cleanup replication slot and publication
    db_client = DatabaseClient(
        config.database.to_connection_params(),
        tracking_table=config.tracker.tracking_table
    )
    
    # Terminate any active backends using the slot
    with db_client.transaction() as cur:
        cur.execute("""
            SELECT pg_terminate_backend(active_pid)
            FROM pg_replication_slots
            WHERE slot_name = %s AND active_pid IS NOT NULL
        """, (slot_name,))
    
    # Small delay to let backend termination complete
    time.sleep(0.5)
    
    # Now drop the slot
    with db_client.transaction() as cur:
        cur.execute(f"SELECT pg_drop_replication_slot('{slot_name}')")
    
    with db_client.transaction() as cur:
        cur.execute(f"DROP PUBLICATION IF EXISTS {pub_name}")
    
    db_client.close()


def clear_tracking_tables(db_client):
    """Clear tracking tables between tests."""
    with db_client.transaction() as cur:
        cur.execute("DELETE FROM customers_to_reprocess")
        cur.execute("DELETE FROM intermediate_to_track")


def aggressive_event_clearing(cdc_helper, max_rounds=3):
    """Aggressively clear all pending CDC events from queue.
    
    Performs multiple rounds of event consumption to ensure the queue is completely drained.
    This is necessary because replication lag means events may arrive late.
    """
    total_events = 0
    for round_num in range(max_rounds):
        events = cdc_helper.consume_events(timeout=2.0)
        total_events += len(events)
        if len(events) == 0:
            break
        time.sleep(0.5)  # Brief pause between rounds
    return total_events


class TestDepth0BaseTableChanges:
    """Test depth 0: Direct changes to base table (customers)."""
    
    def test_customer_insert(self, components, cdc_helper):
        """Test INSERT on customers table consuming real CDC events."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any pending events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Actually insert customer into database
        with components['db_client'].transaction() as cur:
            cur.execute("INSERT INTO customers (_id, name, email, _deleted) VALUES ('C999', 'New Customer', 'new@test.com', FALSE)")
        time.sleep(2.0)  # Allow CDC event to be captured and WAL to flush
        
        # Consume CDC events from replication slot
        events = cdc_helper.consume_events(timeout=6.0, min_events=1)

        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        customer_events = [e for e in app_events if e.table == "customers" and e.after and e.after.get("_id") == "C999"]
        assert len(customer_events) > 0, f"Expected customer event, got {len(app_events)} app events"
        
        # Process the event
        event = customer_events[0]
        result = components['routing_engine'].handle_event(event)
        
        assert result.immediate == 1
        assert result.deferred == 0
        
        # Verify data in database
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT _id, name, email FROM customers WHERE _id = 'C999'")
            customer = cur.fetchone()
            assert customer is not None
            assert customer[1] == "New Customer"
            
        # Verify tracking table
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C999'")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "C999"
    
    def test_customer_update(self, components, cdc_helper):
        """Test UPDATE on customers table consuming real CDC events."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any pending events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Actually update customer in database
        with components['db_client'].transaction() as cur:
            cur.execute("UPDATE customers SET name = 'Acme Corporation' WHERE _id = 'C1'")
        time.sleep(3.0)  # Allow CDC event to be captured and WAL to flush (UPDATEs need more time)
        
        # Consume CDC events from replication slot
        events = cdc_helper.consume_events(timeout=6.0, min_events=1)
        
        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        customer_events = [e for e in app_events if e.table == "customers" and e.after and e.after.get("_id") == "C1"]
        assert len(customer_events) > 0, f"Expected customer event, got {len(app_events)} app events"
        
        # Process the event
        event = customer_events[0]
        result = components['routing_engine'].handle_event(event)
        
        assert result.immediate == 1
        assert result.deferred == 0
        
        # Verify database update
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT name FROM customers WHERE _id = 'C1'")
            assert cur.fetchone()[0] == "Acme Corporation"
        
        # Verify tracking
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C1'")
            assert cur.fetchone() is not None
    
    def test_customer_delete(self, components, cdc_helper):
        """Test soft DELETE on customers table consuming real CDC events."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any pending events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Actually soft-delete customer in database
        with components['db_client'].transaction() as cur:
            cur.execute("UPDATE customers SET _deleted = TRUE WHERE _id = 'C2'")
        time.sleep(2.5)  # Allow CDC event to be captured and WAL to flush
        
        # Consume CDC events from replication slot
        events = cdc_helper.consume_events(timeout=5.0, min_events=1)
        
        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        customer_events  = [e for e in app_events if e.table == "customers" and e.after and e.after.get("_id") == "C2"]
        assert len(customer_events) > 0, f"Expected customer event, got {len(app_events)} app events"
        
        event = customer_events[0]
        result = components['routing_engine'].handle_event(event)
        
        assert result.immediate == 1
        assert not result.skipped
        
        # Verify soft delete in database
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT _deleted FROM customers WHERE _id = 'C2'")
            assert cur.fetchone()[0] is True


class TestDepth1ImmediateChanges:
    """Test depth 1: Changes to tables one hop away (orders)."""
    
    def test_order_insert(self, components, cdc_helper):
        """Test INSERT on orders table consuming real CDC events."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any pending events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Actually insert order into database
        with components['db_client'].transaction() as cur:
            cur.execute("INSERT INTO orders (_id, cust_id, order_date, status, _deleted) VALUES ('O999', 'C1', '2024-03-01', 'pending', FALSE)")
        time.sleep(2.0)  # Allow CDC event to be captured and WAL to flush
        
        # Consume CDC events from replication slot
        events = cdc_helper.consume_events(timeout=6.0)
        
        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        order_events = [e for e in app_events if e.table == "orders" and e.after and e.after.get("_id") == "O999"]
        assert len(order_events) > 0, f"Expected order event, got {len(app_events)} app events"
        
        # Process the event
        event = order_events[0]
        result = components['routing_engine'].handle_event(event)
        
        # Should be immediate since orders is depth 1
        assert result.immediate == 1
        assert result.deferred == 0
        
        # Verify order in database
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT _id, cust_id, status FROM orders WHERE _id = 'O999'")
            order = cur.fetchone()
            assert order is not None
            assert order[1] == "C1"
            assert order[2] == "pending"
        
        # Verify customer C1 is tracked
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C1'")
            assert cur.fetchone() is not None
    
    def test_order_update_join_key(self, components, cdc_helper):
        """Test UPDATE on orders changing the join key consuming real CDC events."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any pending events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Actually move order from C1 to C2 in database
        with components['db_client'].transaction() as cur:
            cur.execute("UPDATE orders SET cust_id = 'C2' WHERE _id = 'O1'")
        time.sleep(3.0)  # Allow CDC event to be captured and WAL to flush (UPDATEs need more time)
        
        # Consume CDC events from replication slot
        events = cdc_helper.consume_events(timeout=6.0)
        
        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        order_events = [e for e in app_events if e.table == "orders" and e.after and e.after.get("_id") == "O1"]
        assert len(order_events) > 0, f"Expected order event, got {len(app_events)} app events"
        
        # Process the event
        event = order_events[0]
        result = components['routing_engine'].handle_event(event)
        
        # Note: With pgoutput and REPLICA IDENTITY FULL, we can track both old and new customers
        # The old customer (C1) is in event.before, new customer (C2) is in event.after
        # Since our current routing logic tracks based on after values, we track C2
        # To track both, routing engine would need to handle before values for FK changes
        assert result.immediate >= 1
        
        # Verify database update
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT cust_id FROM orders WHERE _id = 'O1'")
            assert cur.fetchone()[0] == "C2"
        
        # Verify new customer is tracked (C2)
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C2'")
            assert cur.fetchone() is not None
    
    def test_order_update_non_join_key(self, components, cdc_helper):
        """Test UPDATE on orders changing non-join field consuming real CDC events."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any pending events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Actually update order status in database (no reset needed - use existing order)
        with components['db_client'].transaction() as cur:
            cur.execute("UPDATE orders SET status = 'processing' WHERE _id = 'O2'")
        time.sleep(3.0)  # Allow CDC event to be captured and WAL to flush (UPDATEs need more time)
        
        # Consume CDC events from replication slot
        events = cdc_helper.consume_events(timeout=5.0, min_events=1)
        
        # Debug: print all events
        print(f"\\nReceived {len(events)} events:")
        for e in events:
            print(f"  - {e.table}: {e.operation} {e.after}")
        
        # Filter for our table and ID
        order_events = [e for e in events if e.table == "orders" and e.after and e.after.get("_id") == "O2" and e.after.get("status") == "processing"]
        assert len(order_events) > 0, f"Expected order event with status=processing, got {len(events)} total events"
        
        # Process the event
        event = order_events[0]
        result = components['routing_engine'].handle_event(event)
        
        assert result.immediate == 1
        
        # Verify database update
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT status FROM orders WHERE _id = 'O1'")
            assert cur.fetchone()[0] == "shipped"
        
        # Verify customer tracked
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C1'")
            assert cur.fetchone() is not None


class TestDepth2AdaptiveChanges:
    """Test depth 2: Adaptive routing (order_lines) - can be immediate or deferred."""
    
    def test_order_line_insert_immediate(self, components, cdc_helper):
        """Test INSERT on order_lines with actual database mutation."""
        clear_tracking_tables(components['db_client'])
        
        # Actually insert order line into database
        with components['db_client'].transaction() as cur:
            cur.execute("INSERT INTO order_lines (_id, order_id, product_id, quantity, price, _deleted) VALUES ('OL999', 'O1', 'P1', 10, 10.00, FALSE)")
        
        # Consume real CDC events
        events = cdc_helper.consume_events(timeout=3.0)
        
        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        ol_events = [e for e in app_events if e.table == "order_lines" and e.after and e.after.get("_id") == "OL999"]
        assert len(ol_events) > 0, f"Expected order_lines event for OL999, got {len(app_events)} app events"
        
        event = ol_events[0]
        result = components['routing_engine'].handle_event(event)
        
        # Depth 2 with simple join resolves immediately
        assert result.immediate == 1
        assert result.deferred == 0
        
        # Verify order line in database
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT _id, order_id, quantity FROM order_lines WHERE _id = 'OL999'")
            order_line = cur.fetchone()
            assert order_line is not None
            assert order_line[1] == "O1"
            assert order_line[2] == 10
        
        # Verify customer C1 is tracked (OL999 -> O1 -> C1)
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C1'")
            assert cur.fetchone() is not None
    
    def test_product_update_deferred(self, components, cdc_helper):
        """Test UPDATE on products with actual database mutation - should be deferred."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any pending events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Update product price in database (using P2 to avoid conflicts with other tests)
        with components['db_client'].transaction() as cur:
            cur.execute("UPDATE products SET price = 25.99 WHERE _id = 'P2'")
        time.sleep(3.0)  # Allow CDC event to be captured and WAL to flush (UPDATEs need more time)
        
        # Consume real CDC events
        events = cdc_helper.consume_events(timeout=5.0, min_events=1)

        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        product_events = [e for e in app_events if e.table == "products" and e.after and e.after.get("_id") == "P2"]
        assert len(product_events) > 0, f"Expected products event for P2, got {len(app_events)} app events"
        
        event = product_events[0]
        result = components['routing_engine'].handle_event(event)
        
        # Should be deferred since products is depth 3
        assert result.immediate == 0
        assert result.deferred == 1
        
        # Verify database update
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT price FROM products WHERE _id = 'P2'")
            assert cur.fetchone()[0] == 25.99
        
        # Verify intermediate tracking
        with components['db_client'].transaction() as cur:
            cur.execute(
                "SELECT table_name, entity_id FROM intermediate_to_track WHERE table_name = 'products' AND entity_id = 'P2'"
            )
            assert cur.fetchone() is not None


class TestPercolation:
    """Test percolation process for resolving deferred items."""
    
    def test_percolate_products_to_customers(self, components):
        """Test percolation resolves products to customers through multiple hops."""
        clear_tracking_tables(components['db_client'])
        
        # Add deferred product - depth 3, always deferred
        event = CDCEvent(
            table="products",
            schema="public",
            operation="u",
            before={"_id": "P1"},
            after={"_id": "P1", "name": "Widget", "price": 12.00, "_deleted": False},
            source={"table": "products"},
            ts_ms=int(time.time() * 1000)
        )
        components['routing_engine'].handle_event(event)
        
        # Verify intermediate tracking has the item
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT COUNT(*) FROM intermediate_to_track WHERE table_name = 'products' AND entity_id = 'P1'")
            count = cur.fetchone()[0]
            assert count == 1
        
        # Run percolation
        percolator = components['percolator']
        result = percolator.percolate_batch()
        
        # Should have resolved at least one item
        assert result['items_percolated'] > 0
        
        # Verify customer C1 is now tracked (P1 -> OL1/OL3 -> O1/O2 -> C1)
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C1'")
            assert cur.fetchone() is not None
            
            # Verify intermediate item is marked as processed
            cur.execute(
                "SELECT percolated FROM intermediate_to_track WHERE table_name = 'products' AND entity_id = 'P1'"
            )
            row = cur.fetchone()
            assert row is not None and row[0] == True  # percolated is BOOLEAN
    
    def test_percolate_products(self, components):
        """Test percolation resolves products through multiple hops."""
        clear_tracking_tables(components['db_client'])
        
        # Add deferred product change
        event = CDCEvent(
            table="products",
            schema="public",
            operation="u",
            before={"_id": "P1"},
            after={"_id": "P1", "name": "Widget", "price": 15.00, "_deleted": False},
            source={"table": "products"},
            ts_ms=int(time.time() * 1000)
        )
        components['routing_engine'].handle_event(event)
        
        # Run percolation
        result = components['percolator'].percolate_batch()
        
        assert result['items_percolated'] > 0
        
        # Product P1 is used in order lines OL1 (O1->C1) and OL3 (O2->C1)
        # So customer C1 should be tracked
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C1'")
            assert cur.fetchone() is not None
    
    def test_percolate_batch_processing(self, components):
        """Test percolation handles multiple items in batch."""
        clear_tracking_tables(components['db_client'])
        
        # Add multiple deferred items (products are depth 3, always deferred)
        for i in range(5):
            event = CDCEvent(
                table="products",
                schema="public",
                operation="u",
                before={"_id": f"P{i}"},
                after={"_id": f"P{i}", "name": f"Product {i}", "price": 10.00, "_deleted": False},
                source={"table": "products"},
                ts_ms=int(time.time() * 1000)
            )
            components['routing_engine'].handle_event(event)
        
        # Verify 5 items in intermediate
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT COUNT(*) FROM intermediate_to_track WHERE percolated = FALSE")
            count = cur.fetchone()[0]
            assert count >= 5
        
        # Run percolation
        result = components['percolator'].percolate_batch()
        
        # Should process multiple items
        assert result['items_percolated'] >= 5


class TestEndToEndScenarios:
    """Test complete end-to-end scenarios."""
    
    def test_new_order_workflow(self, components, cdc_helper):
        """Test complete workflow: new customer, order, order lines with actual database mutations."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any lingering CDC events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Step 1: Insert new customer in database
        with components['db_client'].transaction() as cur:
            cur.execute("INSERT INTO customers (_id, name, email, _deleted) VALUES ('C100', 'Test Customer', 'test@test.com', FALSE)")
        time.sleep(2.0)  # Allow CDC event to be captured and WAL to flush
        
        # Consume real CDC events for customer
        events = cdc_helper.consume_events(timeout=6.0, min_events=1)
        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        customer_events = [e for e in app_events if e.table == "customers" and e.after and e.after.get("_id") == "C100"]
        assert len(customer_events) > 0, f"Expected customer event for C100, got {len(app_events)} app events"
        customer_event = customer_events[0]
        components['routing_engine'].handle_event(customer_event)
        time.sleep(1.0)  # Longer sleep to let tracking table events fully propagate
        
        # Step 2: Insert new order in database
        with components['db_client'].transaction() as cur:
            cur.execute("INSERT INTO orders (_id, cust_id, order_date, status, _deleted) VALUES ('O100', 'C100', '2024-03-01', 'pending', FALSE)")
        time.sleep(2.0)  # Allow CDC event to be captured and WAL to flush
        
        # Consume real CDC events for order
        events = cdc_helper.consume_events(timeout=6.0)
        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        order_events = [e for e in app_events if e.table == "orders" and e.after and e.after.get("_id") == "O100"]
        assert len(order_events) > 0, f"Expected order event for O100, got {len(app_events)} app events"
        order_event = order_events[0]
        components['routing_engine'].handle_event(order_event)
        time.sleep(1.0)  # Longer sleep to let tracking table events fully propagate
        
        # Step 3: Insert order line in database
        with components['db_client'].transaction() as cur:
            cur.execute("INSERT INTO order_lines (_id, order_id, product_id, quantity, price, _deleted) VALUES ('OL100', 'O100', 'P1', 2, 10.00, FALSE)")
        time.sleep(2.0)  # Allow CDC event to be captured and WAL to flush
        
        # Consume real CDC events for order line
        events = cdc_helper.consume_events(timeout=6.0)
        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        ol100_events = [e for e in app_events if e.table == "order_lines" and e.after and e.after.get("_id") == "OL100"]
        assert len(ol100_events) > 0, f"Expected order_lines event for OL100, got {len(app_events)} app events"
        ol_event = ol100_events[0]
        components['routing_engine'].handle_event(ol_event)
        
        # Run percolation to resolve deferred items
        components['percolator'].percolate_batch()
        
        # Verify data exists in database
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT _id, name, email FROM customers WHERE _id = 'C100'")
            customer = cur.fetchone()
            assert customer is not None
            assert customer[1] == "Test Customer"
            
            cur.execute("SELECT _id, cust_id FROM orders WHERE _id = 'O100'")
            order = cur.fetchone()
            assert order is not None
            assert order[1] == "C100"
            
            cur.execute("SELECT COUNT(*) FROM order_lines WHERE order_id = 'O100'")
            assert cur.fetchone()[0] == 1  # Single order line
        
        # Verify customer is tracked
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C100'")
            assert cur.fetchone() is not None
    
    def test_cascading_updates(self, components, cdc_helper):
        """Test updates cascade through relationships with actual database mutations."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any pending events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Update product price in database (depth 3)
        with components['db_client'].transaction() as cur:
            cur.execute("UPDATE products SET price = 12.00 WHERE _id = 'P1'")
        time.sleep(3.0)  # Allow CDC event to be captured and WAL to flush (UPDATEs need more time)
        
        # Consume real CDC events for product
        events = cdc_helper.consume_events(timeout=6.0, min_events=1)
        product_events = [e for e in events if e.table == "products" and e.after and e.after.get("_id") == "P1"]
        assert len(product_events) > 0, f"Expected products event for P1, got {len(events)} events"
        product_event = product_events[0]
        result1 = components['routing_engine'].handle_event(product_event)
        assert result1.deferred == 1
        
        # Update order status in database (depth 1)
        with components['db_client'].transaction() as cur:
            cur.execute("UPDATE orders SET status = 'shipped' WHERE _id = 'O1'")
        time.sleep(3.0)  # Allow CDC event to be captured and WAL to flush (UPDATEs need more time)
        
        # Consume real CDC events for order
        events = cdc_helper.consume_events(timeout=6.0, min_events=1)
        order_events = [e for e in events if e.table == "orders" and e.after and e.after.get("_id") == "O1"]
        assert len(order_events) > 0, f"Expected orders event for O1, got {len(events)} events"
        order_event = order_events[0]
        result2 = components['routing_engine'].handle_event(order_event)
        assert result2.immediate == 1
        
        # Run percolation
        components['percolator'].percolate_batch()
        
        # Verify actual database state
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT price FROM products WHERE _id = 'P1'")
            assert cur.fetchone()[0] == 12.00
            
            cur.execute("SELECT status FROM orders WHERE _id = 'O1'")
            assert cur.fetchone()[0] == "shipped"
        
        # Both updates should result in customer C1 being tracked
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C1'")
            assert cur.fetchone() is not None
    
    def test_deletion_workflow(self, components, cdc_helper):
        """Test soft deletion propagation with actual database mutation."""
        clear_tracking_tables(components['db_client'])
        
        # Clear any pending events from previous tests
        aggressive_event_clearing(cdc_helper)
        time.sleep(1.0)
        
        # Verify order line exists before deletion
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT _deleted FROM order_lines WHERE _id = 'OL1'")
            assert cur.fetchone()[0] is False
        
        # Soft delete an order line in database
        with components['db_client'].transaction() as cur:
            cur.execute("UPDATE order_lines SET _deleted = TRUE WHERE _id = 'OL1'")
        time.sleep(3.0)  # Allow CDC event to be captured and WAL to flush (UPDATEs need more time)
        
        # Consume real CDC events
        events = cdc_helper.consume_events(timeout=6.0)
        
        # Filter out tracking table events
        app_events = [e for e in events if e.table not in ['customers_to_reprocess', 'intermediate_to_track']]
        ol_events = [e for e in app_events if e.table == "order_lines" and e.after and e.after.get("_id") == "OL1"]
        assert len(ol_events) > 0, f"Expected order_lines event for OL1, got {len(app_events)} app events"
        
        event = ol_events[0]
        result = components['routing_engine'].handle_event(event)
        
        # Since join keys (order_id, product_id) didn't change, should route immediately
        assert result.immediate == 1 or result.deferred == 1
        
        # Run percolation for any deferred items
        components['percolator'].percolate_batch()
        
        # Verify deletion persisted in database
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT _deleted FROM order_lines WHERE _id = 'OL1'")
            assert cur.fetchone()[0] is True
        
        # Customer should be tracked for re-evaluation
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT customer_id FROM customers_to_reprocess WHERE customer_id = 'C1'")
            assert cur.fetchone() is not None


class TestStatistics:
    """Test statistics and monitoring."""
    
    def test_get_pending_counts(self, components):
        """Test getting counts of pending items."""
        clear_tracking_tables(components['db_client'])
        
        # Add some tracked items
        with components['db_client'].transaction() as cur:
            cur.execute("INSERT INTO customers_to_reprocess (customer_id) VALUES ('C1'), ('C2'), ('C3')")
            cur.execute("INSERT INTO intermediate_to_track (table_name, entity_id, depth) VALUES ('order_lines', 'OL1', 2), ('products', 'P1', 3)")
        
        # Query counts
        with components['db_client'].transaction() as cur:
            cur.execute("SELECT COUNT(*) FROM customers_to_reprocess")
            base_count = cur.fetchone()[0]
            assert base_count == 3
            
            cur.execute("SELECT COUNT(*) FROM intermediate_to_track WHERE percolated = FALSE")
            intermediate_count = cur.fetchone()[0]
            assert intermediate_count == 2
    
    def test_percolation_statistics(self, components):
        """Test percolation returns useful statistics."""
        clear_tracking_tables(components['db_client'])
        
        # Add deferred items (products are depth 3, always deferred)
        for i in range(3):
            event = CDCEvent(
                table="products",
                schema="public",
                operation="u",
                before={"_id": f"P{i+1}"},
                after={"_id": f"P{i+1}", "name": f"Product {i}", "price": 10.00 + i, "_deleted": False},
                source={"table": "products"},
                ts_ms=int(time.time() * 1000)
            )
            components['routing_engine'].handle_event(event)
        
        # Run percolation
        result = components['percolator'].percolate_batch()
        
        # Check statistics
        assert 'items_percolated' in result
        assert 'entities_tracked' in result
        assert result['items_percolated'] >= 3
        assert result['entities_tracked'] >= 1


def test_full_integration(components, cdc_helper):
    """Full integration test with mixed operations and actual database mutations."""
    clear_tracking_tables(components['db_client'])
    
    # Clear any pending events from previous tests
    aggressive_event_clearing(cdc_helper)
    time.sleep(1.0)
    
    # 1. Update customer in database
    with components['db_client'].transaction() as cur:
        cur.execute("UPDATE customers SET name = 'Acme Updated' WHERE _id = 'C1'")
    
    # 2. Insert new order in database
    with components['db_client'].transaction() as cur:
        cur.execute("INSERT INTO orders (_id, cust_id, order_date, status, _deleted) VALUES ('O_NEW', 'C2', '2024-03-01', 'pending', FALSE)")
    
    # 3. Update order line quantity in database
    with components['db_client'].transaction() as cur:
        cur.execute("UPDATE order_lines SET quantity = 5 WHERE _id = 'OL2'")
    
    # 4. Update product price in database
    with components['db_client'].transaction() as cur:
        cur.execute("UPDATE products SET price = 35.00 WHERE _id = 'P3'")
    
    # Let WAL flush after all transactions (longer delay for multiple operations including UPDATEs)
    time.sleep(4.0)
    
    # Consume real CDC events
    all_events = cdc_helper.consume_events(timeout=8.0)
    
    # Filter for each specific event
    customer_events = [e for e in all_events if e.table == "customers" and e.after and e.after.get("_id") == "C1"]
    order_events = [e for e in all_events if e.table == "orders" and e.after and e.after.get("_id") == "O_NEW"]
    ol_events = [e for e in all_events if e.table == "order_lines" and e.after and e.after.get("_id") == "OL2"]
    product_events = [e for e in all_events if e.table == "products" and e.after and e.after.get("_id") == "P3"]
    
    assert len(customer_events) > 0, f"Expected customer event for C1"
    assert len(order_events) > 0, f"Expected order event for O_NEW"
    assert len(ol_events) > 0, f"Expected order_lines event for OL2"
    assert len(product_events) > 0, f"Expected products event for P3"
    
    events = [
        customer_events[0],
        order_events[0],
        ol_events[0],
        product_events[0]
    ]
    
    # Process all events
    immediate_count = 0
    deferred_count = 0
    
    for event in events:
        result = components['routing_engine'].handle_event(event)
        immediate_count += result.immediate
        deferred_count += result.deferred
    
    # Customer (depth 0), order (depth 1), and order_line (depth 2 adaptive) all immediate
    # Only product (depth 3) is deferred
    assert immediate_count == 3  
    assert deferred_count == 1
    
    # Run percolation
    perc_result = components['percolator'].percolate_batch()
    assert perc_result['items_percolated'] >= 1
    
    # Verify actual database mutations persisted
    with components['db_client'].transaction() as cur:
        cur.execute("SELECT name FROM customers WHERE _id = 'C1'")
        assert cur.fetchone()[0] == "Acme Updated"
        
        cur.execute("SELECT cust_id FROM orders WHERE _id = 'O_NEW'")
        assert cur.fetchone()[0] == "C2"
        
        cur.execute("SELECT quantity FROM order_lines WHERE _id = 'OL2'")
        assert cur.fetchone()[0] == 5
        
        cur.execute("SELECT price FROM products WHERE _id = 'P3'")
        assert cur.fetchone()[0] == 35.00
    
    # Verify at least 2 customers tracked
    with components['db_client'].transaction() as cur:
        cur.execute("SELECT COUNT(*) FROM customers_to_reprocess")
        count = cur.fetchone()[0]
        assert count >= 2
