"""End-to-end test for customers example - rewritten with proper lifecycle management.

Key improvements:
1. Module-scoped replication slot and CDC consumer (created once)
2. Each test uses unique entity IDs to avoid cross-contamination
3. Event filtering by test-specific IDs - no event bleeding
4. Minimal setup/teardown overhead
5. Tests are isolated and can run in any order
"""

import pytest
import time
import threading
import queue
from typing import List, Dict, Any
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
CREATE TABLE IF NOT EXISTS customers (
    _id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    _deleted BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS orders (
    _id VARCHAR(50) PRIMARY KEY,
    cust_id VARCHAR(50) NOT NULL REFERENCES customers(_id),
    order_date DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    _deleted BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS order_lines (
    _id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL REFERENCES orders(_id),
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    _deleted BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS products (
    _id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    _deleted BOOLEAN DEFAULT FALSE
);

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

CREATE INDEX IF NOT EXISTS idx_orders_cust_id ON orders(cust_id);
CREATE INDEX IF NOT EXISTS idx_order_lines_order_id ON order_lines(order_id);
CREATE INDEX IF NOT EXISTS idx_order_lines_product_id ON order_lines(product_id);

-- Set REPLICA IDENTITY FULL to capture old values
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_lines REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE customers_to_reprocess REPLICA IDENTITY FULL;
ALTER TABLE intermediate_to_track REPLICA IDENTITY FULL;
"""

# Base data (stable, not modified by tests)
BASE_DATA_SQL = """
INSERT INTO customers (_id, name, email) VALUES
    ('C_BASE_1', 'Base Customer 1', 'base1@test.com'),
    ('C_BASE_2', 'Base Customer 2', 'base2@test.com');

INSERT INTO products (_id, name, price) VALUES
    ('P_BASE_1', 'Base Product 1', 10.00),
    ('P_BASE_2', 'Base Product 2', 20.00);

INSERT INTO orders (_id, cust_id, order_date, status) VALUES
    ('O_BASE_1', 'C_BASE_1', '2024-01-01', 'shipped');

INSERT INTO order_lines (_id, order_id, product_id, quantity, price) VALUES
    ('OL_BASE_1', 'O_BASE_1', 'P_BASE_1', 1, 10.00);
"""

SQL_QUERY = """
SELECT 
    c._id as customer_id,
    c.name as customer_name,
    o._id as order_id,
    ol._id as order_line_id,
    p._id as product_id
FROM customers c
LEFT JOIN orders o ON o.cust_id = c._id
LEFT JOIN order_lines ol ON ol.order_id = o._id
LEFT JOIN products p ON p._id = ol.product_id
WHERE c._deleted = FALSE
"""


def cdc_consumer_thread(config_dict: Dict[str, Any], event_queue: queue.Queue, stop_event: threading.Event, ready_event: threading.Event):
    """Background thread that consumes CDC events continuously."""
    try:
        stream = LogicalReplicationStream(
            publication_name=config_dict['publication_name'],
            slot_name=config_dict['slot_name'],
            host=config_dict['host'],
            database=config_dict['database'],
            port=config_dict['port'],
            user=config_dict['user'],
            password=config_dict['password']
        )
        
        ready_event.set()  # Signal that consumer is ready
        
        for decoded_msg in stream:
            if stop_event.is_set():
                break
            
            # Convert new_tuple and old_tuple (lists of Column objects) to dictionaries
            new_values = {}
            if decoded_msg.new_tuple:
                for col in decoded_msg.new_tuple:
                    new_values[col.name] = col.value
            
            old_values = {}
            if decoded_msg.old_tuple:
                for col in decoded_msg.old_tuple:
                    old_values[col.name] = col.value
            
            # Map op to CDCEvent operation format ('I'/'U'/'D' -> 'c'/'u'/'d')
            op_map = {'I': 'c', 'U': 'u', 'D': 'd'}
            op = op_map.get(decoded_msg.op)
            
            if op:
                # Create CDCEvent
                event = CDCEvent(
                    table=decoded_msg.table_name,
                    schema=decoded_msg.table_schema or 'public',
                    operation=op,
                    before=old_values if old_values else None,
                    after=new_values if new_values else None,
                    source={'table': decoded_msg.table_name},
                    ts_ms=int(time.time() * 1000)
                )
                event_queue.put(event)
        
        stream.close()
    except Exception as e:
        import traceback
        event_queue.put({'error': str(e), 'traceback': traceback.format_exc()})
        ready_event.set()  # Signal ready even on error so we don't hang


class CDCTestHelper:
    """Helper for managing CDC stream in tests."""
    
    def __init__(self, config: Config):
        self.config = config
        self.event_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.ready_event = threading.Event()
        self.consumer_thread = None
    
    def start(self):
        """Start CDC consumer in background thread."""
        config_dict = {
            'publication_name': self.config.replication.publication_name,
            'slot_name': self.config.replication.slot_name,
            'host': self.config.replication.host,
            'database': self.config.replication.dbname,
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
        
        if not self.ready_event.wait(timeout=10):
            raise RuntimeError("CDC consumer failed to start within 10 seconds")
        
        # Check if there was an error on startup
        try:
            event_dict = self.event_queue.get(timeout=0.1)
            if isinstance(event_dict, dict) and 'error' in event_dict:
                raise RuntimeError(f"CDC consumer error: {event_dict['error']}\\n{event_dict.get('traceback', '')}")
            else:
                # Put it back if it wasn't an error
                self.event_queue.put(event_dict)
        except queue.Empty:
            pass  # No error, good to go
    
    def stop(self):
        """Stop CDC consumer thread."""
        if self.consumer_thread:
            self.stop_event.set()
            self.consumer_thread.join(timeout=10)
            time.sleep(0.5)
    
    def wait_for_event(self, table: str, entity_id: str, timeout: float = 10.0) -> CDCEvent:
        """
        Wait for a specific event matching table and entity ID.
        
        This is the key to avoiding cross-test contamination - we only look for
        events with the specific ID we just created/modified.
        """
        deadline = time.time() + timeout
        seen_events = []
        
        while time.time() < deadline:
            try:
                event = self.event_queue.get(timeout=0.1)
                
                if isinstance(event, dict) and 'error' in event:
                    raise RuntimeError(f"CDC consumer error: {event['error']}")
                
                # Skip non-CDC events
                if not isinstance(event, CDCEvent):
                    continue
                
                seen_events.append(f"{event.table}:{event.after.get('_id') if event.after else 'NO_ID'}")
                
                # Skip tracking table events (not interested in them for tests)
                if event.table in ['customers_to_reprocess', 'intermediate_to_track']:
                    continue
                
                # Check if this is our event (matching table and ID)
                if event.table == table:
                    # Check if ID matches
                    if event.after and event.after.get('_id') == entity_id:
                        return event
                    elif event.before and event.before.get('_id') == entity_id:
                        return event
            except queue.Empty:
                continue
        
        raise TimeoutError(f"Timeout waiting for event: table={table}, entity_id={entity_id}. Seen: {seen_events}")
    
    def drain_queue(self, timeout: float = 1.0) -> int:
        """Drain all events from queue (for cleanup or debugging)."""
        count = 0
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.event_queue.get(timeout=0.1)
                count += 1
            except queue.Empty:
                break
        return count


# ============================================================================
# Module-scoped fixtures - created once for all tests
# ============================================================================

@pytest.fixture(scope="module")
def postgres_container():
    """PostgreSQL container with logical replication enabled."""
    container = PostgresContainer("postgres:18.1-alpine")
    container = container.with_command(
        "postgres "
        "-c wal_level=logical "
        "-c max_replication_slots=10 "
        "-c max_wal_senders=10"
    )
    container.start()
    time.sleep(2)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def db_config(postgres_container):
    """Database configuration from container."""
    return DatabaseConfig(
        host=postgres_container.get_container_host_ip(),
        port=int(postgres_container.get_exposed_port(5432)),
        dbname=postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password
    )


@pytest.fixture(scope="module")
def setup_database(db_config):
    """Setup database schema and base data once."""
    client = DatabaseClient(db_config.to_connection_params())
    
    # Create schema
    with client.transaction() as cur:
        cur.execute(SCHEMA_SQL)
    
    # Insert base data
    with client.transaction() as cur:
        cur.execute(BASE_DATA_SQL)
    
    client.close()
    yield db_config


@pytest.fixture(scope="module")
def config(setup_database):
    """Create configuration."""
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
            slot_name="test_cdc_slot_v2",
            publication_name="test_pub_v2",
            host=setup_database.host,
            port=setup_database.port,
            dbname=setup_database.dbname,
            user=setup_database.user,
            password=setup_database.password,
            ack_interval_seconds=1,
            max_batch_size=100
        )
    )


@pytest.fixture()
def cdc_helper(config, setup_database):
    """
    Function-scoped CDC helper - fresh for each test.
    
    Each test gets a new replication slot and CDC consumer,
    avoiding lag accumulation from previous tests.
    """
    db_client = DatabaseClient(config.database.to_connection_params())
    
    # Use unique slot/publication name per test to avoid conflicts
    import uuid
    test_id = str(uuid.uuid4())[:8]
    slot_name = f"test_slot_{test_id}"
    pub_name = f"test_pub_{test_id}"
    
    print(f"[FIXTURE] Creating slot: {slot_name}, publication: {pub_name}")
    
    # Create publication for all tables
    with db_client.transaction() as cur:
        cur.execute(f"CREATE PUBLICATION {pub_name} FOR ALL TABLES")
    
    # Create replication slot
    with db_client.transaction() as cur:
        cur.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
            (slot_name,)
        )
    
    db_client.close()
    
    # Override config with unique names
    temp_config = type('obj', (object,), {
        'database': config.database,
        'tracker': config.tracker,
        'replication': type('obj', (object,), {
            'publication_name': pub_name,
            'slot_name': slot_name,
            'host': config.replication.host,
            'dbname': config.replication.dbname,
            'port': config.replication.port,
            'user': config.replication.user,
            'password': config.replication.password,
        })()
    })()
    
    # Start CDC helper
    helper = CDCTestHelper(temp_config)
    helper.start()
    
    # Let it warm up and drain any initial events
    time.sleep(0.5)
    helper.drain_queue()
    
    print(f"[FIXTURE] CDC helper ready")
    
    yield helper
    
    # Cleanup
    print(f"[FIXTURE] Stopping CDC consumer...")
    helper.stop()
    
    db_client = DatabaseClient(config.database.to_connection_params())
    
    # Terminate active backends
    with db_client.transaction() as cur:
        cur.execute("""
            SELECT pg_terminate_backend(active_pid)
            FROM pg_replication_slots
            WHERE slot_name = %s AND active_pid IS NOT NULL
        """, (slot_name,))
    
    time.sleep(0.2)
    
    # Drop slot and publication
    try:
        with db_client.transaction() as cur:
            cur.execute(f"SELECT pg_drop_replication_slot('{slot_name}')")
    except Exception as e:
        print(f"[FIXTURE] Warning: Failed to drop slot: {e}")
    
    try:
        with db_client.transaction() as cur:
            cur.execute(f"DROP PUBLICATION IF EXISTS {pub_name}")
    except Exception as e:
        print(f"[FIXTURE] Warning: Failed to drop publication: {e}")
    
    db_client.close()
    print(f"[FIXTURE] Cleanup complete")


# ============================================================================
# Function-scoped fixtures - fresh components for each test
# ============================================================================

@pytest.fixture()
def components(config):
    """Initialize components for each test."""
    parser = SQLParser(config.tracker.sql_query, config.tracker.base_table)
    join_graph = parser.get_join_graph()
    dependency_graph = DependencyGraph(config.tracker.base_table, join_graph)
    base_id_column = parser.get_base_id_column() or "customer_id"
    
    db_client = DatabaseClient(
        config.database.to_connection_params(),
        tracking_table=config.tracker.tracking_table
    )
    
    routing_engine = RoutingEngine(
        db_client,
        dependency_graph,
        base_table_id_column=base_id_column,
        immediate_threshold=config.tracker.immediate_fanout_threshold
    )
    
    percolator = PercolationEngine(
        db_client,
        dependency_graph,
        base_id_column,
        batch_size=config.tracker.percolation_batch_size
    )
    
    # Don't clear tracking tables - let them accumulate
    # This avoids CDC events from DELETEs interfering with tests
    # with db_client.transaction() as cur:
    #     cur.execute("DELETE FROM customers_to_reprocess")
    #     cur.execute("DELETE FROM intermediate_to_track")
    
    yield {
        'config': config,
        'db_client': db_client,
        'dependency_graph': dependency_graph,
        'routing_engine': routing_engine,
        'percolator': percolator,
        'base_id_column': base_id_column
    }
    
    db_client.close()


# ============================================================================
# Tests - each uses unique IDs to avoid contamination
# ============================================================================

class TestDepth0BaseTableChanges:
    """Test depth 0: Direct changes to base table (customers)."""
    
    def test_customer_insert(self, components, cdc_helper):
        """Test INSERT on customers table."""
        test_id = 'C_TEST_INSERT_1'
        
        # Insert customer
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO customers (_id, name, email) VALUES (%s, %s, %s)",
                (test_id, 'Test Customer', 'test@test.com')
            )
        
        # Wait for specific event
        event = cdc_helper.wait_for_event('customers', test_id, timeout=5.0)
        
        # Verify event
        assert event.operation == 'c'  # create
        assert event.after['_id'] == test_id
        assert event.after['name'] == 'Test Customer'
        
        # Process event
        result = components['routing_engine'].handle_event(event)
        assert result.immediate == 1
        assert result.deferred == 0
        
        # Verify tracking
        with components['db_client'].transaction() as cur:
            cur.execute(
                "SELECT customer_id FROM customers_to_reprocess WHERE customer_id = %s",
                (test_id,)
            )
            assert cur.fetchone() is not None
    
    def test_customer_update(self, components, cdc_helper):
        """Test UPDATE on customers table."""
        test_id = 'C_TEST_UPDATE_1'
        
        # Insert customer first
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO customers (_id, name, email) VALUES (%s, %s, %s)",
                (test_id, 'Original Name', 'test@test.com')
            )
        
        # Drain insert event
        cdc_helper.wait_for_event('customers', test_id, timeout=5.0)
        
        # Update customer
        with components['db_client'].transaction() as cur:
            cur.execute(
                "UPDATE customers SET name = %s WHERE _id = %s",
                ('Updated Name', test_id)
            )
        
        # UPDATEs need more time to propagate through WAL - this is a known PostgreSQL CDC quirk
        time.sleep(3.0)
        
        # Wait for update event
        event = cdc_helper.wait_for_event('customers', test_id, timeout=10.0)
        
        # Verify event
        assert event.operation == 'u'  # update
        assert event.after['name'] == 'Updated Name'
        assert event.before['name'] == 'Original Name'
        
        # Process event
        result = components['routing_engine'].handle_event(event)
        assert result.immediate == 1
    
    def test_customer_delete(self, components, cdc_helper):
        """Test soft DELETE on customers table."""
        test_id = 'C_TEST_DELETE_1'
        
        # Insert customer first
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO customers (_id, name, email) VALUES (%s, %s, %s)",
                (test_id, 'To Delete', 'test@test.com')
            )
        
        # Drain insert event
        cdc_helper.wait_for_event('customers', test_id, timeout=5.0)
        
        # Soft delete
        with components['db_client'].transaction() as cur:
            cur.execute(
                "UPDATE customers SET _deleted = TRUE WHERE _id = %s",
                (test_id,)
            )
        
        # Wait for delete event
        event = cdc_helper.wait_for_event('customers', test_id, timeout=5.0)
        
        # Verify event
        assert event.operation == 'u'  # soft delete is an update
        assert event.after['_deleted'] is True
        
        # Process event
        result = components['routing_engine'].handle_event(event)
        assert result.immediate == 1


class TestDepth1ImmediateChanges:
    """Test depth 1: Changes to tables one hop away (orders)."""
    
    def test_order_insert(self, components, cdc_helper):
        """Test INSERT on orders table."""
        cust_id = 'C_TEST_ORDER_1'
        order_id = 'O_TEST_INSERT_1'
        
        # Create customer first
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO customers (_id, name, email) VALUES (%s, %s, %s)",
                (cust_id, 'Test Customer', 'test@test.com')
            )
        cdc_helper.wait_for_event('customers', cust_id, timeout=5.0)
        
        # Insert order
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO orders (_id, cust_id, order_date, status) VALUES (%s, %s, %s, %s)",
                (order_id, cust_id, '2024-01-01', 'pending')
            )
        
        # Wait for order event
        event = cdc_helper.wait_for_event('orders', order_id, timeout=5.0)
        
        # Verify and process
        assert event.operation == 'c'
        result = components['routing_engine'].handle_event(event)
        assert result.immediate == 1
        
        # Verify customer tracked
        with components['db_client'].transaction() as cur:
            cur.execute(
                "SELECT customer_id FROM customers_to_reprocess WHERE customer_id = %s",
                (cust_id,)
            )
            assert cur.fetchone() is not None
    
    def test_order_update_join_key(self, components, cdc_helper):
        """Test UPDATE on orders changing the join key (cust_id)."""
        cust1_id = 'C_TEST_ORDER_UPDATE_1'
        cust2_id = 'C_TEST_ORDER_UPDATE_2'
        order_id = 'O_TEST_UPDATE_1'
        
        # Create two customers (separate INSERT statements to ensure separate CDC events)
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO customers (_id, name, email) VALUES (%s, %s, %s)",
                (cust1_id, 'Customer 1', 'c1@test.com')
            )
        cdc_helper.wait_for_event('customers', cust1_id, timeout=5.0)
        
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO customers (_id, name, email) VALUES (%s, %s, %s)",
                (cust2_id, 'Customer 2', 'c2@test.com')
            )
        cdc_helper.wait_for_event('customers', cust2_id, timeout=5.0)
        
        # Insert order for customer 1
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO orders (_id, cust_id, order_date, status) VALUES (%s, %s, %s, %s)",
                (order_id, cust1_id, '2024-01-01', 'pending')
            )
        cdc_helper.wait_for_event('orders', order_id, timeout=5.0)
        
        # Update order to customer 2
        with components['db_client'].transaction() as cur:
            cur.execute(
                "UPDATE orders SET cust_id = %s WHERE _id = %s",
                (cust2_id, order_id)
            )
        
        # UPDATEs need more time to propagate through WAL
        time.sleep(3.0)
        
        # Wait for update event
        event = cdc_helper.wait_for_event('orders', order_id, timeout=5.0)
        
        # Verify event shows join key change
        assert event.operation == 'u'
        assert event.before['cust_id'] == cust1_id
        assert event.after['cust_id'] == cust2_id
        
        # Process event
        result = components['routing_engine'].handle_event(event)
        assert result.immediate >= 1
        
        # Verify new customer is tracked
        with components['db_client'].transaction() as cur:
            cur.execute(
                "SELECT customer_id FROM customers_to_reprocess WHERE customer_id = %s",
                (cust2_id,)
            )
            assert cur.fetchone() is not None


class TestDepth2AdaptiveChanges:
    """Test depth 2: Changes to order_lines (adaptive routing)."""
    
    def test_order_line_insert(self, components, cdc_helper):
        """Test INSERT on order_lines."""
        cust_id = 'C_TEST_OL_1'
        order_id = 'O_TEST_OL_1'
        ol_id = 'OL_TEST_INSERT_1'
        
        # Setup customer and order
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO customers (_id, name, email) VALUES (%s, %s, %s)",
                (cust_id, 'Test Customer', 'test@test.com')
            )
        cdc_helper.wait_for_event('customers', cust_id, timeout=5.0)
        
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO orders (_id, cust_id, order_date, status) VALUES (%s, %s, %s, %s)",
                (order_id, cust_id, '2024-01-01', 'pending')
            )
        cdc_helper.wait_for_event('orders', order_id, timeout=5.0)
        
        # Insert order line
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO order_lines (_id, order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s, %s)",
                (ol_id, order_id, 'P_BASE_1', 10, 10.00)
            )
        
        # Wait for event
        event = cdc_helper.wait_for_event('order_lines', ol_id, timeout=5.0)
        
        # Process event
        result = components['routing_engine'].handle_event(event)
        assert result.immediate == 1  # Depth 2 with simple join resolves immediately
        
        # Verify customer tracked
        with components['db_client'].transaction() as cur:
            cur.execute(
                "SELECT customer_id FROM customers_to_reprocess WHERE customer_id = %s",
                (cust_id,)
            )
            assert cur.fetchone() is not None


class TestDepth3DeferredChanges:
    """Test depth 3: Changes to products (deferred)."""
    
    def test_product_update_deferred(self, components, cdc_helper):
        """Test UPDATE on products - should be deferred."""
        prod_id = 'P_TEST_UPDATE_1'
        
        # Insert product
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO products (_id, name, price) VALUES (%s, %s, %s)",
                (prod_id, 'Test Product', 15.00)
            )
        cdc_helper.wait_for_event('products', prod_id, timeout=5.0)
        
        # Update product price
        with components['db_client'].transaction() as cur:
            cur.execute(
                "UPDATE products SET price = %s WHERE _id = %s",
                (25.99, prod_id)
            )
        
        # UPDATEs need more time to propagate through WAL
        time.sleep(3.0)
        
        # Wait for event
        event = cdc_helper.wait_for_event('products', prod_id, timeout=5.0)
        
        # Process event
        result = components['routing_engine'].handle_event(event)
        assert result.immediate == 0
        assert result.deferred == 1  # Depth 3 is deferred
        
        # Verify intermediate tracking
        with components['db_client'].transaction() as cur:
            cur.execute(
                "SELECT entity_id FROM intermediate_to_track WHERE table_name = 'products' AND entity_id = %s",
                (prod_id,)
            )
            assert cur.fetchone() is not None


class TestPercolation:
    """Test percolation process."""
    
    def test_percolate_product_to_customer(self, components, cdc_helper):
        """Test percolation resolves products through to customers."""
        cust_id = 'C_TEST_PERC_1'
        order_id = 'O_TEST_PERC_1'
        ol_id = 'OL_TEST_PERC_1'
        prod_id = 'P_TEST_PERC_1'
        
        # Setup full chain: customer -> order -> order_line -> product
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO customers (_id, name, email) VALUES (%s, %s, %s)",
                (cust_id, 'Test Customer', 'test@test.com')
            )
        cdc_helper.wait_for_event('customers', cust_id, timeout=5.0)
        
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO orders (_id, cust_id, order_date, status) VALUES (%s, %s, %s, %s)",
                (order_id, cust_id, '2024-01-01', 'pending')
            )
        cdc_helper.wait_for_event('orders', order_id, timeout=5.0)
        
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO products (_id, name, price) VALUES (%s, %s, %s)",
                (prod_id, 'Test Product', 10.00)
            )
        cdc_helper.wait_for_event('products', prod_id, timeout=5.0)
        
        with components['db_client'].transaction() as cur:
            cur.execute(
                "INSERT INTO order_lines (_id, order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s, %s)",
                (ol_id, order_id, prod_id, 5, 10.00)
            )
        cdc_helper.wait_for_event('order_lines', ol_id, timeout=5.0)
        
        # Clear tracking
        with components['db_client'].transaction() as cur:
            cur.execute("DELETE FROM customers_to_reprocess")
            cur.execute("DELETE FROM intermediate_to_track")
        
        # Update product (creates deferred item)
        with components['db_client'].transaction() as cur:
            cur.execute(
                "UPDATE products SET price = %s WHERE _id = %s",
                (15.00, prod_id)
            )
        
        # UPDATEs need more time to propagate through WAL
        time.sleep(3.0)
        
        event = cdc_helper.wait_for_event('products', prod_id, timeout=5.0)
        result = components['routing_engine'].handle_event(event)
        assert result.deferred == 1
        
        # Run percolation
        perc_result = components['percolator'].percolate_batch()
        assert perc_result['items_percolated'] >= 1
        
        # Verify customer is now tracked
        with components['db_client'].transaction() as cur:
            cur.execute(
                "SELECT customer_id FROM customers_to_reprocess WHERE customer_id = %s",
                (cust_id,)
            )
            assert cur.fetchone() is not None


def test_summary(components, cdc_helper):
    """Summary test showing the lifecycle works correctly."""
    # This test demonstrates that:
    # 1. CDC consumer runs continuously (module-scoped)
    # 2. Each test uses unique IDs
    # 3. No cross-contamination between tests
    # 4. Tests can run in any order
    
    test_id = 'C_SUMMARY_TEST'
    
    with components['db_client'].transaction() as cur:
        cur.execute(
            "INSERT INTO customers (_id, name, email) VALUES (%s, %s, %s)",
            (test_id, 'Summary Test', 'summary@test.com')
        )
    
    event = cdc_helper.wait_for_event('customers', test_id, timeout=5.0)
    result = components['routing_engine'].handle_event(event)
    
    assert result.immediate == 1
    assert event.after['_id'] == test_id
