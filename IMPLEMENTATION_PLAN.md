# CDC Dependency Tracker - Implementation Plan

## Overview
Python CLI tool that processes Debezium CDC events (via stdin JSON) and tracks which base table rows need reprocessing when dependent table data changes. Uses YAML config for database connection and SQL query definition.

## Architecture

### Core Components
1. **Config Parser** - Load and validate YAML configuration
2. **SQL Parser** - Extract join relationships from SQL query
3. **Dependency Graph** - Build bidirectional join graph for traversal
4. **CDC Event Handler** - Parse incoming CDC events from stdin
5. **Resolution Engine** - Query database to find affected base table rows
6. **Tracking Writer** - Insert affected IDs into tracking table

## Change Scenario Analysis

### Base Table (customers) Changes
| Operation | Scenario | Action |
|-----------|----------|--------|
| INSERT | New customer created | **No tracking needed** - no dependent data exists yet |
| UPDATE | Customer data modified | **Track customer_id** - customer's own data changed |
| DELETE | Customer deleted/marked _deleted=TRUE | **Track customer_id** - for cleanup/final processing |

### Dependent Table 1 (orders) Changes
| Operation | Join Keys | Scenario | Resolution |
|-----------|-----------|----------|------------|
| INSERT | cust_id | New order created | Query: `SELECT cust_id WHERE _id = after.cust_id` → Track customer |
| UPDATE | cust_id unchanged | Order data modified (date, etc.) | Query via `after.cust_id` → Track customer |
| UPDATE | cust_id changed | Order moved to different customer | Query BOTH `before.cust_id` AND `after.cust_id` → Track both customers |
| DELETE | cust_id | Order removed | Query via `before.cust_id` → Track customer |

### Dependent Table 2 (order_lines) Changes
| Operation | Join Keys | Scenario | Resolution |
|-----------|-----------|----------|------------|
| INSERT | order_id, product_id | New order line | Query: `orders` via `after.order_id` → `cust_id` → Track customer |
| UPDATE | Both unchanged | Quantity/price changed | Query: `orders` via `after.order_id` → Track customer |
| UPDATE | order_id changed | Line moved to different order | Query `orders` via `before.order_id` → Track old customer<br>Query `orders` via `after.order_id` → Track new customer |
| UPDATE | product_id changed | Product switched on same order | Query: `orders` via `after.order_id` → Track customer |
| UPDATE | Both changed | Line moved AND product changed | Query both paths as above → Track both customers |
| DELETE | order_id, product_id | Order line removed | Query: `orders` via `before.order_id` → Track customer |

### Dependent Table 3 (products) Changes
| Operation | Scenario | Resolution |
|-----------|----------|------------|
| INSERT | New product | **No tracking needed** - no order lines reference it yet |
| UPDATE | Product data changed | Multi-hop query:<br>1. `SELECT order_id FROM order_lines WHERE product_id = after._id AND _deleted = FALSE`<br>2. `SELECT cust_id FROM orders WHERE _id IN (...order_ids) AND _deleted = FALSE`<br>→ Track all affected customers |
| DELETE | Product deleted | Same as UPDATE but use `before._id` |

## Critical Implementation Details

### 1. CDC Event Format
Expected Debezium JSON structure via stdin:
```json
{
  "before": {
    "_id": "OL1",
    "order_id": "O1", 
    "product_id": "P1",
    "quantity": 5,
    "_deleted": false
  },
  "after": {
    "_id": "OL1",
    "order_id": "O2",
    "product_id": "P2", 
    "quantity": 10,
    "_deleted": false
  },
  "source": {
    "version": "2.1.0",
    "connector": "postgresql",
    "name": "mydb",
    "ts_ms": 1707649200000,
    "snapshot": "false",
    "db": "mydb",
    "schema": "public",
    "table": "order_lines",
    "txId": 12345,
    "lsn": 67890
  },
  "op": "u",
  "ts_ms": 1707649200000
}
```

**Debezium Operation Codes:**
- `c` = CREATE (INSERT)
- `u` = UPDATE
- `d` = DELETE
- `r` = READ (initial snapshot, treat as INSERT)
- `t` = TRUNCATE (not handled)

**Field Mapping:**
- Table name: `source.table`
- Schema name: `source.schema`
- Operation: `op` field
- Before state: `before` object (null for INSERT)
- After state: `after` object (null for DELETE)

**Important Debezium Behaviors:**
- INSERT (`op='c'`): `before` is null, `after` contains full row
- UPDATE (`op='u'`): Both `before` and `after` contain full row data
- DELETE (`op='d'`): `before` contains deleted row, `after` is null
- Snapshot READ (`op='r'`): Treat as INSERT for dependency tracking
- Tombstone: After DELETE, Debezium may send second event with null payload (skip these)
- TRUNCATE (`op='t'`): Not supported, log warning and skip

### 2. Join Graph Structure
Parse SQL to extract:
```python
join_graph = {
    # Forward relationships (from base table outward)
    "customers": [("orders", "_id", "cust_id")],
    "orders": [("order_lines", "_id", "order_id")],
    "order_lines": [("products", "product_id", "_id")],
    
    # Reverse relationships (for backward traversal)
    "orders": [("customers", "cust_id", "_id")],
    "order_lines": [("orders", "order_id", "_id")],
    "products": [("order_lines", "_id", "product_id")]
}
```

### 3. UPDATE Operation Logic
```
For UPDATE operations:
1. Identify all columns in CDC event that are join keys
2. Compare before vs after for each join key
3. If ANY join key changed:
   - Resolve path using BEFORE values → collect affected base IDs
   - Resolve path using AFTER values → collect affected base IDs
   - Track ALL unique base IDs found
4. If NO join keys changed:
   - Resolve path using AFTER values only → track base IDs
```

### 4. Resolution Algorithm
```python
def resolve_affected_customers(table, op_code, before_data, after_data):
    """Resolve affected customers from Debezium CDC event.
    
    Args:
        table: Table name from source.table
        op_code: Operation code ('c', 'u', 'd', 'r')
        before_data: before object (may be None)
        after_data: after object (may be None)
    """
    customers = set()
    
    # Map Debezium op codes to logical operations
    operation = {
        'c': 'INSERT',
        'r': 'INSERT',  # Snapshot read treated as insert
        'u': 'UPDATE',
        'd': 'DELETE'
    }.get(op_code)
    
    if table == "customers":
        # Base table itself changed
        id_field = "_id"
        if operation == "DELETE":
            customers.add(before_data[id_field])
        else:
            customers.add(after_data[id_field])
    
    elif operation == "INSERT":
        # Use after_data to traverse backwards
        customers.update(traverse_to_base(table, after_data))
    
    elif operation == "DELETE":
        # Use before_data to traverse backwards
        customers.update(traverse_to_base(table, before_data))
    
    elif operation == "UPDATE":
        # Check if any join keys changed
        join_keys = get_join_keys_for_table(table)
        keys_changed = [k for k in join_keys if before_data.get(k) != after_data.get(k)]
        
        if keys_changed:
            # Resolve both paths
            customers.update(traverse_to_base(table, before_data))
            customers.update(traverse_to_base(table, after_data))
        else:
            # Only non-join columns changed
            customers.update(traverse_to_base(table, after_data))
    
    return customers

def traverse_to_base(start_table, row_data):
    """Query database backwards through joins until reaching base table"""
    if start_table == "customers":
        return {row_data["_id"]}
    
    current_ids = {row_data["_id"]}
    current_table = start_table
    
    # Walk backwards through join graph
    while current_table != "customers":
        parent_table, join_col, parent_col = get_parent_join(current_table)
        
        # Query database for matching parent rows
        query = f"""
            SELECT DISTINCT {parent_col} 
            FROM {parent_table} 
            WHERE {parent_col} IN ({','.join(['%s'] * len(current_ids))})
              AND _deleted = FALSE
        """
        parent_ids = execute_query(query, list(current_ids))
        
        current_ids = set(parent_ids)
        current_table = parent_table
    
    return current_ids
```

### 5. Tracking Table Operations
```sql
-- Insert affected customer IDs
INSERT INTO customers_to_reprocess (customer_id) 
VALUES (%s)
ON CONFLICT (customer_id) 
DO UPDATE SET last_tracked_at = CURRENT_TIMESTAMP;
```

## Edge Cases & Considerations

### Handled Cases
✅ **Mutable join keys** - UPDATE detects changed keys and queries both paths
✅ **Multi-table traversal** - Products → order_lines → orders → customers
✅ **_deleted flag filtering** - Applied in all backward traversal queries
✅ **Duplicate tracking** - ON CONFLICT updates timestamp
✅ **Base table changes** - Customers table updates tracked directly

### Potential Issues
⚠️ **Orphaned references** - If order_line.order_id references non-existent order, traversal returns empty set (no customer tracked)
⚠️ **NULL join keys** - Should be rejected by foreign key constraints, but handle gracefully
⚠️ **Cascade deletes** - If database has CASCADE delete, multiple CDC events will fire
⚠️ **Performance** - Product update affecting 10,000+ order lines requires batched queries
⚠️ **Race conditions** - If CDC events processed out of order, may miss dependencies
⚠️ **Debezium tombstones** - Delete events followed by null payload tombstone (ignore tombstones)
⚠️ **Schema changes** - Debezium schema evolution may add/remove fields (validate required fields only)
⚠️ **Snapshot events** - Initial snapshot generates `op='r'` events (treat as inserts)

### Error Handling
- Missing join key in CDC event → Log error, skip tracking
- Database query failure → Retry with exponential backoff or fail-fast
- Invalid Debezium JSON → Log error, exit code 1
- Missing `source.table` field → Log error, exit code 1
- Unknown operation code → Log warning, skip event
- Table not in join graph → Log warning, skip (might be unrelated table)
- Tombstone event (null payload after delete) → Skip silently

## Configuration Schema

```yaml
database:
  host: localhost
  port: 5432
  dbname: mydb
  user: postgres
  password: secret

tracker:
  base_table: customers
  tracking_table: customers_to_reprocess
  sql_query: |
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
```

## Implementation Steps

### Phase 1: Core Infrastructure
1. **Project setup**
   - Create `pyproject.toml` with dependencies: `sqlglot`, `psycopg2-binary`, `pyyaml`, `click`
   - Optional: `kafka-python` if streaming directly from Kafka
   - Create package structure: `src/cdc_dependency_tracker/`
   - Setup entry point: `cdc-tracker` command

2. **Config module** (`config.py`)
   - Pydantic models for validation
   - YAML loader
   - Database connection factory

3. **SQL parser** (`sql_parser.py`)
   - Use `sqlglot` to parse SQL query
   - Extract JOIN conditions with table aliases resolved
   - Build bidirectional join graph dict
   - Extract WHERE clause filters

### Phase 2: Resolution Engine
4. **Dependency graph** (`dependency_graph.py`)
   - Graph representation with forward/reverse edges
   - Method: `get_join_keys_for_table(table) -> List[column]`
   - Method: `get_parent_join(table) -> (parent_table, join_col, parent_col)`
   - Method: `get_path_to_base(table) -> List[join_spec]`

5. **CDC handler** (`cdc_handler.py`)
   - Parse Debezium JSON from stdin
   - Validate event structure (source, op, before/after fields)
   - Extract table name from `source.table`
   - Map operation code (`op`) to logical operation
   - Extract before/after data (handle null values)
   - Support both envelope and schema-less formats

6. **Resolver** (`resolver.py`)
   - Implement `resolve_affected_customers()` with UPDATE logic
   - Implement `traverse_to_base()` with database queries
   - Apply _deleted filters from WHERE clause
   - Batch queries for performance (IN clause with multiple IDs)

### Phase 3: Database Operations
7. **Database client** (`db_client.py`)
   - Connection pooling (use `psycopg2.pool`)
   - Execute SELECT queries with parameters
   - Execute INSERT into tracking table with ON CONFLICT
   - Transaction management

8. **Tracking writer** (`tracking.py`)
   - Batch insert customer IDs
   - Handle conflicts with timestamp update
   - Log number of customers tracked

### Phase 4: CLI & Integration
9. **CLI** (`cli.py`)
   - Click command: `cdc-tracker --config path/to/config.yaml [--schema-filter public]`
   - Read Debezium CDC event from stdin
   - Optional: Filter by schema name from `source.schema`
   - Orchestrate: parse config → build graph → handle event → resolve → track
   - Exit codes: 0 (success), 1 (error), 2 (skipped - filtered out)
   - Logging to stderr (INFO/ERROR levels)

10. **Error handling**
    - Graceful handling of all edge cases listed above
    - Clear error messages
    - Validation at each stage

### Phase 5: Testing & Documentation
11. **Unit tests** (`tests/`)
    - Test SQL parser with various JOIN patterns
    - Test dependency graph traversal
    - Test UPDATE logic with changed/unchanged join keys
    - Test Debezium event parsing (all operation codes)
    - Test handling of null before/after fields
    - Test tombstone event filtering
    - Mock database for resolver tests

12. **Integration tests**
    - Use testcontainers for PostgreSQL with Debezium
    - Insert test data for all 4 tables
    - Generate Debezium CDC events or use captured samples
    - Feed CDC events via stdin
    - Verify tracking table contents

13. **Documentation** (`README.md`)
    - Installation instructions
    - Config file example
    - Debezium CDC event format and field mapping
    - Tracking table schema
    - Usage examples with Debezium connectors
    - Integration with Kafka Connect
    - Troubleshooting guide

## File Structure
```
cdc-dependency-tracker/
├── pyproject.toml
├── README.md
├── DESIGN.md
├── IMPLEMENTATION_PLAN.md
├── src/
│   └── cdc_dependency_tracker/
│       ├── __init__.py
│       ├── cli.py
│       ├── config.py
│       ├── sql_parser.py
│       ├── dependency_graph.py
│       ├── cdc_handler.py
│       ├── resolver.py
│       ├── db_client.py
│       └── tracking.py
├── tests/
│   ├── __init__.py
│   ├── test_sql_parser.py
│   ├── test_dependency_graph.py
│   ├── test_resolver.py
│   ├── test_cdc_handler.py
│   └── test_integration.py
└── examples/
    ├── config.yaml
    ├── sample_debezium_insert.json
    ├── sample_debezium_update.json
    ├── sample_debezium_delete.json
    └── kafka_integration.sh
```

## Verification Checklist

### All Change Scenarios Covered?
- [x] Customer INSERT/UPDATE/DELETE
- [x] Order INSERT with new customer reference
- [x] Order UPDATE with cust_id change (mutable join key)
- [x] Order UPDATE without cust_id change
- [x] Order DELETE
- [x] Order line INSERT
- [x] Order line UPDATE with order_id change
- [x] Order line UPDATE with product_id change
- [x] Order line UPDATE with both join keys changed
- [x] Order line UPDATE with no join keys changed
- [x] Order line DELETE
- [x] Product INSERT (no tracking needed)
- [x] Product UPDATE (multi-hop traversal)
- [x] Product DELETE

### Resolution Correctness
- [x] INSERT uses `after` data
- [x] DELETE uses `before` data
- [x] UPDATE detects changed join keys and queries both paths
- [x] UPDATE with unchanged join keys uses `after` data only
- [x] Multi-hop queries apply _deleted filters at each level
- [x] All unique customer IDs collected across multiple resolution paths
- [x] Base table changes tracked directly

### Missing Scenarios?
None identified. All DML operations (INSERT/UPDATE/DELETE) on all tables (customers, orders, order_lines, products) are covered with proper handling of mutable join keys.

## Usage Example with Debezium

### Kafka Connect Pipeline
```bash
# Consume from Debezium Kafka topic and process each event
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic mydb.public.order_lines \
  --from-beginning | \
  cdc-tracker --config config.yaml
```

### Debezium Engine (Embedded)
```bash
# Process Debezium events from file
cat debezium_event.json | cdc-tracker --config config.yaml
```

### Example Debezium Event
```bash
echo '{
  "before": null,
  "after": {
    "_id": "OL123",
    "order_id": "O456",
    "product_id": "P789",
    "quantity": 5,
    "unit_price": 29.99,
    "_deleted": false
  },
  "source": {
    "connector": "postgresql",
    "table": "order_lines",
    "schema": "public"
  },
  "op": "c",
  "ts_ms": 1707649200000
}' | cdc-tracker --config config.yaml
```

## Open Questions
1. **Performance**: Should we add query result caching for repeated lookups within same CDC batch?
2. **Monitoring**: Should tool emit metrics (# customers tracked, query execution time)?
3. **Dry run mode**: Add `--dry-run` flag to show what would be tracked without writing?
4. **Batch processing**: Support reading multiple CDC events from stdin (JSONL format)?
5. **Debezium variants**: Support both with and without schema registry (Avro vs JSON)?
