# Refactoring Summary: Generic Configuration-Driven Implementation

## Overview

The implementation has been refactored from being hardcoded to the "customers" example to being fully configuration-driven. The system now works with **any base table** and dependency structure defined in the configuration file.

## Key Changes

### 1. Database Client (`db_client.py`)
**Before**: Hardcoded methods for tracking customers
- `track_customer(customer_id)` 
- `track_customers(customer_ids)`
- `query_customer_for_order(order_id)`

**After**: Generic methods that work with any entity
- `track_entity(entity_id, id_column)` - Track single entity with configurable ID column
- `track_entities(entity_ids, id_column)` - Batch track entities with parameterized queries
- `query_parent_entity(parent_table, parent_col, child_col_value, filters)` - Generic parent traversal

**Impact**: Methods now accept the entity type and column names as parameters instead of assuming "customers" and "customer_id".

---

### 2. Routing Engine (`routing.py`)
**Before**: Hardcoded logic for customers/orders/order_lines/products
- Fixed join column names like "cust_id"
- Comments referencing specific tables
- Variable names like `customers`

**After**: Dynamic routing based on dependency graph
- Extracts join columns from `dependency_graph.get_parent_join()`
- Generic variable names like `base_entities`
- Comments reference depth levels, not specific tables
- Added `base_table_id_column` parameter to constructor

**Impact**: Can handle any table hierarchy defined in the SQL query configuration.

---

### 3. Percolation Engine (`percolator.py`)
**Before**: Hardcoded SQL query for customers/orders/order_lines/products
```sql
INSERT INTO customers_to_reprocess (customer_id, last_tracked_at)
SELECT DISTINCT o.cust_id, NOW()
FROM intermediate_to_track it
JOIN order_lines ol ON ...
JOIN orders o ON o._id = ol.order_id
```

**After**: Dynamic query builder using dependency graph
- `_build_percolation_query()` generates SQL from graph paths
- Walks dependency graph to build JOIN chains automatically
- Uses `UNION ALL` to combine paths from multiple tables
- Table names and column names from configuration

**Impact**: Percolation query adapts to any dependency structure without code changes.

---

### 4. CLI Tools (`cli.py`, `percolator_cli.py`)
**Before**: Passed hardcoded parameters to components

**After**: Derives parameters from configuration
- Calculates `base_id_column` from `base_table` name (e.g., "customers" → "customer_id")
- Passes `tracking_table` from config to `DatabaseClient`
- Builds `dependency_graph` from SQL query
- Passes all required parameters to components

**Impact**: CLIs are now configuration-driven, no code changes needed for different schemas.

---

### 5. Schema (`schema.py`)
**Before**: Fixed table name "customers_to_reprocess"
```sql
CREATE TABLE customers_to_reprocess (
    customer_id VARCHAR PRIMARY KEY,
    ...
);
```

**After**: Template-based schema with parameters
- `create_schema(cursor, tracking_table, id_column)` 
- Uses string formatting to inject table and column names
- Default values maintain backward compatibility

**Impact**: Schema creation adapts to any base table name.

---

### 6. Setup Script (`setup_db.py`)
**Before**: Created fixed "customers_to_reprocess" table

**After**: Reads configuration and creates matching table
- Derives `base_id_column` from config
- Passes to `create_schema()` and `drop_schema()`
- Validates correct tables were created

**Impact**: Database setup matches configuration automatically.

---

## ID Column Derivation Logic

The system automatically extracts the ID column name from the SQL query's SELECT clause:

**Primary Method: Parse from SQL**
1. Finds the base table's alias (e.g., `c` for `customers c`)
2. Locates columns from that alias with name `_id` or `id`
3. Returns the column's alias from the SELECT clause

```sql
SELECT c._id as customer_id, ...
FROM customers c
```
→ Extracts `customer_id` as the ID column name

**Fallback: Name-based heuristic** (when ID not in SELECT)
```python
if base_table.endswith('s'):
    base_id_column = base_table[:-1] + "_id"  # customers → customer_id
else:
    base_id_column = base_table + "_id"  # user → user_id
```

**Benefits:**
- Works with irregular table names (e.g., `people` → extracts `person_id` from SQL)
- Respects actual query structure instead of assuming naming conventions
- Clear logging indicates which method was used: "parsed from SQL" or "derived from table name"

---

## Configuration Examples

### Example 1: Original Customers Schema
```yaml
tracker:
  base_table: customers
  tracking_table: customers_to_reprocess
  sql_query: |
    SELECT c._id as customer_id, o._id, ol._id, p._id
    FROM customers c
    JOIN orders o ON c._id = o.cust_id
    JOIN order_lines ol ON o._id = ol.order_id
    JOIN products p ON ol.product_id = p._id
```
→ Creates table `customers_to_reprocess` with column `customer_id` (parsed from SQL)

### Example 2: Users Schema  
```yaml
tracker:
  base_table: users
  tracking_table: users_to_reprocess
  sql_query: |
    SELECT u._id as user_id, s._id, a._id
    FROM users u
    JOIN sessions s ON u._id = s.user_id
    JOIN activities a ON s._id = a.session_id
```
→ Creates table `users_to_reprocess` with column `user_id` (parsed from SQL)

### Example 3: Accounts Schema
```yaml
tracker:
  base_table: accounts
  tracking_table: accounts_to_reprocess
  sql_query: |
    SELECT a._id as account_id, t._id
    FROM accounts a
    JOIN transactions t ON a._id = t.account_id
```
→ Creates table `accounts_to_reprocess` with column `account_id` (parsed from SQL)

---

## What Remains Generic vs Hardcoded

### ✅ Now Generic (Configuration-Driven)
- Base table name
- Tracking table name  
- ID column names (extracted from SQL SELECT clause, with fallback to naming heuristic)
- Join relationships (parsed from SQL)
- Dependency depths (calculated from SQL)
- All SQL queries (built dynamically)

### ⚠️ Still Assumes
- Tables have `_id` or `id` as primary key column name (parsed from SELECT)
- Tables have `_deleted` flag for soft deletes
- Debezium CDC event format
- PostgreSQL database

These assumptions are documented and consistent across the codebase.

---

## Testing

All unit tests pass after refactoring:
```
16 tests passed in 0.07s
```

Tests validate:
- SQL parsing extracts correct JOINs
- **Base ID column extraction from SELECT clause (6 new tests)**
- Dependency graph calculates depths correctly
- CDC event parsing works with any table
- Parent join lookup is generic

New test cases added:
- Extract base ID column with alias (`c._id as customer_id` → `customer_id`)
- Extract base ID column without alias (`c._id` → `_id`)
- Handle missing base ID in SELECT (returns `None`, falls back to heuristic)
- No base table specified (returns `None`)
- Irregular table names (`people` → correctly extracts `person_id` from SQL)
- Support for `id` column (not just `_id`)

---

## Migration Path

**For existing deployments**: Configuration needs to be explicit now:

```yaml
# Must specify both base_table and tracking_table
tracker:
  base_table: customers
  tracking_table: customers_to_reprocess
```

**For new deployments**: Choose any base table and matching tracking table:

```yaml
tracker:
  base_table: users  # or accounts, products, etc.
  tracking_table: users_to_reprocess
```

---

## Benefits

1. **Reusability**: Same codebase works for any schema
2. **Maintainability**: No code changes when schema evolves
3. **Testing**: Can test with different schemas without modifying code
4. **Documentation**: Examples show flexibility clearly

## Next Steps (Future Enhancements)

1. Add explicit `base_id_column` to config for non-standard naming
2. Make `_id` and `_deleted` column names configurable
3. Add configuration validation to catch mistakes early
4. Add integration tests with different schemas
