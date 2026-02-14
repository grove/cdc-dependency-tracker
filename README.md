# CDC Dependency Tracker

A Python tool for tracking database dependencies via Change Data Capture (CDC) events. Uses an adaptive hybrid strategy to efficiently propagate changes from dependent tables to a base table.

## Features

- **PostgreSQL Streaming**: Native logical replication support with pgoutput plugin
- **Adaptive Routing**: Automatically routes events to immediate or deferred processing based on complexity
- **Hybrid Strategy**: 90% immediate resolution (low latency), 10% batched (high efficiency)
- **SQL Parser**: Automatically extracts join relationships from SQL queries
- **Two-Level Tracking**: Intermediate staging for expensive resolutions
- **Background Percolation**: Daemon for batch resolution of deferred items
- **Transaction Isolation**: Uses REPEATABLE READ for consistent multi-hop queries

## Architecture

```
PostgreSQL WAL → Replication Slot → pgoutput → CDC Event → Routing Engine

Routing Engine:
    ↓
    Immediate (90%) → customers_to_reprocess
    ↓
    Deferred (10%) → intermediate_to_track
                          ↓
    Percolator (background) → customers_to_reprocess
```

## Installation

### Prerequisites

- Python 3.10+
- PostgreSQL database with logical replication enabled (`wal_level = logical`)
- pgoutput plugin (included with PostgreSQL 10+)

### Install

```bash
cd cdc-dependency-tracker
pip install -e .

# For development
pip install -e ".[dev]"
```

## Configuration

Create a `config.yaml` file:

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
  
  # Adaptive routing thresholds
  immediate_fanout_threshold: 100
  
  # Background percolation settings
  percolation_interval_seconds: 30
  percolation_batch_size: 1000
  
  sql_query: |
    SELECT 
        c._id as customer_id,
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

**Note**: The `base_table` and `tracking_table` are fully configurable. This example tracks `customers`, but you can track any base entity (e.g., `users`, `accounts`, etc.) by changing these values. The `sql_query` defines the join relationships between your tables.

**Important**: The tracking table's ID column name is automatically extracted from the SQL query's SELECT clause. For example, if your query has `SELECT c._id as customer_id FROM customers c`, the tracking table will use the column name `customer_id`. If the ID column is not found in the SELECT clause, the system falls back to a naming heuristic (e.g., `customers` → `customer_id`).

## Database Setup

Create the tracking tables:

```bash
python -c "
from src.cdc_dependency_tracker.db_client import DatabaseClient
from src.cdc_dependency_tracker.schema import create_schema
from src.cdc_dependency_tracker.config import Config

cfg = Config.from_yaml('examples/config.yaml')
db = DatabaseClient(cfg.database.to_connection_params())
with db.transaction() as cur:
    create_schema(cur)
print('Tracking tables created successfully')
db.close()
"
```

Or manually execute the SQL from `src/cdc_dependency_tracker/schema.py`.

## Usage

### Stream CDC Events from PostgreSQL

Stream CDC events directly from PostgreSQL using logical replication:

```bash
# Start streaming from replication slot
cdc-tracker stream --config config.yaml

# With verbose logging
cdc-tracker stream --config config.yaml --verbose

# With schema filter
cdc-tracker stream --config config.yaml --schema-filter public
```

**Prerequisites:**
1. PostgreSQL configured for logical replication (`wal_level = logical`)
2. User with REPLICATION privilege
3. *(Optional)* Replication slot and publication - **auto-created by default!**

**Easy setup:** The tool automatically creates the replication slot and publication on first run using pgoutput (PostgreSQL's native logical replication plugin). Just configure replication settings and start streaming!

See [STREAMING.md](STREAMING.md) for complete setup instructions.

**Configuration for streaming:**

```yaml
# Add to config.yaml
replication:
  enabled: true
  slot_name: cdc_slot
  plugin: pgoutput
  host: localhost
  port: 5432
  dbname: mydb
  user: replication_user
  password: replication_password
  
  # Auto-creation (enabled by default)
  auto_create_slot: true          # Creates slot if missing
  auto_create_publication: true   # Creates publication if missing
  publication_name: cdc_pub        # Publication name to use/create
  
  ack_interval_seconds: 10
  max_batch_size: 100
```

See [STREAMING.md](STREAMING.md) for complete setup instructions.

### Background Percolation

Run the percolation daemon to process deferred items:

```bash
# Continuous daemon
cdc-percolator --config examples/config.yaml

# Run once and exit
cdc-percolator --config examples/config.yaml --once

# With cleanup of old items
cdc-percolator --config examples/config.yaml --cleanup
```

### Monitoring

Check tracking queue status:

```sql
-- Pending customers to reprocess
SELECT COUNT(*) FROM customers_to_reprocess;

-- Pending intermediate items
SELECT COUNT(*) FROM intermediate_to_track WHERE percolated = FALSE;

-- Percolation lag
SELECT EXTRACT(EPOCH FROM (NOW() - MIN(tracked_at)))::INTEGER as lag_seconds
FROM intermediate_to_track WHERE percolated = FALSE;

-- Processing breakdown
SELECT table_name, depth, COUNT(*) 
FROM intermediate_to_track 
WHERE percolated = FALSE 
GROUP BY table_name, depth;
```

## Event Routing Logic

| Table | Distance | Strategy | Latency | Queries |
|-------|----------|----------|---------|---------|
| customers | 0 hops | Immediate | <1ms | 0 |
| orders | 1 hop | Immediate | <1ms | 0 |
| order_lines | 2 hops | Adaptive | <5ms or 30s | 0-1 |
| products | 3 hops | Deferred | 30s | 0 |

### Adaptive Logic for Order Lines

- **Immediate** if: No join keys changed AND simple resolution
- **Deferred** if: Join keys changed (order_id, product_id) OR high complexity

## Performance

### Typical E-commerce Workload (1000 events/min)

**Without Adaptive Routing:**
- 1,500 queries/min
- 20,000 inserts/min
- Variable latency (1-100ms)

**With Adaptive Routing:**
- 160 queries/min (90% reduction)
- 1,000 inserts/min (95% reduction)
- <5ms for 90% of events
- 30s for 10% of events

## Configuration Tuning

### Low Latency (Real-time Priority)
```yaml
tracker:
  immediate_fanout_threshold: 1000  # Resolve more immediately
  percolation_interval_seconds: 5   # Fast percolation
  percolation_batch_size: 100       # Small batches
```

### High Throughput (Efficiency Priority)
```yaml
tracker:
  immediate_fanout_threshold: 10    # Defer more aggressively
  percolation_interval_seconds: 60  # Slower percolation
  percolation_batch_size: 5000      # Large batches
```

### Balanced (Recommended)
```yaml
tracker:
  immediate_fanout_threshold: 100
  percolation_interval_seconds: 30
  percolation_batch_size: 1000
```

## Development

### Run Tests

```bash
pytest tests/ -v
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint
ruff check src/ tests/

# Type check
mypy src/
```

## Troubleshooting

### High Percolation Lag

**Symptom:** Oldest unpercolated item > 120 seconds

**Solutions:**
- Increase `percolation_interval_seconds` frequency (e.g., 10s)
- Increase `percolation_batch_size` (e.g., 5000)
- Scale up database resources
- Check for missing indexes on join columns

### High Queue Depth

**Symptom:** `intermediate_to_track` unpercolated count > 10,000

**Solutions:**
- Run multiple percolation batches: `cdc-percolator --once` repeatedly
- Increase batch size in config
- Optimize percolation SQL query
- Add indexes: `order_lines(product_id)`, `orders(cust_id)`

### Missing Customers

**Symptom:** Expected customers not in tracking table

**Solutions:**
- Check `_deleted` flags in source data
- Verify foreign key relationships exist
- Check percolator logs for errors
- Verify SQL query matches actual schema

## License

MIT

## See Also

- [DESIGN.md](DESIGN.md) - Original design document
- [IMPLEMENTATION_PLAN_HYBRID.md](IMPLEMENTATION_PLAN_HYBRID.md) - Detailed implementation plan
- [STREAMING.md](STREAMING.md) - PostgreSQL logical replication streaming setup guide
