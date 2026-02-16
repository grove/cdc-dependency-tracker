# CDC Dependency Tracker

![Python Version](https://img.shields.io/badge/python-3.12%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)

A Python tool for tracking database dependencies via Change Data Capture (CDC) events. Uses an adaptive hybrid strategy to efficiently propagate changes from dependent tables to a base table.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Database Setup](#database-setup)
- [Usage](#usage)
- [Event Routing Logic](#event-routing-logic)
- [Performance](#performance)
- [Configuration Tuning](#configuration-tuning)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Documentation](#documentation)
- [Contributing](#contributing)

## Features

- **High-Performance CDC Streaming**: Rust-powered PostgreSQL logical replication using [pgoutput-decoder](https://pypi.org/project/pgoutput-decoder/)
- **Native Async Support**: Background async event loop in a thread, synchronous application code
- **Adaptive Routing**: Automatically routes events to immediate or deferred processing based on complexity
- **Hybrid Strategy**: 90% immediate resolution (low latency), 10% batched (high efficiency)
- **SQL Parser**: Automatically extracts join relationships from SQL queries
- **Two-Level Tracking**: Intermediate staging for expensive resolutions
- **Background Percolation**: Daemon for batch resolution of deferred items
- **Transaction Isolation**: Uses REPEATABLE READ for consistent multi-hop queries
- **Auto-Setup**: Automatically creates replication slots and publications
- **Zero-Copy Decoding**: Minimal allocations for high-throughput CDC scenarios

## Architecture

```
PostgreSQL WAL → Replication Slot → pgoutput Plugin
                                          ↓
                            pgoutput-decoder (Rust)
                                          ↓
                          Background Thread (asyncio)
                                          ↓
                            Thread-Safe Queue
                                          ↓
                          Synchronous Application
                                          ↓
                              Routing Engine
                                    ↓
              ┌─────────────────────┴──────────────────┐
              ↓                                        ↓
    Immediate (90%)                           Deferred (10%)
              ↓                                        ↓
    customers_to_reprocess                  intermediate_to_track
                                                       ↓
                                       Percolator (background)
                                                       ↓
                                            customers_to_reprocess
```

### CDC Streaming Architecture

The tool uses a hybrid synchronous/asynchronous architecture for optimal performance:

1. **Background Thread**: Runs an asyncio event loop with [pgoutput-decoder](https://pypi.org/project/pgoutput-decoder/)
2. **Rust Performance**: Binary protocol decoding happens in Rust for maximum efficiency
3. **Thread-Safe Queue**: Bridges async CDC messages to synchronous application code
4. **No Application Changes**: Rest of the codebase remains synchronous and easy to maintain

### Performance Benefits

Compared to pure Python CDC implementations:
- **Zero-copy decoding**: Minimal memory allocations in hot path
- **Native async**: True async/await powered by Rust's tokio runtime
- **Type-safe conversions**: Comprehensive PostgreSQL type support
- **Auto-reconnect**: Exponential backoff for connection failures
- **Debezium-compatible**: Drop-in compatible with existing Debezium pipelines

## Quick Start

```bash
# 1. Install
uv pip install -e .

# 2. Configure
cp examples/config.yaml config.yaml
# Edit config.yaml with your database settings

# 3. Setup database
python setup_db.py

# 4. Start streaming CDC events
cdc-tracker stream --config config.yaml

# 5. (Optional) Start background percolator
cdc-percolator --config config.yaml
```

See [QUICKSTART.md](QUICKSTART.md) for detailed setup instructions.

## Installation

### Prerequisites

- **Python 3.12+** (Python 3.13 recommended - Python 3.14 has compatibility issues with pgoutput-decoder)
- PostgreSQL 12+ with logical replication enabled (`wal_level = logical`)
- User with REPLICATION privilege
- pgoutput plugin (included with PostgreSQL 10+)

### Install

```bash
# Clone the repository
git clone https://github.com/yourusername/cdc-dependency-tracker.git
cd cdc-dependency-tracker

# Install the package
uv pip install -e .

# For development (includes pytest, ruff, ty, testcontainers)
uv pip install -e ".[dev]"
```

### Dependencies

The tool uses the high-performance [pgoutput-decoder](https://pypi.org/project/pgoutput-decoder/) library for CDC streaming:
- Rust-powered binary protocol decoding
- Native async support with tokio-postgres
- Debezium-compatible message format
- Auto-reconnect with exponential backoff

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

The tool requires three tracking tables:
- `customers_to_reprocess` - Queue of base entities that need reprocessing
- `intermediate_to_track` - Staging area for deferred dependency resolution
- `percolation_runs` - Log of background percolation batches

### Automated Setup

Use the provided setup script:

```bash
python setup_db.py
```

This creates all necessary tracking tables with proper indexes and constraints.

### Manual Setup

Or create tables manually:

```python
from src.cdc_dependency_tracker.db_client import DatabaseClient
from src.cdc_dependency_tracker.schema import create_schema
from src.cdc_dependency_tracker.config import Config

cfg = Config.from_yaml('examples/config.yaml')
db = DatabaseClient(cfg.database.to_connection_params())
with db.transaction() as cur:
    create_schema(cur)
print('Tracking tables created successfully')
db.close()
```

The schema is defined in `src/cdc_dependency_tracker/schema.py` if you need to customize it.

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
# Run all tests
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=src/cdc_dependency_tracker --cov-report=term-missing

# Run specific test file
pytest tests/test_e2e_customers_v2.py -v

# Run E2E tests only (requires Docker)
pytest tests/ -v --tb=short -m e2e

# Run unit tests only (exclude E2E)
pytest tests/ -v --tb=short -m "not e2e"
```

**Test Coverage:**
- **35 total tests** (9 E2E integration tests + 26 unit tests)
- E2E tests use [testcontainers](https://testcontainers-python.readthedocs.io/) for ephemeral PostgreSQL instances
- E2E tests pin Testcontainers image to `postgres:18.1-alpine`
- Tests cover CDC streaming, dependency graph traversal, routing logic, and percolation
- Function-scoped CDC fixtures ensure test isolation

### Code Quality

```bash
# Format code
ruff format src/ tests/

# Lint
ruff check src/ tests/

# Type check
ty check src/

# Run all quality checks
ruff format src/ tests/ && ruff check src/ tests/ && ty check src/
```

## Troubleshooting

### Python Version Compatibility

**Symptom:** Segmentation fault when running tests or streaming CDC events

**Solution:**
- Use Python 3.12 or 3.13 (recommended)
- Avoid Python 3.14 - pgoutput-decoder library has compatibility issues with Python 3.14
- Check your Python version: `python3 --version`
- Recreate virtual environment with correct Python version:
  ```bash
  rm -rf .venv
  python3.13 -m venv .venv
  source .venv/bin/activate
  uv pip install -e ".[dev]"
  ```

### CDC Streaming Issues

**Symptom:** "module 'pgoutput_decoder' has no attribute 'PgOutputDecoder'"

**Solution:**
- Ensure pgoutput-decoder is installed: `uv pip install "pgoutput-decoder>=0.1.0"`
- Verify installation: `python3 -c "import pgoutput_decoder; print(pgoutput_decoder.__version__)"`
- Check that you're using the correct Python version (3.12 or 3.13)

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

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Getting started guide with step-by-step setup
- **[DESIGN.md](DESIGN.md)** - Original design document and architectural decisions
- **[IMPLEMENTATION_PLAN_HYBRID.md](IMPLEMENTATION_PLAN_HYBRID.md)** - Detailed hybrid strategy implementation
- **[STREAMING.md](STREAMING.md)** - PostgreSQL logical replication setup and configuration
- **[REFACTORING_SUMMARY.md](REFACTORING_SUMMARY.md)** - Recent architectural improvements

## External Resources

- [pgoutput-decoder](https://pypi.org/project/pgoutput-decoder/) - High-performance Rust-based PostgreSQL CDC library
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html) - Official documentation
- [Debezium](https://debezium.io/) - Open-source CDC platform (message format compatible)
- [testcontainers-python](https://testcontainers-python.readthedocs.io/) - Integration testing with Docker

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `pytest tests/ -v`
5. Run code quality checks: `ruff format src/ tests/ && ruff check src/ tests/`
6. Submit a pull request

## Architecture Highlights

- **Adaptive Routing**: Automatically switches between immediate and deferred processing based on event complexity
- **Background Percolation**: Batch processes deferred items to amortize expensive queries
- **SQL-Based Configuration**: Define dependencies through SQL queries, not code
- **Zero Downtime**: Stream processing continues during percolation batches
- **Transaction Safety**: REPEATABLE READ isolation ensures consistent multi-hop queries

---

**Built with ❤️ using Python and Rust**
