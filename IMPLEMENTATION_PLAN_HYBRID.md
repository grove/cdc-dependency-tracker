# CDC Dependency Tracker - Hybrid Implementation Plan

## Overview
Python CLI tool that intelligently routes CDC events using an adaptive hybrid strategy:
- **Immediate resolution** for cheap operations (customers, orders, simple order lines)
- **Deferred batching** for expensive operations (complex order lines, products)
- **Two-level tracking** (intermediate + final) instead of per-table tracking
- **Background percolation** for batched items with tunable frequency

This approach optimizes for both **low latency** (90% of events) and **high efficiency** (expensive 10% of events).

## Architecture

### Tracking Tables

```sql
-- Intermediate staging for expensive resolutions
CREATE TABLE intermediate_to_track (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    depth INTEGER NOT NULL,  -- 1=order_lines, 2=products
    tracked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    percolated BOOLEAN DEFAULT FALSE,
    UNIQUE(table_name, entity_id)
);

CREATE INDEX idx_intermediate_pending ON intermediate_to_track (percolated, depth) 
WHERE percolated = FALSE;

-- Final destination for affected customers
CREATE TABLE customers_to_reprocess (
    customer_id VARCHAR PRIMARY KEY,
    last_tracked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Decision Flow

```
CDC Event → Determine Table
    ↓
    ├─ customers → INSERT customers_to_reprocess (immediate, O(1))
    ├─ orders → INSERT customers_to_reprocess (immediate, O(1), cust_id in event)
    ├─ order_lines → Estimate fanout
    │       ├─ Small (<100) → Query order → INSERT customers (immediate, O(1))
    │       └─ Large (≥100) → INSERT intermediate_to_track (defer)
    └─ products → INSERT intermediate_to_track (always defer)

Background Job (every 10-30s):
    intermediate_to_track → Batch query → customers_to_reprocess
```

## Adaptive Routing Logic

### Configuration
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
  immediate_fanout_threshold: 100  # Max entities to resolve immediately
  
  # Background percolation settings
  percolation_interval_seconds: 30
  percolation_batch_size: 1000
  
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

### Event Processing Logic

```python
IMMEDIATE_THRESHOLD = config.immediate_fanout_threshold

def handle_cdc_event(event: DebeziumEvent) -> ProcessingResult:
    """
    Route CDC event based on table and estimated cost.
    Returns: immediate_tracked, deferred_tracked, query_count
    """
    table = event['source']['table']
    op = event['op']
    before = event.get('before')
    after = event.get('after')
    
    if table == 'customers':
        return handle_customers_immediate(op, before, after)
    
    elif table == 'orders':
        return handle_orders_immediate(op, before, after)
    
    elif table == 'order_lines':
        return handle_order_lines_adaptive(op, before, after)
    
    elif table == 'products':
        return handle_products_deferred(op, before, after)
    
    else:
        log.warning(f"Unknown table: {table}, skipping")
        return ProcessingResult(skipped=True)


def handle_customers_immediate(op, before, after):
    """Customers: Base table, always immediate"""
    if op == 'd':
        customer_id = before['_id']
    else:
        customer_id = after['_id']
    
    track_customer_immediate(customer_id)
    return ProcessingResult(immediate=1, queries=0)


def handle_orders_immediate(op, before, after):
    """Orders: 0-hop (cust_id in event), always immediate"""
    customers = set()
    
    if op == 'c' or op == 'r':  # INSERT or snapshot
        customers.add(after['cust_id'])
    
    elif op == 'u':  # UPDATE
        # Check if cust_id changed (mutable join key)
        if before['cust_id'] != after['cust_id']:
            customers.add(before['cust_id'])  # Old customer
            customers.add(after['cust_id'])   # New customer
        else:
            customers.add(after['cust_id'])
    
    elif op == 'd':  # DELETE
        customers.add(before['cust_id'])
    
    track_customers_immediate(customers)
    return ProcessingResult(immediate=len(customers), queries=0)


def handle_order_lines_adaptive(op, before, after):
    """Order lines: 1-hop, adaptive based on fanout"""
    
    # Check if join keys changed (requires both paths)
    if op == 'u':
        order_id_changed = before.get('order_id') != after.get('order_id')
        product_id_changed = before.get('product_id') != after.get('product_id')
        
        # If join keys changed, defer (complex resolution)
        if order_id_changed or product_id_changed:
            entity_id = after['_id']
            insert_intermediate_tracking('order_lines', entity_id, depth=1)
            return ProcessingResult(deferred=1, queries=1)
    
    # Estimate fanout for immediate resolution
    if op == 'd':
        order_id = before['order_id']
    else:
        order_id = after['order_id']
    
    # Quick check: does this order exist and how complex is it?
    estimated_impact = estimate_order_complexity(order_id)
    
    if estimated_impact > IMMEDIATE_THRESHOLD:
        # High fanout or complex: defer
        entity_id = after['_id'] if after else before['_id']
        insert_intermediate_tracking('order_lines', entity_id, depth=1)
        return ProcessingResult(deferred=1, queries=1)
    
    # Low impact: resolve immediately
    cust_id = query_customer_for_order(order_id)
    if cust_id:
        track_customer_immediate(cust_id)
        return ProcessingResult(immediate=1, queries=1)
    else:
        # Orphaned order line, no customer to track
        return ProcessingResult(skipped=True, queries=1)


def handle_products_deferred(op, before, after):
    """Products: 2-hop, high fanout, always defer"""
    if op == 'd':
        product_id = before['_id']
    else:
        product_id = after['_id']
    
    insert_intermediate_tracking('products', product_id, depth=2)
    return ProcessingResult(deferred=1, queries=0)


def estimate_order_complexity(order_id: str) -> int:
    """
    Quick estimate of resolution complexity.
    Returns small number if cheap, large if expensive.
    """
    # Could check:
    # - Number of order lines for this order
    # - If order still exists (_deleted = FALSE)
    # For now, simple existence check
    with db.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM order_lines 
            WHERE order_id = %s AND _deleted = FALSE
            LIMIT %s
        """, (order_id, IMMEDIATE_THRESHOLD + 1))
        count = cur.fetchone()[0]
        return count
```

## Background Percolation

### Percolation Strategy

**Single unified query handles all intermediate items:**

```python
def percolate_intermediate_to_customers(batch_size=1000):
    """
    Batch resolve all pending intermediate items to customers.
    Handles both order_lines and products in single query.
    """
    with db.transaction(isolation_level='REPEATABLE READ') as tx:
        # Unified resolution query
        tx.execute("""
            INSERT INTO customers_to_reprocess (customer_id, last_tracked_at)
            SELECT DISTINCT o.cust_id, NOW()
            FROM intermediate_to_track it
            JOIN order_lines ol ON 
                (it.table_name = 'order_lines' AND ol._id = it.entity_id)
                OR (it.table_name = 'products' AND ol.product_id = it.entity_id)
            JOIN orders o ON o._id = ol.order_id
            WHERE it.percolated = FALSE
              AND ol._deleted = FALSE
              AND o._deleted = FALSE
            LIMIT %s
            ON CONFLICT (customer_id) 
            DO UPDATE SET last_tracked_at = CURRENT_TIMESTAMP
        """, (batch_size,))
        
        affected_customers = tx.rowcount
        
        # Mark items as percolated
        tx.execute("""
            UPDATE intermediate_to_track
            SET percolated = TRUE
            WHERE id IN (
                SELECT id FROM intermediate_to_track
                WHERE percolated = FALSE
                LIMIT %s
            )
        """, (batch_size,))
        
        percolated_items = tx.rowcount
        
        return {
            'customers_tracked': affected_customers,
            'items_percolated': percolated_items
        }


def background_percolation_loop(interval_seconds=30):
    """
    Run continuous percolation in background.
    Can be separate process or async task.
    """
    while True:
        try:
            result = percolate_intermediate_to_customers()
            log.info(f"Percolated {result['items_percolated']} items, "
                    f"tracked {result['customers_tracked']} customers")
            
            # Cleanup old percolated items (optional)
            if should_cleanup():
                cleanup_old_percolated_items(days=7)
            
        except Exception as e:
            log.error(f"Percolation error: {e}")
        
        time.sleep(interval_seconds)
```

### Cleanup Strategy

```sql
-- Periodically clean up old percolated items (retention: 7 days)
DELETE FROM intermediate_to_track
WHERE percolated = TRUE
  AND tracked_at < NOW() - INTERVAL '7 days';

-- Optional: Archive before deletion
INSERT INTO intermediate_to_track_archive
SELECT * FROM intermediate_to_track
WHERE percolated = TRUE
  AND tracked_at < NOW() - INTERVAL '7 days';
```

## Performance Characteristics

### Workload Distribution (Typical E-commerce)

| Event Type | Frequency | Strategy | Latency | Cost |
|------------|-----------|----------|---------|------|
| Customer update | 5% | Immediate | <1ms | 0 queries, 1 insert |
| Order created | 40% | Immediate | <1ms | 0 queries, 1 insert |
| Order updated | 10% | Immediate | <1ms | 0-2 queries, 1-2 inserts |
| Order line added | 30% | Adaptive | 1-5ms or defer | 0-1 queries |
| Order line updated | 10% | Adaptive | 1-5ms or defer | 0-1 queries |
| Product updated | 5% | Deferred | 10-30s latency | 0 queries immediate |

**90% of events**: Immediate resolution (low latency)  
**10% of events**: Deferred batching (high efficiency)

### Cost Comparison: 1000 CDC Events/Minute

**Original Immediate Multi-Hop Plan:**
```
Queries: 1000 × avg(1.5) = 1,500 queries/min
Inserts: 1000 × avg(20) = 20,000 inserts/min
Latency: Variable (1-100ms)
```

**Pure Multi-Table Percolation Plan:**
```
Queries: ~10 queries/min (background batch)
Inserts: 1,000 immediate + 500 batched = 1,500 inserts/min
Latency: 10-30s for all
```

**Hybrid Adaptive Plan:**
```
Queries: 150 immediate + 10 background = 160 queries/min
Inserts: 950 immediate + 50 batched = 1,000 inserts/min
Latency: <5ms for 90%, 10-30s for 10%

Total reduction: 90% fewer queries, 95% fewer inserts vs original
Best latency: 90% immediate vs 0% in pure batch
```

## Monitoring & Metrics

### Key Metrics to Track

```python
class ProcessingMetrics:
    immediate_count = 0
    deferred_count = 0
    query_count = 0
    total_latency_ms = 0
    
    # Per-table breakdown
    customers_immediate = 0
    orders_immediate = 0
    order_lines_immediate = 0
    order_lines_deferred = 0
    products_deferred = 0


def emit_metrics():
    """Emit metrics for monitoring"""
    return {
        'cdc_events_processed': metrics.immediate_count + metrics.deferred_count,
        'immediate_resolution_pct': metrics.immediate_count / total * 100,
        'avg_latency_ms': metrics.total_latency_ms / total,
        'queries_per_event': metrics.query_count / total,
        
        # Queue depth
        'intermediate_queue_depth': get_intermediate_queue_depth(),
        'customers_pending': get_customers_pending_count(),
        
        # Percolation metrics
        'percolation_lag_seconds': get_oldest_unpercolated_age(),
    }
```

### Health Checks

```sql
-- Alert if intermediate queue grows too large
SELECT COUNT(*) FROM intermediate_to_track WHERE percolated = FALSE;
-- Alert threshold: > 10,000

-- Alert if percolation lag is too high
SELECT EXTRACT(EPOCH FROM (NOW() - MIN(tracked_at)))
FROM intermediate_to_track 
WHERE percolated = FALSE;
-- Alert threshold: > 120 seconds
```

## Implementation Steps

### Phase 1: Core Infrastructure
1. **Project setup**
   - `pyproject.toml` with dependencies: `sqlglot`, `psycopg2-binary`, `pyyaml`, `click`
   - Package structure: `src/cdc_dependency_tracker/`
   - Entry points: `cdc-tracker` (CDC processor), `cdc-percolator` (background job)

2. **Config module** (`config.py`)
   - Pydantic models with hybrid strategy settings
   - Validation for thresholds and intervals
   - Database connection factory with connection pooling

3. **Database setup** (`schema.py`)
   - Migration script for tracking tables
   - Indexes creation
   - Cleanup job SQL templates

### Phase 2: SQL Parser & Graph (Same as Original)
4. **SQL parser** (`sql_parser.py`)
   - Use `sqlglot` to parse SQL query
   - Extract JOIN conditions with aliases resolved
   - Build bidirectional join graph

5. **Dependency graph** (`dependency_graph.py`)
   - Graph representation with path distance (depth)
   - Method: `get_join_keys_for_table(table)`
   - Method: `get_path_depth(table)` → 0=customers, 1=orders, 2=order_lines, 3=products

### Phase 3: Hybrid Resolution Engine
6. **CDC handler** (`cdc_handler.py`)
   - Parse Debezium JSON from stdin
   - Validate event structure
   - Extract table, operation, before/after

7. **Routing engine** (`routing.py`)
   - Implement `handle_cdc_event()` with table-based dispatch
   - Per-table handlers: customers, orders, order_lines, products
   - Fanout estimation logic for adaptive routing

8. **Immediate resolver** (`resolver_immediate.py`)
   - Simple 1-hop queries for order_lines
   - Direct tracking for orders (cust_id in event)
   - Handle mutable join keys (before/after comparison)

9. **Deferred tracker** (`tracker_deferred.py`)
   - Insert into `intermediate_to_track`
   - Track by table_name, entity_id, depth

### Phase 4: Background Percolation
10. **Percolation engine** (`percolator.py`)
    - Unified batch query for all intermediate items
    - Transaction handling with REPEATABLE READ
    - Batch size limiting
    - Error handling and retry logic

11. **Percolation scheduler** (`scheduler.py`)
    - Event loop with configurable interval
    - Graceful shutdown handling
    - Metrics emission

### Phase 5: Database Operations
12. **Database client** (`db_client.py`)
    - Connection pooling (`psycopg2.pool.ThreadedConnectionPool`)
    - Execute queries with parameters
    - Transaction context managers
    - Isolation level management

13. **Tracking writer** (`tracking.py`)
    - Batch insert customers with ON CONFLICT
    - Insert intermediate items
    - Efficient bulk operations

### Phase 6: CLI & Integration
14. **CDC CLI** (`cli.py`)
    - Command: `cdc-tracker --config config.yaml [--schema-filter public]`
    - Read Debezium event from stdin
    - Route and process event
    - Emit metrics and exit

15. **Percolator CLI** (`percolator_cli.py`)
    - Command: `cdc-percolator --config config.yaml [--once]`
    - Background loop or one-shot execution
    - Graceful shutdown on SIGTERM
    - Health check endpoint (optional)

16. **Error handling & logging**
    - Structured logging (JSON format)
    - Error categorization (retriable vs fatal)
    - Metrics for error rates

### Phase 7: Testing & Documentation
17. **Unit tests** (`tests/`)
    - Test routing logic with various fanout scenarios
    - Test immediate vs deferred decisions
    - Test percolation queries
    - Test Debezium event parsing
    - Mock database for isolated tests

18. **Integration tests**
    - TestContainers for PostgreSQL
    - End-to-end CDC event processing
    - Verify immediate vs deferred routing
    - Verify percolation correctness
    - Load test with high event volume

19. **Documentation** (`README.md`)
    - Architecture overview with diagrams
    - Configuration guide with tuning recommendations
    - Deployment guide (CDC processor + percolator)
    - Monitoring and alerting setup
    - Troubleshooting guide
    - Performance tuning guide

## Deployment Architecture

### Components

```
┌─────────────────┐
│ Debezium / CDC  │
│  Source (Kafka) │
└────────┬────────┘
         │ JSON events
         ↓
┌─────────────────────────────────────┐
│  CDC Processor (cdc-tracker)        │
│  - Reads from stdin/Kafka           │
│  - Routes based on table & fanout   │
│  - Immediate: track customers       │
│  - Deferred: insert intermediate    │
└────────┬────────────────────────────┘
         │
         ↓
┌─────────────────────────────────────┐
│  PostgreSQL                         │
│  ┌─────────────────────────────┐   │
│  │ intermediate_to_track       │   │
│  │ customers_to_reprocess      │   │
│  └─────────────────────────────┘   │
└────────┬────────────────────────────┘
         ↑
         │ Batch queries every 30s
┌────────┴────────────────────────────┐
│  Percolator (cdc-percolator)        │
│  - Background daemon                │
│  - Resolves intermediate → customers│
│  - Cleanup old items                │
└─────────────────────────────────────┘
```

### Scaling Considerations

**CDC Processor:**
- Stateless, can run multiple instances
- Each instance processes events independently
- ON CONFLICT in database handles deduplication
- Can scale horizontally with Kafka partitions

**Percolator:**
- Single active instance (or leader election)
- Can run multiple for redundancy (with locking)
- Batch size and interval tunable per deployment

**Database:**
- Connection pooling (10-50 connections per processor)
- Indexes on tracking tables critical
- Partition `intermediate_to_track` by tracked_at for archival

## Configuration Tuning Guide

### Low Latency Priority
```yaml
tracker:
  immediate_fanout_threshold: 1000  # Resolve more immediately
  percolation_interval_seconds: 5   # Fast percolation
  percolation_batch_size: 100       # Small batches
```
**Result:** 95% immediate, 5s max latency, higher DB load

### High Throughput Priority
```yaml
tracker:
  immediate_fanout_threshold: 10    # Defer more aggressively
  percolation_interval_seconds: 60  # Slower percolation
  percolation_batch_size: 5000      # Large batches
```
**Result:** 70% immediate, 60s max latency, lower DB load

### Balanced (Recommended)
```yaml
tracker:
  immediate_fanout_threshold: 100
  percolation_interval_seconds: 30
  percolation_batch_size: 1000
```
**Result:** 90% immediate, 30s max latency, moderate DB load

## Edge Cases & Considerations

### Handled Cases
✅ **Mutable join keys** - Detected in adaptive routing, deferred if changed  
✅ **High fanout** - Adaptive threshold defers expensive operations  
✅ **Duplicate tracking** - ON CONFLICT updates timestamp  
✅ **Out-of-order events** - Repeatable Read isolation + idempotent inserts  
✅ **Orphaned references** - Graceful handling (no customer tracked)  
✅ **Race conditions** - Transaction isolation prevents inconsistencies  
✅ **Event replay** - Idempotent due to ON CONFLICT

### Monitoring Alerts

**Queue Depth Alert:**
```
intermediate_to_track unpercolated count > 10,000
→ Increase percolation frequency or batch size
→ Scale up percolator resources
```

**Latency Alert:**
```
Oldest unpercolated item > 120 seconds
→ Check percolator health
→ Check database performance
→ Verify percolation queries have proper indexes
```

**Error Rate Alert:**
```
Failed CDC events > 1% of total
→ Check database connectivity
→ Review error logs for patterns
→ Validate Debezium event schema
```

## Summary

The hybrid adaptive approach provides:

1. **Best latency** for common operations (90% immediate)
2. **Best efficiency** for expensive operations (10% batched)
3. **Simpler schema** than full multi-table tracking (only 2 tables)
4. **Tunable behavior** via configuration (latency vs throughput)
5. **Scalable** horizontally (stateless processors)
6. **Observable** with clear metrics and health checks

This strikes the optimal balance between the immediate multi-hop approach (too expensive) and pure percolation approach (too slow).
