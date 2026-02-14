# Quick Start Guide

## Prerequisites

- Python 3.10+
- PostgreSQL database running
- pip installed

## Step 1: Install Dependencies

```bash
cd cdc-dependency-tracker
pip install -e .
```

## Step 2: Configure Database

Edit `examples/config.yaml` with your database credentials:

```yaml
database:
  host: localhost
  port: 5432
  dbname: mydb
  user: postgres
  password: your_password
```

## Step 3: Create Database Tables

Run the setup script to create tracking tables:

```bash
python setup_db.py --config examples/config.yaml
```

This creates:
- `intermediate_to_track` - Staging for deferred resolutions
- `customers_to_reprocess` - Final tracking table
- `intermediate_to_track_archive` - Optional archive

## Step 4: Configure Logical Replication

Add replication configuration to `examples/config.yaml`:

```yaml
replication:
  enabled: true
  slot_name: cdc_slot
  plugin: pgoutput
  host: localhost
  port: 5432
  dbname: mydb
  user: postgres
  password: your_password
  auto_create_slot: true
  auto_create_publication: true
```

## Step 5: Start Streaming

Start consuming CDC events from PostgreSQL:

```bash
# Start streaming (will auto-create slot and publication)
cdc-tracker stream --config examples/config.yaml

# With verbose logging
cdc-tracker stream --config examples/config.yaml --verbose
```

## Step 6: Run Percolation

Process deferred items in batch:

```bash
# Run once
cdc-percolator --config examples/config.yaml --once

# Or run as daemon
cdc-percolator --config examples/config.yaml
```

## Step 7: Verify Results

Check the tracking tables:

```sql
-- See tracked customers
SELECT * FROM customers_to_reprocess;

-- See pending intermediate items
SELECT * FROM intermediate_to_track WHERE percolated = FALSE;

-- Check percolation lag
SELECT EXTRACT(EPOCH FROM (NOW() - MIN(tracked_at)))::INTEGER as lag_seconds
FROM intermediate_to_track WHERE percolated = FALSE;
```

## Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Check coverage
pytest tests/ --cov=cdc_dependency_tracker
```

## Next Steps

1. **Production Setup**: 
   - Deploy `cdc-tracker` as a service consuming from Kafka
   - Deploy `cdc-percolator` as a background daemon
   - Set up monitoring and alerting

2. **Tuning**:
   - Adjust `immediate_fanout_threshold` based on your workload
   - Tune `percolation_interval_seconds` for latency vs efficiency
   - Monitor queue depth and percolation lag

3. **Scaling**:
   - Run multiple `cdc-tracker` instances (stateless)
   - Use leader election for `cdc-percolator` (single active)
   - Add database indexes on join columns

See [README.md](README.md) for detailed documentation.
