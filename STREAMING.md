# PostgreSQL Logical Replication Streaming

This document explains how to set up and use the PostgreSQL logical replication streaming feature to consume CDC events directly from the database.

## Overview

The streaming mode uses PostgreSQL's logical replication to consume change events in real-time:

```
PostgreSQL WAL → Replication Slot → pgoutput Plugin → CDC Tracker
```

**Benefits:**
- No external Debezium infrastructure required
- Direct connection to PostgreSQL
- At-least-once delivery guarantee with WAL position tracking
- Lower latency than polling
- Automatic reconnection on network failures

## Prerequisites

### 1. PostgreSQL Configuration

Enable logical replication in `postgresql.conf`:

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

Restart PostgreSQL after changing these settings:

```bash
# macOS (Homebrew)
brew services restart postgresql@16

# Linux (systemd)
sudo systemctl restart postgresql
```

### 2. Database User Permissions

Create a user with replication privileges:

```sql
-- Create replication user
CREATE USER replication_user WITH REPLICATION PASSWORD 'secure_password';

-- Grant access to tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
GRANT USAGE ON SCHEMA public TO replication_user;

-- For future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT ON TABLES TO replication_user;
```

Update `pg_hba.conf` to allow replication connections:

```
# TYPE  DATABASE        USER                ADDRESS         METHOD
host    replication     replication_user    127.0.0.1/32    md5
host    replication     replication_user    ::1/128         md5
```

Reload configuration:

```bash
# macOS
brew services reload postgresql@16

# Linux
sudo systemctl reload postgresql
```

### 4. Create Publication (Optional - Auto-Created)

**Note:** Starting from the latest version, the CDC tracker can automatically create the publication if it doesn't exist. You can skip this step and let the tool handle it, or create it manually for more control.

A publication defines which tables' changes to stream:

```sql
-- Create publication for specific tables (manual)
CREATE PUBLICATION cdc_pub FOR TABLE customers, orders, order_lines, products;

-- Or for all tables
CREATE PUBLICATION cdc_pub FOR ALL TABLES;

-- Verify
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables WHERE pubname = 'cdc_pub';
```

**Automatic creation:** When `auto_create_publication: true` (default), the tracker will:
- Detect all tables from your SQL query's JOIN relationships
- Create a publication with those specific tables
- Use the `publication_name` from your config (default: "cdc_pub")

### 5. Create Replication Slot (Optional - Auto-Created)

**Note:** Starting from the latest version, the CDC tracker can automatically create the replication slot if it doesn't exist. You can skip this step and let the tool handle it.

A replication slot tracks the WAL position to prevent data loss:

```sql
-- Create slot with pgoutput plugin (manual)
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');

-- Verify
SELECT * FROM pg_replication_slots;
```

**Automatic creation:** When `auto_create_slot: true` (default), the tracker will:
- Check if the slot exists on startup
- Create it if missing using the configured `slot_name` and `plugin`
- Log the creation for visibility

**Important:** The slot will retain WAL data until consumed. Monitor disk space!

## Configuration

Update your `config.yaml` to enable replication:

```yaml
database:
  host: localhost
  port: 5432
  dbname: cdc-tracker
  user: geir.gronmo
  password: ""

tracker:
  base_table: customers
  tracking_table: customers_to_reprocess
  immediate_fanout_threshold: 100
  percolation_interval_seconds: 30
  percolation_batch_size: 1000
  sql_query: |
    SELECT c._id as customer_id, ...

# Enable replication streaming
replication:
  enabled: true
  slot_name: cdc_slot          # Name of replication slot to use/create
  plugin: pgoutput             # Output plugin (PostgreSQL native)
  
  # Replication connection
  host: localhost
  port: 5432
  dbname: cdc-tracker
  user: replication_user       # User with REPLICATION privilege
  password: secure_password
  
  # Auto-creation (recommended for ease of setup)
  auto_create_slot: true            # Create slot if it doesn't exist
  auto_create_publication: true     # Create publication if it doesn't exist
  publication_name: cdc_pub         # Name of publication to use/create
  
  # Tuning
  ack_interval_seconds: 10     # ACK every 10 seconds
  max_batch_size: 100          # Or after 100 messages
```

**Auto-creation settings:**

- `auto_create_slot: true` - Automatically creates the replication slot on first run if it doesn't exist
- `auto_create_publication: true` - Automatically creates the publication with tables from your SQL query
- `publication_name: cdc_pub` - Name of the publication to create or use

When auto-creation is enabled (default), you don't need to manually run the SQL commands to create the slot and publication. The tracker will:

1. Check if the replication slot exists
2. Create it if missing using the configured name and plugin
3. Check if the publication exists
4. Create it if missing, including all tables from your dependency graph
5. Log all creation activities for visibility

## Usage

### Start Streaming

```bash
cdc-tracker stream --config config.yaml
```

The streaming process will:
1. **Auto-create replication slot and publication** (if enabled and missing)
2. Connect to the replication slot
3. Start consuming changes from the last acknowledged WAL position
4. Decode pgoutput binary messages to CDCEvent format
5. Route events through the tracking and routing engine
6. Process messages with automatic acknowledgment

**First-time setup is simplified:**
- The tool will automatically create the slot and publication on first run
- You'll see log messages like:
  ```
  INFO - Creating replication slot 'cdc_slot' with plugin 'pgoutput'
  INFO - Replication slot 'cdc_slot' created successfully
  INFO - Creating publication 'cdc_pub' for tables: customers, orders, order_lines, products
  INFO - Publication 'cdc_pub' created successfully
  ```

**Subsequent runs:**
- The tool checks if slot/publication exist
- If they exist, just starts streaming immediately
- No manual SQL commands required!

### Stop Streaming

Press `Ctrl+C` for graceful shutdown. The process will:
- Stop consuming new messages
- Close the replication connection
- Save the last WAL position in the slot

When restarted, it will resume from the last acknowledged position.

### Monitor

Check replication lag:

```sql
SELECT 
    slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size,
    active,
    restart_lsn
FROM pg_replication_slots 
WHERE slot_name = 'cdc_slot';
```

View activity:

```sql
SELECT * FROM pg_stat_replication;
```

## Troubleshooting

### Slot Not Found

```
psycopg2.errors.UndefinedObject: replication slot "cdc_slot" does not exist
```

Create the slot:

```sql
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
```

### Plugin Not Found

```
ERROR: could not access file "pgoutput": No such file or directory
```

pgoutput is included with PostgreSQL by default. This error shouldn't occur with modern PostgreSQL versions (10+).

### Permission Denied

```
ERROR: permission denied for replication
```

Grant REPLICATION privilege:

```sql
ALTER USER replication_user WITH REPLICATION;
```

### Too Much WAL Data

```
WARNING: replication slot "cdc_slot" has not been active for 1 day
```

The slot retains WAL data until consumed. Either:
- Start consuming to clear the backlog
- Drop and recreate the slot (loses un-consumed changes)

```sql
SELECT pg_drop_replication_slot('cdc_slot');
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
```

### Connection Refused

Check `pg_hba.conf` allows replication connections from your IP:

```
host    replication     replication_user    YOUR_IP/32      md5
```

### No Events Received

Verify the publication includes your tables:

```sql
SELECT * FROM pg_publication_tables WHERE pubname = 'cdc_pub';
```

Make test changes:

```sql
UPDATE customers SET name = 'Test' WHERE _id = 'C1';
```

### Schema Not Matching

pgoutput sends schema name. If filtering by schema, ensure it matches:

```bash
# Filter by schema
cdc-tracker stream --config config.yaml --schema-filter public
```

## Advanced Topics

### Multiple Consumers

Each consumer needs its own replication slot:

```sql
-- Consumer 1
SELECT pg_create_logical_replication_slot('cdc_slot_1', 'pgoutput');

-- Consumer 2
SELECT pg_create_logical_replication_slot('cdc_slot_2', 'pgoutput');
```

Update config.yaml for each consumer with different `slot_name`.

### Initial Snapshot

The current implementation only tracks changes, not initial state. To process existing data, manually populate tracking tables:

```sql
-- Insert all existing base entities
INSERT INTO customers_to_reprocess (customer_id, status)
SELECT _id, 'pending' FROM customers WHERE _deleted = FALSE;
```

Then start streaming for ongoing changes.

### Monitoring & Alerting

Track key metrics:

```sql
-- Replication lag
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) / 1024 / 1024 as lag_mb
FROM pg_replication_slots WHERE slot_name = 'cdc_slot';

-- Slot activity
SELECT active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots WHERE slot_name = 'cdc_slot';
```

Alert if:
- `lag_mb > 100` (falling behind)
- `active = false` for > 5 minutes (consumer down)
- WAL disk usage > 80%

## Performance Tuning

### ACK Interval

Adjust `ack_interval_seconds` based on latency vs durability tradeoff:

```yaml
replication:
  ack_interval_seconds: 5   # More frequent ACKs, less re-processing on failure
  # OR
  ack_interval_seconds: 60  # Less frequent ACKs, more re-processing on failure
```

### Batch Size

Control ACKs by message count:

```yaml
replication:
  max_batch_size: 1000      # ACK after 1000 messages (high throughput)
  # OR
  max_batch_size: 10        # ACK after 10 messages (low latency)
```

### Network Timeouts

PostgreSQL replication has built-in keepalives. To tune:

```sql
-- In postgresql.conf
wal_sender_timeout = 60s      # Disconnect inactive connections
```

### WAL Retention

Limit WAL retention to prevent disk full:

```sql
-- In postgresql.conf
max_slot_wal_keep_size = 10GB  # Limit per-slot WAL retention
```

## Security

### Encryption

Use SSL for replication connections:

```yaml
# Not yet implemented - requires psycopg2 SSL parameters
# Future enhancement
```

### Least Privilege

Replication user only needs:
- REPLICATION privilege
- SELECT on published tables
- No INSERT/UPDATE/DELETE privileges

```sql
CREATE USER replication_user WITH REPLICATION;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
-- No write privileges needed
```

### Password Management

Use environment variables or secret managers instead of plaintext:

```yaml
replication:
  password: "${REPLICATION_PASSWORD}"  # Will be supported in future version
```

## See Also

- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [pgoutput Documentation](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
- [pypgoutput Library](https://github.com/gunnarmorling/pypgoutput)
- Main README.md for overall architecture
- DESIGN.md for routing and tracking strategy
