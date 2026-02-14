#!/bin/bash
# Test script for CDC dependency tracker

set -e

echo "=== CDC Dependency Tracker Test Script ==="
echo

# Check if config exists
if [ ! -f "examples/config.yaml" ]; then
    echo "Error: examples/config.yaml not found"
    exit 1
fi

echo "1. Testing INSERT event (orders)..."
cat examples/sample_debezium_insert.json | python -m cdc_dependency_tracker.cli --config examples/config.yaml
echo "✓ INSERT processed"
echo

echo "2. Testing UPDATE event (order_lines with join key change)..."
cat examples/sample_debezium_update.json | python -m cdc_dependency_tracker.cli --config examples/config.yaml
echo "✓ UPDATE processed"
echo

echo "3. Testing DELETE event (products)..."
cat examples/sample_debezium_delete.json | python -m cdc_dependency_tracker.cli --config examples/config.yaml
echo "✓ DELETE processed"
echo

echo "4. Testing percolation (once)..."
python -m cdc_dependency_tracker.percolator_cli --config examples/config.yaml --once
echo "✓ Percolation completed"
echo

echo "=== All tests passed! ==="
