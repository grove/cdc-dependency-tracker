set shell := ["bash", "-cu"]

venv := ".venv"
python := ".venv/bin/python"
uv := "uv"
pytest := ".venv/bin/pytest"
ruff := ".venv/bin/ruff"
ty := ".venv/bin/ty"

# Show available commands
@default:
    just --list

# Create virtual environment and install package + dev deps
setup:
    python3.13 -m venv {{venv}} || python3.12 -m venv {{venv}} || python3 -m venv {{venv}}
    {{uv}} pip install --python {{python}} -e ".[dev]"

# Reinstall dependencies in existing venv
install:
    {{uv}} pip install --python {{python}} -e ".[dev]"

# Show Python and tool versions
versions:
    {{python}} --version
    {{pytest}} --version
    {{ruff}} --version
    {{ty}} version

# Validate config loads (replace path if needed)
check-config config="examples/config.yaml":
    {{python}} -c "from cdc_dependency_tracker.config import Config; Config.from_yaml('{{config}}'); print('Config OK:', '{{config}}')"

# Create tracking tables
setup-db config="examples/config.yaml":
    {{python}} setup_db.py --config {{config}}

# Drop and recreate tracking tables
reset-db config="examples/config.yaml":
    {{python}} setup_db.py --config {{config}} --drop
    {{python}} setup_db.py --config {{config}}

# Run CDC stream consumer
stream config="examples/config.yaml":
    {{venv}}/bin/cdc-tracker stream --config {{config}}

# Run CDC stream consumer with verbose logging
stream-verbose config="examples/config.yaml":
    {{venv}}/bin/cdc-tracker stream --config {{config}} --verbose

# Run percolator once
percolate-once config="examples/config.yaml":
    {{venv}}/bin/cdc-percolator --config {{config}} --once

# Run percolator daemon
percolate config="examples/config.yaml":
    {{venv}}/bin/cdc-percolator --config {{config}}

# Run percolator with cleanup
percolate-cleanup config="examples/config.yaml":
    {{venv}}/bin/cdc-percolator --config {{config}} --cleanup

# Run all tests
test:
    {{pytest}} tests/ -v --tb=short

# Run fast unit tests only (skip E2E)
test-unit:
    {{pytest}} tests/ -v --tb=short -m "not e2e"

# Run E2E tests
test-e2e:
    {{pytest}} tests/ -v --tb=short -m e2e

# Run tests with coverage
coverage:
    {{pytest}} tests/ --cov=src/cdc_dependency_tracker --cov-report=term-missing --tb=short

# List collected tests without running
test-list:
    {{pytest}} tests/ --co -q

# Format code
fmt:
    {{ruff}} format src/ tests/ setup_db.py

# Lint code
lint:
    {{ruff}} check src/ tests/ setup_db.py

# Type-check code
typecheck:
    {{ty}} check src/

# Run all quality checks
check: fmt lint typecheck test

# Remove caches and build artifacts
clean:
    find . -type d -name "__pycache__" -prune -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete
    rm -rf .pytest_cache .ruff_cache .coverage htmlcov build dist *.egg-info

# Remove virtual environment
clean-venv:
    rm -rf {{venv}}
