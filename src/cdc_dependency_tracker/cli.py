"""CLI for CDC event processor."""

import sys
import logging
import click

from .config import Config
from .streaming import StreamingManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)

logger = logging.getLogger(__name__)


@click.group()
def cli():
    """
    CDC Dependency Tracker - Track entity dependencies from CDC events.

    Streams CDC events from PostgreSQL logical replication using pgoutput.
    """
    pass


@cli.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Path to configuration YAML file",
)
@click.option("--schema-filter", "-s", default=None, help="Filter events by schema name (optional)")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def stream(config: str, schema_filter: str, verbose: bool):
    """
    Stream CDC events from PostgreSQL logical replication.

    Requires replication configuration in config.yaml and proper PostgreSQL setup.

    Example:
        cdc-tracker stream --config config.yaml
    """
    try:
        # Load configuration
        cfg = Config.from_yaml(config)
        logger.info(f"Loaded configuration from {config}")

        # Create and start streaming manager
        with StreamingManager(cfg, verbose=verbose) as manager:
            manager.start_streaming(blocking=True)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error in replication stream: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
