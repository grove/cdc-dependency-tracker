"""CLI for background percolation daemon."""

import sys
import time
import signal
import logging
import click

from .config import Config
from .sql_parser import SQLParser
from .dependency_graph import DependencyGraph
from .db_client import DatabaseClient
from .percolator import PercolationEngine
from .utils import derive_id_column_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)

logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown."""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True


@click.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Path to configuration YAML file",
)
@click.option(
    "--once", is_flag=True, help="Run percolation once and exit (instead of continuous loop)"
)
@click.option("--cleanup", is_flag=True, help="Clean up old percolated items after each batch")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def main(config: str, once: bool, cleanup: bool, verbose: bool):
    """
    CDC Dependency Percolator - Background daemon for batch resolution.

    Continuously percolates items from intermediate_to_track table
    to the base entity tracking table by resolving join paths.

    Example:
        cdc-percolator --config config.yaml
        cdc-percolator --config config.yaml --once
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Load configuration
        cfg = Config.from_yaml(config)
        logger.info(f"Loaded configuration: {cfg}")

        # Parse SQL to build dependency graph and extract base ID column
        parser = SQLParser(cfg.tracker.sql_query, cfg.tracker.base_table)
        join_graph = parser.get_join_graph()
        dependency_graph = DependencyGraph(cfg.tracker.base_table, join_graph)
        logger.info(f"Built dependency graph: {dependency_graph}")

        # Get base ID column from parsed SQL or fall back to heuristic
        base_id_column = parser.get_base_id_column()
        if base_id_column:
            logger.info(f"Using base_id_column: {base_id_column} (parsed from SQL)")
        else:
            # Fall back to utility function for deriving column name
            base_id_column = derive_id_column_name(cfg.tracker.base_table)
            logger.info(f"Using base_id_column: {base_id_column} (derived from table name)")

        # Initialize database client with tracking table name
        db_client = DatabaseClient(
            cfg.database.to_connection_params(), tracking_table=cfg.tracker.tracking_table
        )

        # Initialize percolation engine
        percolator = PercolationEngine(
            db_client,
            dependency_graph,
            base_id_column=base_id_column,
            batch_size=cfg.tracker.percolation_batch_size,
        )

        interval = cfg.tracker.percolation_interval_seconds

        logger.info(
            f"Starting percolator (interval={interval}s, batch_size={cfg.tracker.percolation_batch_size})"
        )

        if once:
            # Run once and exit
            run_percolation_cycle(percolator, cleanup)
            db_client.close()
            sys.exit(0)

        # Continuous loop
        while not shutdown_requested:
            try:
                run_percolation_cycle(percolator, cleanup)

                # Sleep with interrupt check
                for _ in range(interval):
                    if shutdown_requested:
                        break
                    time.sleep(1)

            except Exception as e:
                logger.error(f"Error in percolation cycle: {e}", exc_info=True)
                time.sleep(interval)

        logger.info("Shutdown complete")
        db_client.close()
        sys.exit(0)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


def run_percolation_cycle(percolator: PercolationEngine, cleanup: bool) -> None:
    """Run one percolation cycle."""
    # Get metrics before percolation
    pending_before = percolator.get_pending_count()
    lag_before = percolator.get_percolation_lag()

    logger.info(f"Percolation cycle start: {pending_before} pending, {lag_before}s lag")

    if pending_before == 0:
        logger.debug("No pending items to percolate")
        return

    # Run percolation
    result = percolator.percolate_batch()

    logger.info(
        f"Percolated {result['items_percolated']} items, "
        f"tracked {result['entities_tracked']} entities"
    )

    # Print metrics for monitoring
    print(
        f"items_percolated={result['items_percolated']} "
        f"entities_tracked={result['entities_tracked']}"
    )

    # Cleanup if requested
    if cleanup:
        archived = percolator.archive_old_items(retention_days=7)
        deleted = percolator.cleanup_old_items(retention_days=7)
        if deleted > 0:
            logger.info(f"Cleanup: archived {archived}, deleted {deleted} old items")

    # Get metrics after percolation
    pending_after = percolator.get_pending_count()
    lag_after = percolator.get_percolation_lag()

    logger.info(f"Percolation cycle end: {pending_after} pending, {lag_after}s lag")


if __name__ == "__main__":
    main()
