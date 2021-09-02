from configly.config import Config
from setuplog import log

from databudgie.cli.base import cli, resolver


@resolver.command(cli, "backup")
def backup(config: Config):
    """Perform backup."""

    log.info("Performing backup! (environment: %s)", config.environment)
