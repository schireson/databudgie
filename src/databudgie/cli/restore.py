from configly.config import Config
from setuplog import log

from databudgie.cli.base import cli, resolver


@resolver.command(cli, "restore")
def restore(config: Config):
    """Perform restore."""

    log.info("Performing restore! (environment: %s)", config.environment)
