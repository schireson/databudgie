import logging

import strapp.sentry
from configly import Config

package_verbosity = strapp.logging.package_verbosity_factory(
    ("urllib3", logging.INFO, logging.INFO, logging.INFO, logging.DEBUG),
    ("sqlalchemy.engine", logging.WARNING, logging.WARNING, logging.WARNING, logging.INFO),
    ("docker", logging.INFO, logging.INFO, logging.INFO, logging.DEBUG),
    ("suds", logging.CRITICAL, logging.CRITICAL, logging.CRITICAL, logging.CRITICAL),
    ("botocore", logging.INFO, logging.INFO, logging.INFO, logging.DEBUG),
    ("boto3", logging.INFO, logging.INFO, logging.INFO, logging.DEBUG),
    ("datadog.threadstats", logging.INFO),
    ("datadog.dogstatsd", logging.ERROR),
)


def setup(config: Config, verbosity: int):
    setup_logging(config.logging.level, verbosity)
    setup_sentry(config)


def setup_sentry(config: Config):
    strapp.sentry.setup_sentry(
        dsn=config.sentry.sentry_dsn,
        release=config.sentry.version,
        environment=config.environment,
        service_name="databudgie",
        level="WARNING",
        breadcrumb_level="INFO",
    )


def setup_logging(level: str, verbosity: int):
    from setuplog import setup_logging as _setup_logging

    level_map = {
        "30": logging.WARNING,
        "WARNING": logging.WARNING,
        "20": logging.INFO,
        "INFO": logging.INFO,
        "10": logging.DEBUG,
        "DEBUG": logging.DEBUG,
    }

    chosen_level = level_map.get(level.upper(), logging.INFO)
    actual_level = max(chosen_level - 10 * verbosity, logging.DEBUG)

    _setup_logging(
        actual_level,
        log_level_overrides=package_verbosity(verbosity),
    )
