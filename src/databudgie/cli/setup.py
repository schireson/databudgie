import logging
from typing import Optional

import strapp.logging

from databudgie.config.models import LoggingConfig, SentryConfig

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


def setup(sentry: Optional[SentryConfig], logging: Optional[LoggingConfig], verbosity: int):
    if logging:
        setup_logging(logging.level, verbosity)

    if sentry:
        setup_sentry(sentry)


def setup_sentry(config: SentryConfig):
    import strapp.sentry

    strapp.sentry.setup_sentry(
        dsn=config.dsn,
        release=config.version,
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
