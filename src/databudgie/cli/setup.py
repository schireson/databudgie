import logging
from typing import Optional

import strapp.logging

from databudgie.config import SentryConfig

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


def setup(sentry: Optional[SentryConfig]):
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
