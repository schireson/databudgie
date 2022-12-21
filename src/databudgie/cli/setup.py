from typing import Optional

from databudgie.config import SentryConfig


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
