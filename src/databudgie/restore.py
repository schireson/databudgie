import contextlib
import io
from typing import Generator, Mapping, TypedDict

from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.adapter.base import BaseAdapter, get_adapter
from databudgie.utils import capture_failures, S3Location, wrap_buffer

VALID_STRATEGIES = {
    "use_latest": "use_latest",
}


class RestoreConfig(TypedDict):
    strategy: str
    location: str
    truncate: bool


def restore_all(
    session: Session, s3_resource: S3ServiceResource, tables: Mapping[str, RestoreConfig], **kwargs
) -> None:
    """Perform restore on all tables in the config."""
    strict = kwargs.get("strict", False)

    for table_name, conf in tables.items():
        log.info(f"Restoring {table_name}...")
        with capture_failures(strict=strict):
            restore(session, table_name, s3_resource, **conf, **kwargs)


def restore(
    session: Session,
    table_name: str,
    s3_resource: S3ServiceResource,
    location: str,
    strategy: str = "use_latest",
    truncate: bool = False,
    **kwargs,
) -> None:
    """Restore a CSV file from S3 to the database."""

    adapter: BaseAdapter = get_adapter(session)

    with _download_from_s3(s3_resource, location) as buffer:
        with wrap_buffer(buffer) as wrapper:
            with session:
                if truncate:
                    log.info(f"Truncating {table_name}...")
                    session.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                    session.commit()
                adapter.import_csv(session, wrapper, table_name)

    log.info(f"Restored {table_name} from {location}")


@contextlib.contextmanager
def _download_from_s3(s3_resource: S3ServiceResource, location: str) -> Generator[io.BytesIO, None, None]:
    s3_location = S3Location(location)
    s3_bucket: Bucket = s3_resource.Bucket(s3_location.bucket)

    buffer = io.BytesIO()
    s3_bucket.download_fileobj(s3_location.key, buffer)
    buffer.seek(0)

    yield buffer

    buffer.close()
