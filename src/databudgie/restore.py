import contextlib
import io
from typing import Generator, Mapping, Optional, TypedDict

from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.manifest.manager import Manifest
from databudgie.utils import capture_failures, S3Location, wrap_buffer

VALID_STRATEGIES = {
    "use_latest": "use_latest",
}


class RestoreConfig(TypedDict):
    strategy: str
    location: str
    truncate: bool


def restore_all(
    session: Session,
    s3_resource: S3ServiceResource,
    tables: Mapping[str, RestoreConfig],
    manifest: Optional[Manifest] = None,
    **kwargs,
) -> None:
    """Perform restore on all tables in the config.

    kwargs can include:
        - adapter (Adapter): TODO
        - strategy (str): TODO
    """
    strict = kwargs.get("strict", False)

    for table_name, conf in tables.items():
        if manifest and table_name in manifest:
            log.info(f"Skipping {table_name}...")
            continue

        log.info(f"Restoring {table_name}...")
        with capture_failures(strict=strict):
            restore(session, table_name, s3_resource, manifest=manifest, **conf, **kwargs)


def restore(
    session: Session,
    table_name: str,
    s3_resource: S3ServiceResource,
    location: str,
    manifest: Optional[Manifest] = None,
    strategy: str = "use_latest",
    truncate: bool = False,
    **kwargs,
) -> None:
    """Restore a CSV file from S3 to the database."""

    adapter = Adapter.get_adapter(kwargs.get("adapter", None) or session)

    with _download_from_s3(s3_resource, location) as buffer:
        with wrap_buffer(buffer) as wrapper:
            with session:
                if truncate:
                    log.info(f"Truncating {table_name}...")
                    session.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                    session.commit()
                adapter.import_csv(session, wrapper, table_name)

    if manifest:
        manifest.record(table_name, location)

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
