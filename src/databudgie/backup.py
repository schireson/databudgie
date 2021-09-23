import io
from typing import Mapping, Optional, TypedDict

from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.manifest.manager import Manifest
from databudgie.utils import capture_failures, S3Location, wrap_buffer


class BackupConfig(TypedDict):
    query: str
    location: str


def backup_all(
    session: Session,
    s3_resource: S3ServiceResource,
    tables: Mapping[str, BackupConfig],
    manifest: Optional[Manifest] = None,
    **kwargs,
):
    """Perform backup on all tables in the config.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        s3_resource: boto S3 resource from an authenticated session.
        tables: config object mapping table names to their query and location.
        strict: terminate backup after failing one table.
        manifest: optional manifest to record the backup location.
    """
    strict = kwargs.get("strict", False)

    for table_name, conf in tables.items():
        if manifest and table_name in manifest:
            log.info(f"Skipping {table_name}...")
            continue

        log.info(f"Backing up {table_name}...")
        with capture_failures(strict=strict):
            backup(session, conf["query"], s3_resource, conf["location"], table_name, manifest=manifest, **kwargs)


def backup(
    session: Session,
    query: str,
    s3_resource: S3ServiceResource,
    location: str,
    table_name: str,
    manifest: Optional[Manifest] = None,
    **kwargs,
):
    """Dump query contents to S3 as a CSV file.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        query: string SQL query to run against the session.
        s3_resource: boto S3 resource from an authenticated session.
        location: folder path on S3 of where to put the CSV
        table_name: identifer for the table, used in the CSV filename.
        manifest: optional manifest to record the backup location.
        kwargs: additional keyword arguments.
    """
    adapter = Adapter.get_adapter(kwargs.get("adapter", None) or session)

    buffer = io.BytesIO()
    with wrap_buffer(buffer) as wrapper:
        adapter.export_query(session, query, wrapper)

    _upload_to_s3(s3_resource, location, buffer)

    if manifest:
        manifest.record(table_name, location)

    log.info(f"Uploaded {table_name} to {location}")
    buffer.close()


def _upload_to_s3(s3_resource: S3ServiceResource, location: str, buffer: io.BytesIO):
    s3_location = S3Location(location)
    s3_bucket: Bucket = s3_resource.Bucket(s3_location.bucket)
    s3_bucket.put_object(Key=s3_location.key, Body=buffer)
