import io
from datetime import datetime
from os import path
from typing import Optional

from configly import Config
from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.compat import TypedDict
from databudgie.etl.base import expand_table_ops, TableOp
from databudgie.manifest.manager import Manifest
from databudgie.utils import capture_failures, FILENAME_FORMAT, S3Location, wrap_buffer


class BackupConfig(TypedDict):
    query: str
    location: str
    exclude: Optional[str]


def backup_all(
    session: Session,
    s3_resource: S3ServiceResource,
    config: Config,
    manifest: Optional[Manifest] = None,
    strict=False,
    adapter=None,
):
    """Perform backup on all tables in the config.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        s3_resource: boto S3 resource from an authenticated session.
        config: config object mapping table names to their query and location.
        strict: terminate backup after failing one table.
        manifest: optional manifest to record the backup location.
        adapter: optional adapter
    """
    table_ops = expand_table_ops(session, config.backup.tables, ref=config, manifest=manifest)

    for table_op in table_ops:
        log.info(f"Backing up {table_op.table_name}...")

        with capture_failures(strict=strict):
            backup(
                session,
                s3_resource,
                table_op=table_op,
                manifest=manifest,
                adapter=adapter,
            )


def backup(
    session: Session,
    s3_resource: S3ServiceResource,
    *,
    table_op: TableOp,
    manifest: Optional[Manifest] = None,
    adapter=None,
):
    """Dump query contents to S3 as a CSV file.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        query: string SQL query to run against the session.
        s3_resource: boto S3 resource from an authenticated session.
        location: folder path on S3 of where to put the CSV
        table_name: identifer for the table, used in the CSV filename.
        manifest: optional manifest to record the backup location.
        adapter: optional adapter
    """
    adapter = Adapter.get_adapter(adapter or session)

    buffer = io.BytesIO()
    with wrap_buffer(buffer) as wrapper:
        adapter.export_query(session, table_op.query, wrapper)

    # path.join will handle optionally trailing slashes in the location
    fully_qualified_path = path.join(table_op.location, datetime.now().strftime(FILENAME_FORMAT))

    _upload_to_s3(s3_resource, fully_qualified_path, buffer)
    buffer.close()

    if manifest:
        manifest.record(table_op.table_name, fully_qualified_path)

    log.info(f"Uploaded {table_op.table_name} to {fully_qualified_path}")


def _upload_to_s3(s3_resource: S3ServiceResource, s3_path: str, buffer: io.BytesIO):
    s3_location = S3Location(s3_path)
    s3_bucket: Bucket = s3_resource.Bucket(s3_location.bucket)
    s3_bucket.put_object(Key=s3_location.key, Body=buffer)
