import io
import json
import os
import pathlib
from datetime import datetime
from os import path
from typing import List, Optional

from configly import Config
from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.compat import TypedDict
from databudgie.etl.base import expand_table_ops, TableOp
from databudgie.manifest.manager import Manifest
from databudgie.s3 import is_s3_path, optional_s3_resource, S3Location
from databudgie.utils import capture_failures, generate_filename, join_paths, wrap_buffer


class BackupConfig(TypedDict):
    query: str
    location: str
    exclude: Optional[str]


def backup_all(
    session: Session,
    config: Config,
    manifest: Optional[Manifest] = None,
    strict=False,
    adapter=None,
    s3_resource: Optional[S3ServiceResource] = None,
):
    """Perform backup on all tables in the config.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        config: config object mapping table names to their query and location.
        strict: terminate backup after failing one table.
        manifest: optional manifest to record the backup location.
        adapter: optional adapter
        s3_resource: optional boto S3 resource from an authenticated session.
    """
    concrete_adapter = Adapter.get_adapter(adapter or session)
    s3_resource = optional_s3_resource(config)
    timestamp = datetime.now()

    table_ops = expand_table_ops(session, config.backup.tables, manifest=manifest)

    backup_ddl(
        session, config.backup, table_ops, timestamp=timestamp, adapter=concrete_adapter, s3_resource=s3_resource
    )

    for table_op in table_ops:
        log.info(f"Backing up {table_op.table_name}...")

        with capture_failures(strict=strict):
            backup(
                session,
                config=config,
                table_op=table_op,
                manifest=manifest,
                timestamp=timestamp,
                adapter=concrete_adapter,
                s3_resource=s3_resource,
            )


def backup_ddl(
    session: Session,
    config: Config,
    table_ops: List[TableOp],
    *,
    timestamp: datetime,
    adapter=Adapter,
    s3_resource: Optional[S3ServiceResource] = None,
):
    ddl_config = config.get("ddl", {})
    if not ddl_config.get("enabled", False):
        return

    ddl_path = ddl_config["location"]

    for table_op in table_ops:
        log.debug(f"Backing up {table_op.table_name} DDL...")
        result = adapter.export_table_ddl(session, table_op.table_name)

        full_table_path = table_op.location(config)
        fully_qualified_path = join_paths(ddl_path, full_table_path, generate_filename(timestamp))

        with io.BytesIO(result) as buffer:
            persist_backup(fully_qualified_path, buffer, s3_resource=s3_resource)

        log.debug(f"Uploaded {table_op.table_name} to {fully_qualified_path}")

    manifest_data = json.dumps([op.table_name for op in table_ops]).encode("utf-8")
    with io.BytesIO(manifest_data) as buffer:
        manifest_path = join_paths(ddl_path, generate_filename(timestamp, "json"))
        persist_backup(manifest_path, buffer, s3_resource=s3_resource)


def backup(
    session: Session,
    *,
    config: Config,
    table_op: TableOp,
    adapter: Adapter,
    timestamp: Optional[datetime] = None,
    manifest: Optional[Manifest] = None,
    s3_resource: Optional[S3ServiceResource] = None,
):
    """Dump query contents to S3 as a CSV file.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        config: The raw backup configuration.
        table_op: The table operation being acted up on.
        timestamp: optional timestamp to use for the backup filename.
        adapter: the selected behavior adapter
        manifest: optional manifest to record the backup location.
        s3_resource: optional boto S3 resource from an authenticated session.
    """
    buffer = io.BytesIO()
    with wrap_buffer(buffer) as wrapper:
        adapter.export_query(session, table_op.query(config), wrapper)

    # path.join will handle optionally trailing slashes in the location
    fully_qualified_path = path.join(table_op.location(config), generate_filename(timestamp))

    persist_backup(fully_qualified_path, buffer, s3_resource=s3_resource)
    buffer.close()

    if manifest:
        manifest.record(table_op.table_name, fully_qualified_path)

    log.info(f"Uploaded {table_op.table_name} to {fully_qualified_path}")


def persist_backup(path: str, buffer: io.BytesIO, s3_resource: Optional[S3ServiceResource] = None):
    if is_s3_path(path):
        s3_location = S3Location(path)
        s3_bucket: Bucket = s3_resource.Bucket(s3_location.bucket)  # type: ignore
        s3_bucket.put_object(Key=s3_location.key, Body=buffer)
    else:
        parent = pathlib.PurePath(path).parent
        os.makedirs(parent, exist_ok=True)
        with open(path, "wb") as f:
            f.write(buffer.getbuffer())
