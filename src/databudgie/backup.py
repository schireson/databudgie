from __future__ import annotations

import io
import json
import os
import pathlib
from datetime import datetime
from os import path
from typing import List, Optional, Sequence, TYPE_CHECKING

from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.compression import Compressor
from databudgie.config import BackupConfig, BackupTableConfig
from databudgie.manifest.manager import Manifest
from databudgie.output import Console, default_console, Progress
from databudgie.s3 import is_s3_path, optional_s3_resource, S3Location
from databudgie.table_op import expand_table_ops, TableOp
from databudgie.utils import capture_failures, generate_filename, join_paths, wrap_buffer

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource


def backup_all(
    session: Session,
    backup_config: BackupConfig,
    manifest: Optional[Manifest] = None,
    s3_resource: Optional["S3ServiceResource"] = None,
    console: Console = default_console,
):
    """Perform backup on all tables in the config.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        backup_config: config object mapping table names to their query and location.
        manifest: optional manifest to record the backup location.
        s3_resource: optional boto S3 resource from an authenticated session.
        console: Console used for output
    """
    adapter = Adapter.get_adapter(session, backup_config.adapter)
    s3_resource = optional_s3_resource(backup_config)
    timestamp = datetime.now()

    existing_tables = adapter.collect_existing_tables()
    table_ops = expand_table_ops(
        session,
        backup_config.tables,
        existing_tables,
        manifest=manifest,
        console=console,
        warn_for_unused_tables=True,
    )

    table_ops = adapter.materialize_table_dependencies(
        table_ops,
        console=console,
    )

    backup_ddl(
        backup_config,
        table_ops,
        timestamp=timestamp,
        adapter=adapter,
        s3_resource=s3_resource,
        console=console,
    )
    backup_sequences(
        table_ops,
        timestamp=timestamp,
        adapter=adapter,
        s3_resource=s3_resource,
        console=console,
    )
    backup_tables(
        table_ops=table_ops,
        manifest=manifest,
        adapter=adapter,
        s3_resource=s3_resource,
        console=console,
    )


def backup_ddl(
    backup_config: BackupConfig,
    table_ops: List[TableOp[BackupTableConfig]],
    *,
    timestamp: datetime,
    adapter: Adapter,
    console: Console = default_console,
    s3_resource: Optional["S3ServiceResource"] = None,
):
    if not backup_config.ddl.enabled:
        return

    ddl_path = backup_config.ddl.location

    # Backup schemas first
    schema_names = set()
    schemas = []

    for table_op in table_ops:
        schema_op = table_op.schema_op()
        if schema_op.name in schemas:
            continue

        if not table_op.raw_conf.ddl:
            continue

        schema_names.add(schema_op.name)
        schemas.append(schema_op)

    with Progress(console) as progress:
        table_names = []

        total = len(schemas) + len(table_ops)
        task = progress.add_task("Backing up schema DDL", total=total)

        for schema_op in schemas:
            progress.update(task, description=f"Backing up schema DDL: {schema_op.name}")

            result = adapter.export_schema_ddl(schema_op.name)

            path = schema_op.location()
            fully_qualified_path = join_paths(ddl_path, path, generate_filename(timestamp))

            with io.BytesIO(result) as buffer:
                persist_backup(fully_qualified_path, buffer, s3_resource=s3_resource)

            console.trace(f"Uploaded {schema_op.name} to {fully_qualified_path}")

        console.info("Finished backing up schema DDL")

        for table_op in table_ops:
            if not table_op.raw_conf.ddl:
                continue

            progress.update(task, description=f"Backing up DDL: {table_op.full_name}")
            result = adapter.export_table_ddl(table_op.full_name)

            full_table_path = table_op.location()
            fully_qualified_path = join_paths(ddl_path, full_table_path, generate_filename(timestamp))

            with io.BytesIO(result) as buffer:
                persist_backup(fully_qualified_path, buffer, s3_resource=s3_resource)

            console.trace(f"Uploaded {table_op.full_name} to {fully_qualified_path}")
            table_names.append(table_op.full_name)

    console.info("Finished backing up DDL")

    # On the restore-side, the tables may not already exist (at the extreme, you
    # might start with an empty database), so we need to record the set of tables
    # being backed up.
    manifest_data = json.dumps(table_names).encode("utf-8")
    with io.BytesIO(manifest_data) as buffer:
        manifest_path = join_paths(ddl_path, generate_filename(timestamp, filetype="json"))
        persist_backup(manifest_path, buffer, s3_resource=s3_resource)


def backup_sequences(
    table_ops: List[TableOp[BackupTableConfig]],
    *,
    timestamp: datetime,
    adapter: Adapter,
    console: Console = default_console,
    s3_resource: Optional["S3ServiceResource"] = None,
):
    has_sequences = any(o.raw_conf.sequences for o in table_ops)
    if not has_sequences:
        return

    table_sequences = adapter.collect_table_sequences()

    with Progress(console) as progress:
        task = progress.add_task("Backing up sequence positions", total=len(table_ops))

        for table_op in table_ops:
            progress.update(task, description=f"Backing up sequence position: {table_op.full_name}")

            if not table_op.raw_conf.sequences:
                continue

            sequences = table_sequences.get(table_op.full_name)
            if not sequences:
                continue

            sequence_values = {}
            for sequence in sequences:
                sequence_values[sequence] = adapter.collect_sequence_value(sequence)

            result = json.dumps(sequence_values).encode("utf-8")

            path = table_op.location()
            fully_qualified_path = join_paths(path, "sequences", generate_filename(timestamp, filetype="json"))

            with io.BytesIO(result) as buffer:
                persist_backup(fully_qualified_path, buffer, s3_resource=s3_resource)

            console.trace(f"Wrote {table_op.full_name} sequences to {fully_qualified_path}")

    console.info("Finished backing up sequence positions")


def backup_tables(
    table_ops: Sequence[TableOp],
    *,
    adapter: Adapter,
    console: Console = default_console,
    manifest: Optional[Manifest] = None,
    s3_resource: Optional["S3ServiceResource"] = None,
) -> None:
    with Progress(console) as progress:
        task = progress.add_task("Backing up tables", total=len(table_ops))

        for table_op in table_ops:
            progress.update(task, description=f"Backing up table: {table_op.full_name}")

            if not table_op.raw_conf.data:
                continue

            with capture_failures(strict=table_op.raw_conf.strict):
                backup(
                    table_op=table_op,
                    manifest=manifest,
                    adapter=adapter,
                    s3_resource=s3_resource,
                    console=console,
                )

    console.info("Finished Backing up tables")


def backup(
    *,
    table_op: TableOp[BackupTableConfig],
    adapter: Adapter,
    console: Console = default_console,
    timestamp: Optional[datetime] = None,
    manifest: Optional[Manifest] = None,
    s3_resource: Optional["S3ServiceResource"] = None,
):
    """Dump query contents to S3 as a CSV file.

    Arguments:
        config: The raw backup configuration.
        table_op: The table operation being acted up on.
        timestamp: optional timestamp to use for the backup filename.
        adapter: the selected behavior adapter
        manifest: optional manifest to record the backup location.
        s3_resource: optional boto S3 resource from an authenticated session.
        console: Console used for output
    """
    buffer = io.BytesIO()
    with wrap_buffer(buffer) as wrapper:
        adapter.export_query(table_op.query(), wrapper)

    # path.join will handle optionally trailing slashes in the location
    compression = table_op.raw_conf.compression
    fully_qualified_path = path.join(table_op.location(), generate_filename(timestamp, compression=compression))

    persist_backup(fully_qualified_path, buffer, s3_resource=s3_resource, compression=compression)
    buffer.close()

    if manifest:
        manifest.record(table_op.full_name, fully_qualified_path)

    console.trace(f"Uploaded {table_op.full_name} to {fully_qualified_path}")


def persist_backup(path: str, buffer: io.BytesIO, s3_resource: Optional["S3ServiceResource"] = None, compression=None):
    buffer = Compressor.get_with_name(compression).compress(buffer)

    if is_s3_path(path):
        s3_location = S3Location(path)
        s3_bucket: Bucket = s3_resource.Bucket(s3_location.bucket)  # type: ignore
        s3_bucket.put_object(Key=s3_location.key, Body=buffer)
    else:
        parent = pathlib.PurePath(path).parent
        os.makedirs(parent, exist_ok=True)
        with open(path, "wb") as f:
            f.write(buffer.getbuffer())
