import contextlib
import io
import json
import os
import pathlib
from dataclasses import dataclass
from datetime import datetime
from typing import Generator, Iterable, Optional, Sequence, Tuple, TYPE_CHECKING, Union

import sqlalchemy
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.compression import Compressor
from databudgie.config.models import RestoreConfig
from databudgie.etl.base import expand_table_ops, SchemaOp, TableOp
from databudgie.manifest.manager import Manifest
from databudgie.s3 import is_s3_path, optional_s3_resource, S3Location
from databudgie.utils import capture_failures, join_paths, parse_table, restore_filename, wrap_buffer

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource


def restore_all(
    session: Session,
    restore_config: RestoreConfig,
    manifest: Optional[Manifest] = None,
    adapter: Optional[str] = None,
    strict=False,
) -> None:
    """Perform restore on all tables in the config."""
    concrete_adapter = Adapter.get_adapter(adapter or session)
    s3_resource = optional_s3_resource(restore_config)

    if restore_config.ddl.clean:
        concrete_adapter.reset_database(session)
    restore_all_ddl(session, restore_config, s3_resource=s3_resource)

    existing_tables = concrete_adapter.collect_existing_tables(session)
    table_ops = expand_table_ops(
        session,
        restore_config.tables,
        existing_tables,
        manifest=manifest,
    )

    # restrict table ops to tables which we can actually restore:
    table_ops = list(filter(lambda op: check_location_exists(op.location(), s3_resource), table_ops))

    truncate_tables(session, list(reversed(table_ops)), adapter=concrete_adapter)

    for table_op in table_ops:
        log.info(f"Restoring {table_op.table_name}...")

        with capture_failures(strict=strict):
            restore(
                session,
                table_op=table_op,
                manifest=manifest,
                adapter=concrete_adapter,
                s3_resource=s3_resource,
            )


def restore_all_ddl(
    session: Session,
    restore_config: RestoreConfig,
    s3_resource: Optional["S3ServiceResource"] = None,
):

    ddl_config = restore_config.ddl
    if not ddl_config.enabled:
        return

    ddl_path = ddl_config.location
    strategy = ddl_config.strategy

    manifest_path = os.path.join(ddl_path)
    with get_file_contents(manifest_path, strategy, s3_resource=s3_resource, filetype="json") as (buffer, _):
        tables = json.load(buffer)

    table_ops = expand_table_ops(session, restore_config.tables, existing_tables=tables)

    schemas = set()
    for table_op in table_ops:
        schema_op = table_op.schema_op()
        if schema_op.name in schemas:
            continue

        schemas.add(schema_op.name)

        path = restore_ddl(session, schema_op, ddl_path, s3_resource)
        log.debug(f"Restored {schema_op.name} schema from {path}...")

    for table_op in table_ops:
        restore_ddl(session, table_op, ddl_path, s3_resource)
        log.info(f"Restored {table_op.table_name} DDL from {path}")


def restore_ddl(
    session: Session,
    op: Union[TableOp, SchemaOp],
    ddl_path: str,
    s3_resource: Optional["S3ServiceResource"] = None,
):
    location = op.location()
    strategy: str = op.raw_conf.strategy  # type: ignore

    path = join_paths(ddl_path, location)
    with get_file_contents(path, strategy, s3_resource=s3_resource) as (buffer, path):
        query = buffer.read().decode("utf-8")

    query = "\n".join(
        line
        for line in query.splitlines()
        # XXX: These should be being omitted at the backup stage, it's not the restore process' responsibility!
        if not line.startswith("--")
        and not line.startswith("SET")
        and not line.startswith("SELECT pg_catalog")
        and line
    )
    session.execute(sqlalchemy.text(query))
    session.commit()
    return path


def truncate_tables(session: Session, table_ops: Sequence[TableOp], adapter: Adapter):
    for table_op in table_ops:
        truncate = getattr(table_op.raw_conf, "truncate") or False
        if not truncate:
            continue

        adapter.truncate_table(session, table_op.table_name)


def restore(
    session: Session,
    *,
    adapter: Adapter,
    table_op: TableOp,
    manifest: Optional[Manifest] = None,
    s3_resource: Optional["S3ServiceResource"] = None,
) -> None:
    """Restore a CSV file from S3 to the database."""

    # Force table_name to be fully qualified
    schema, table = parse_table(table_op.table_name)
    table_name = f"{schema}.{table}"

    strategy: str = table_op.raw_conf.strategy  # type: ignore
    compression = table_op.raw_conf.compression

    file_contents = get_file_contents(
        table_op.location(),
        strategy,
        s3_resource=s3_resource,
        compression=compression,
    )
    with file_contents as (buffer, path):
        with wrap_buffer(buffer) as wrapper:
            with session:
                adapter.import_csv(session, wrapper, table_op.table_name)
                session.commit()

        if manifest:
            manifest.record(table_name, path)

    log.info(f"Restored {table_name} from {path}")


def check_location_exists(
    location: str,
    s3_resource: Optional["S3ServiceResource"] = None,
) -> bool:
    matching_objects: list
    if is_s3_path(location):
        if not s3_resource:
            raise ValueError("No S3 resource provided")

        s3_location = S3Location(location)
        s3_bucket: "Bucket" = s3_resource.Bucket(s3_location.bucket)
        matching_objects = list(s3_bucket.objects.filter(Prefix=s3_location.key).all())
    else:
        matching_objects = list(dir_entry for dir_entry in os.scandir(location) if dir_entry.is_file())

    return len(matching_objects) >= 1


@dataclass(frozen=True)
class ObjectSummary:
    key: str
    last_modified: datetime

    @property
    def path(self):
        return pathlib.PurePath(self.key)

    @classmethod
    def from_stat(cls, name, stat: os.stat_result):
        return cls(name, datetime.fromtimestamp(stat.st_mtime))


@contextlib.contextmanager
def get_file_contents(
    location: str,
    strategy: str,
    s3_resource: Optional["S3ServiceResource"] = None,
    filetype="csv",
    compression=None,
) -> Generator[Tuple[io.BytesIO, str], None, None]:
    concrete_strategy = VALID_STRATEGIES[strategy]

    buffer = io.BytesIO()

    target_object: ObjectSummary
    if is_s3_path(location):
        # this location.key should be a folder
        s3_location = S3Location(location)
        s3_bucket: "Bucket" = s3_resource.Bucket(s3_location.bucket)  # type: ignore

        object_generator = (
            ObjectSummary(o.key, o.last_modified) for o in s3_bucket.objects.filter(Prefix=s3_location.key)
        )
        target_object = concrete_strategy(object_generator, filetype=filetype, compression=compression)

        s3_bucket.download_fileobj(target_object.key, buffer)
        path = f"s3://{s3_location.bucket}/{target_object.key}"
    else:
        object_generator = (
            ObjectSummary.from_stat(os.path.sep.join([location, dir_entry.name]), dir_entry.stat())
            for dir_entry in os.scandir(location)
            if dir_entry.is_file()
        )
        target_object = concrete_strategy(object_generator, filetype=filetype, compression=compression)

        with open(target_object.key, "rb") as f:
            buffer.write(f.read())
        path = target_object.key

    log.info(f"Using {target_object.key}...")

    buffer.seek(0)

    cbuffer = Compressor.get_with_name(compression).extract(buffer)

    yield cbuffer, path
    buffer.close()


def _use_filename_strategy(
    available_objects: Iterable[ObjectSummary], filetype="csv", compression=None
) -> ObjectSummary:
    objects_by_filename = {obj.path.name: obj for obj in available_objects}
    ordered_filenames = sorted(
        objects_by_filename.keys(),
        key=lambda x: restore_filename(x, filetype=filetype, compression=compression),
        reverse=True,
    )

    return objects_by_filename[ordered_filenames[0]]


def _use_metadata_strategy(
    available_objects: Iterable[ObjectSummary], filetype="csv", compression=None
) -> ObjectSummary:
    objects_by_last_modified_date = {s3_object.last_modified: s3_object for s3_object in available_objects}
    ordered_last_modified_dates = sorted(objects_by_last_modified_date.keys(), reverse=True)

    return objects_by_last_modified_date[ordered_last_modified_dates[0]]


VALID_STRATEGIES = {
    "use_latest_metadata": _use_metadata_strategy,
    "use_latest_filename": _use_filename_strategy,
}
