import contextlib
import io
import json
import os
import pathlib
from dataclasses import dataclass
from datetime import datetime
from typing import Generator, Iterable, List, Optional, Tuple, TYPE_CHECKING

import sqlalchemy
from configly import Config
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.compat import TypedDict
from databudgie.etl.base import expand_table_ops, TableOp
from databudgie.manifest.manager import Manifest
from databudgie.s3 import is_s3_path, optional_s3_resource, S3Location
from databudgie.utils import capture_failures, join_paths, parse_table, restore_filename, wrap_buffer

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource


class RestoreConfig(TypedDict):
    strategy: str
    location: str
    truncate: bool


def restore_all(
    session: Session,
    config: Config,
    manifest: Optional[Manifest] = None,
    adapter: Optional[str] = None,
    strict=False,
) -> None:
    """Perform restore on all tables in the config."""
    concrete_adapter = Adapter.get_adapter(adapter or session)
    s3_resource = optional_s3_resource(config)

    if config.restore.ddl.clean:
        concrete_adapter.reset_database(session)
    restore_all_ddl(session, config, s3_resource=s3_resource)

    table_ops = expand_table_ops(session, config.restore.tables, manifest=manifest)

    truncate_tables(session, table_ops, adapter=concrete_adapter)

    for table_op in table_ops:
        log.info(f"Restoring {table_op.table_name}...")

        with capture_failures(strict=strict):
            restore(
                session,
                config=config,
                table_op=table_op,
                manifest=manifest,
                adapter=concrete_adapter,
                s3_resource=s3_resource,
            )


def restore_all_ddl(
    session: Session,
    config: Config,
    s3_resource: Optional["S3ServiceResource"] = None,
):
    restore_config = config.restore
    ddl_config = restore_config.get("ddl", {})
    if not ddl_config.get("enabled", False):
        return

    ddl_path = ddl_config.get("location", "ddl")
    strategy = ddl_config.get("strategy", "use_latest_filename")

    manifest_path = os.path.join(ddl_path)
    with get_file_contents(manifest_path, strategy, s3_resource=s3_resource, filetype="json") as (buffer, _):
        tables = json.load(buffer)

    table_ops = expand_table_ops(session, restore_config.tables, existing_tables=tables)
    for table_op in table_ops:
        location = table_op.location(ref=config)
        strategy = table_op.raw_conf.get("strategy", "use_latest_filename")

        path = join_paths(ddl_path, location)
        with get_file_contents(path, strategy, s3_resource=s3_resource) as (buffer, path):
            query = buffer.read().decode("utf-8")

        query = "\n".join(
            line
            for line in query.splitlines()
            if not line.startswith("--")
            and not line.startswith("SET")
            and not line.startswith("SELECT pg_catalog")
            and line
        )
        session.execute(sqlalchemy.text(query))
        session.commit()

        log.info(f"Restored {table_op.table_name} DDL from {path}")


def truncate_tables(session: Session, table_ops: List[TableOp], adapter: Adapter):
    for table_op in table_ops:
        truncate = table_op.raw_conf.get("truncate", False)
        if not truncate:
            continue

        adapter.truncate_table(session, table_op.table_name)


def restore(
    session: Session,
    *,
    config: Config,
    adapter: Adapter,
    table_op: TableOp,
    manifest: Optional[Manifest] = None,
    s3_resource: Optional["S3ServiceResource"] = None,
) -> None:
    """Restore a CSV file from S3 to the database."""

    # Force table_name to be fully qualified
    schema, table = parse_table(table_op.table_name)
    table_name = f"{schema}.{table}"

    strategy = table_op.raw_conf.get("strategy", "use_latest_filename")
    with get_file_contents(table_op.location(config), strategy, s3_resource=s3_resource) as (buffer, path):
        with wrap_buffer(buffer) as wrapper:
            with session:
                adapter.import_csv(session, wrapper, table_op.table_name)
                session.commit()

        if manifest:
            manifest.record(table_name, path)

    log.info(f"Restored {table_name} from {path}")


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
    location: str, strategy: str, s3_resource: Optional["S3ServiceResource"] = None, filetype="csv"
) -> Generator[Tuple[io.BytesIO, str], None, None]:
    concrete_strategy = VALID_STRATEGIES[strategy]

    buffer = io.BytesIO()
    if is_s3_path(location):
        # this location.key should be a folder
        s3_location = S3Location(location)
        s3_bucket: "Bucket" = s3_resource.Bucket(s3_location.bucket)  # type: ignore

        object_generator = (
            ObjectSummary(o.key, o.last_modified) for o in s3_bucket.objects.filter(Prefix=s3_location.key)
        )
        target_object: ObjectSummary = concrete_strategy(object_generator, filetype=filetype)

        s3_bucket.download_fileobj(target_object.key, buffer)
        path = f"s3://{s3_location.bucket}/{target_object.key}"
    else:
        object_generator = (
            ObjectSummary.from_stat(os.path.sep.join([location, dir_entry.name]), dir_entry.stat())
            for dir_entry in os.scandir(location)
            if dir_entry.is_file()
        )
        target_object = concrete_strategy(object_generator, filetype=filetype)

        with open(target_object.key, "rb") as f:
            buffer.write(f.read())
        path = target_object.key

    log.info(f"Using {target_object.key}...")

    buffer.seek(0)
    yield buffer, path
    buffer.close()


def _use_filename_strategy(available_objects: Iterable[ObjectSummary], filetype="csv") -> ObjectSummary:
    objects_by_filename = {obj.path.name: obj for obj in available_objects}
    ordered_filenames = sorted(objects_by_filename.keys(), key=lambda x: restore_filename(x, filetype), reverse=True)

    return objects_by_filename[ordered_filenames[0]]


def _use_metadata_strategy(available_objects: Iterable[ObjectSummary], filetype="csv") -> ObjectSummary:
    objects_by_last_modified_date = {s3_object.last_modified: s3_object for s3_object in available_objects}
    ordered_last_modified_dates = sorted(objects_by_last_modified_date.keys(), reverse=True)

    return objects_by_last_modified_date[ordered_last_modified_dates[0]]


VALID_STRATEGIES = {
    "use_latest_metadata": _use_metadata_strategy,
    "use_latest_filename": _use_filename_strategy,
}
