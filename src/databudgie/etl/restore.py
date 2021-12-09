import contextlib
import io
from datetime import datetime
from typing import Generator, Iterable, List, Optional, Tuple

from configly import Config
from mypy_boto3_s3.service_resource import Bucket, ObjectSummary, S3ServiceResource
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.compat import TypedDict
from databudgie.etl.base import expand_table_ops, TableOp
from databudgie.manifest.manager import Manifest
from databudgie.utils import capture_failures, FILENAME_FORMAT, parse_table, S3Location, wrap_buffer


class RestoreConfig(TypedDict):
    strategy: str
    location: str
    truncate: bool


def restore_all(
    session: Session,
    s3_resource: S3ServiceResource,
    config: Config,
    manifest: Optional[Manifest] = None,
    strict=False,
    adapter: Optional[str] = None,
) -> None:
    """Perform restore on all tables in the config."""
    table_ops = expand_table_ops(session, config.restore.tables, manifest=manifest)

    actual_adapter = Adapter.get_adapter(adapter or session)

    truncate_tables(session, table_ops, adapter=actual_adapter)

    for table_op in table_ops:
        log.info(f"Restoring {table_op.table_name}...")

        with capture_failures(strict=strict):
            restore(session, s3_resource, config=config, table_op=table_op, manifest=manifest, adapter=actual_adapter)


def truncate_tables(session: Session, table_ops: List[TableOp], adapter: Adapter):
    for table_op in table_ops:
        truncate = table_op.raw_conf.get("truncate", False)
        if not truncate:
            continue

        adapter.truncate_table(session, table_op.table_name)


def restore(
    session: Session,
    s3_resource: S3ServiceResource,
    *,
    config: Config,
    adapter: Adapter,
    table_op: TableOp,
    manifest: Optional[Manifest] = None,
) -> None:
    """Restore a CSV file from S3 to the database."""

    # Force table_name to be fully qualified
    schema, table = parse_table(table_op.table_name)
    table_name = f"{schema}.{table}"

    strategy = table_op.raw_conf.get("strategy", "use_latest_filename")
    with _download_from_s3(s3_resource, table_op.location(config), strategy) as (buffer, s3_path):
        with wrap_buffer(buffer) as wrapper:
            with session:
                adapter.import_csv(session, wrapper, table_op.table_name)
                session.commit()

        if manifest:
            manifest.record(table_name, s3_path)

    log.info(f"Restored {table_name} from {s3_path}")


@contextlib.contextmanager
def _download_from_s3(
    s3_resource: S3ServiceResource, location: str, strategy: str
) -> Generator[Tuple[io.BytesIO, str], None, None]:
    # this location.key should be a folder
    s3_location = S3Location(location)
    s3_bucket: Bucket = s3_resource.Bucket(s3_location.bucket)

    object_generator: Iterable[ObjectSummary] = s3_bucket.objects.filter(Prefix=s3_location.key)
    target_object: ObjectSummary = VALID_STRATEGIES[strategy](object_generator)
    log.info(f"Using {target_object.key}...")

    buffer = io.BytesIO()
    s3_bucket.download_fileobj(target_object.key, buffer)
    buffer.seek(0)

    yield buffer, f"s3://{s3_location.bucket}/{target_object.key}"

    buffer.close()


def _use_filename_strategy(available_objects: Iterable[ObjectSummary]) -> ObjectSummary:
    objects_by_filename = {s3_object.key.split("/")[-1]: s3_object for s3_object in available_objects}
    ordered_filenames = sorted(
        objects_by_filename.keys(), key=lambda x: datetime.strptime(x, FILENAME_FORMAT), reverse=True
    )

    return objects_by_filename[ordered_filenames[0]]


def _use_metadata_strategy(available_objects: Iterable[ObjectSummary]) -> ObjectSummary:
    objects_by_last_modified_date = {s3_object.last_modified: s3_object for s3_object in available_objects}
    ordered_last_modified_dates = sorted(objects_by_last_modified_date.keys(), reverse=True)

    return objects_by_last_modified_date[ordered_last_modified_dates[0]]


VALID_STRATEGIES = {
    "use_latest_metadata": _use_metadata_strategy,
    "use_latest_filename": _use_filename_strategy,
}
