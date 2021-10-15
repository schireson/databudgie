import contextlib
import io
from datetime import datetime
from typing import Generator, Iterable, Mapping, Optional, Tuple

from mypy_boto3_s3.service_resource import Bucket, ObjectSummary, S3ServiceResource
from setuplog import log
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.compat import TypedDict
from databudgie.manifest.manager import Manifest
from databudgie.utils import capture_failures, FILENAME_FORMAT, parse_table, S3Location, wrap_buffer


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
    strategy: str = "use_latest_filename",
    truncate: bool = False,
    **kwargs,
) -> None:
    """Restore a CSV file from S3 to the database."""

    adapter = Adapter.get_adapter(kwargs.get("adapter", None) or session)

    # Force table_name to be fully qualified
    schema, table = parse_table(table_name)
    table_name = f"{schema}.{table}"

    with _download_from_s3(s3_resource, location, strategy) as (buffer, s3_path):
        with wrap_buffer(buffer) as wrapper:
            with session:
                if truncate:
                    adapter.truncate_table(session, table_name)
                adapter.import_csv(session, wrapper, table_name)

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
