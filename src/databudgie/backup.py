import contextlib
import csv
import io
from typing import Generator, List, Mapping, TypedDict

from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource
from setuplog import log
from sqlalchemy import text
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.orm import Session

from databudgie.utils import capture_failures, S3Location


class BackupConfig(TypedDict):
    query: str
    location: str


def backup_all(session: Session, s3_resource: S3ServiceResource, tables: Mapping[str, BackupConfig], **kwargs):
    """Perform backup on all tables in the config.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        s3_resource: boto S3 resource from an authenticated session.
        tables: config object mapping table names to their query and location.
        strict: terminate backup after failing one table.
    """
    strict = kwargs.get("strict", False)

    for table_name, conf in tables.items():
        log.info(f"Backing up {table_name}...")
        with capture_failures(strict=strict):
            backup(session, conf["query"], s3_resource, conf["location"], table_name, **kwargs)


def backup(session: Session, query: str, s3_resource: S3ServiceResource, location: str, table_name: str, **kwargs):
    """Dump query contents to S3 as a CSV file.

    Arguments:
        session: A SQLAlchemy session with the PostgreSQL database from which to query data.
        query: string SQL query to run against the session.
        s3_resource: boto S3 resource from an authenticated session.
        location: folder path on S3 of where to put the CSV
        table_name: identifer for the table, used in the CSV filename.
        kwargs: additional keyword arguments.
    """
    chunk_size = kwargs.get("chunk_size", 1000)

    buffer = io.BytesIO()

    with _manage_buffer(buffer) as writer:
        for row in _query_database(session, query, chunk_size):
            writer.writerow(row)

    _upload_to_s3(s3_resource, location, table_name, buffer)


def _query_database(session: Session, query: str, chunk_size: int = 1000) -> Generator[list, None, None]:
    cursor: CursorResult = session.execute(text(query))

    columns: List[str] = list(cursor.keys())
    yield columns

    row: list
    for row in cursor.yield_per(chunk_size):
        yield row


@contextlib.contextmanager
def _manage_buffer(buffer: io.BytesIO):
    wrapper = io.TextIOWrapper(buffer)
    writer = csv.writer(wrapper)
    yield writer
    wrapper.detach()
    buffer.seek(0)


def _upload_to_s3(s3_resource: S3ServiceResource, location: str, table_name: str, buffer: io.BytesIO):
    s3_location = S3Location(location)
    s3_bucket: Bucket = s3_resource.Bucket(s3_location.bucket)

    s3_bucket.put_object(Key=s3_location.key, Body=buffer)
    log.info(f"Uploaded {table_name} to s3://{s3_location.bucket}/{s3_location.key}")
