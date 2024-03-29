from __future__ import annotations

import contextlib
import csv
import enum
import io
import os
import pathlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Generator, Iterable, Optional, TYPE_CHECKING

from databudgie.adapter.base import QueryResult
from databudgie.compression import Compressor
from databudgie.config import BackupConfig, RestoreConfig
from databudgie.manifest.manager import Manifest
from databudgie.output import Console, default_console, Table
from databudgie.s3 import is_s3_path, optional_s3_resource, S3Location
from databudgie.utils import join_paths

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket, S3ServiceResource

DATETIME_FORMAT = r"%Y-%m-%dT%H:%M:%S"
DATE_FORMAT = r"%Y-%m-%d"


@enum.unique
class FileTypes(enum.Enum):
    ddl = "ddl"
    sequences = "sequences"
    data = "data"
    manifest = "manifest"

    @property
    def extension(self) -> str:
        mapping = {
            self.ddl: "sql",
            self.sequences: "json",
            self.data: "csv",
            self.manifest: "json",
        }
        return mapping[self]  # type: ignore


@dataclass
class LocalStorage:
    def write_buffer(self, path: str, buffer: io.BytesIO):
        npath = pathlib.PurePath(path)

        parent = npath.parent
        os.makedirs(parent, exist_ok=True)
        with open(path, "wb") as f:
            f.write(buffer.getbuffer())

    def path_exists(self, path: str) -> bool:
        if not os.path.exists("path"):
            return False

        return any(dir_entry for dir_entry in os.scandir(path) if dir_entry.is_file())

    def get_file_content(self, path: str, selection_strategy: SelectionStrategy) -> FileObject | None:
        parent_path = pathlib.PurePath(path).parent

        try:
            files = os.scandir(parent_path)
        except FileNotFoundError:
            return None

        object_generator = (
            FileStat.from_stat(dir_entry.path, dir_entry.stat())
            for dir_entry in files
            if dir_entry.is_file() and match_path(dir_entry.path, path)
        )

        target_object = selection_strategy(object_generator)
        if not target_object:
            return None

        buffer = io.BytesIO()
        with open(target_object.path, "rb") as f:
            buffer.write(f.read())
        buffer.seek(0)

        return FileObject(path=str(target_object.path), content=buffer)


@dataclass
class S3Storage:
    resource: S3ServiceResource

    @classmethod
    def from_config(cls, config: BackupConfig | RestoreConfig) -> S3Storage | None:
        resource = optional_s3_resource(config)
        if not resource:
            return None

        return cls(resource=resource)

    def write_buffer(self, path: str, buffer: io.BytesIO):
        s3_location = S3Location(path)
        s3_bucket: Bucket = self.resource.Bucket(s3_location.bucket)
        s3_bucket.put_object(Key=s3_location.key, Body=buffer)

    def path_exists(self, path: str) -> bool:
        s3_location = S3Location(path)
        s3_bucket: Bucket = self.resource.Bucket(s3_location.bucket)
        matching_objects = list(s3_bucket.objects.filter(Prefix=s3_location.key).all())
        return len(matching_objects) >= 1

    def get_file_content(self, path: str, selection_strategy: SelectionStrategy) -> FileObject | None:
        buffer = io.BytesIO()

        # this path.key should be a folder
        s3_location = S3Location(path)
        s3_bucket: Bucket = self.resource.Bucket(s3_location.bucket)

        object_generator = self._generate_s3_object_summaries(s3_bucket, s3_location)
        target_object = selection_strategy(object_generator)
        if not target_object:
            return None

        s3_bucket.download_fileobj(str(target_object.path), buffer)
        path = f"s3://{s3_location.bucket}/{target_object.path}"

        buffer.seek(0)
        return FileObject(path, buffer)

    def _generate_s3_object_summaries(self, s3_bucket, s3_location):
        parent_path = str(pathlib.PurePath(s3_location.key).parent)
        for o in s3_bucket.objects.filter(Prefix=parent_path):
            object_summary = FileStat(o.key, o.last_modified)
            if match_path(str(object_summary.path), s3_location.key):
                yield object_summary


@dataclass
class StorageBackend:
    local_storage: LocalStorage
    s3_storage: S3Storage | None = None

    timestamp: datetime = field(default_factory=lambda: datetime.utcnow())

    manifest: Manifest | None = None
    events: dict[str, TableInfo] = field(default_factory=dict)

    record_stats: bool = False
    perform_writes: bool = False

    @classmethod
    def from_config(
        cls,
        config: BackupConfig | RestoreConfig,
        manifest: Manifest | None = None,
        perform_writes=True,
        record_stats=False,
    ) -> StorageBackend:
        return cls(
            local_storage=LocalStorage(),
            s3_storage=S3Storage.from_config(config),
            manifest=manifest,
            perform_writes=perform_writes,
            record_stats=record_stats,
        )

    def check_manifest(self, table_name: str):
        return self.manifest and table_name in self.manifest

    def choose_storage(self, path: str) -> LocalStorage | S3Storage:
        if is_s3_path(path):
            assert self.s3_storage
            return self.s3_storage
        if self.local_storage:
            return self.local_storage

        raise ValueError("No storage backend found")

    def write_buffer(
        self,
        filename: str,
        _buffer: io.BytesIO | QueryResult,
        *,
        file_type: FileTypes,
        name: str | None = None,
        compression: str | None = None,
    ):
        if isinstance(_buffer, QueryResult):
            buffer = _buffer.buffer
        else:
            buffer = _buffer

        if name and self.record_stats:
            table_info = self.events.setdefault(name, TableInfo(name=name))
            table_info.note_file_type(file_type)

            if file_type == FileTypes.data:
                assert isinstance(_buffer, QueryResult)
                row_count = _buffer.row_count
                table_info.rows = row_count

        filename = self.format_path(filename, name=name, file_type=file_type, compression=compression)

        if self.perform_writes:
            final_buffer = Compressor.get_with_name(compression).compress(buffer)

            storage = self.choose_storage(filename)

            storage.write_buffer(filename, final_buffer)

            # `name` is primarily omitted for things spanning individual tables, like schemas.
            if name and self.manifest and file_type == FileTypes.data:
                self.manifest.record(name, filename)

        return filename

    def format_path(
        self,
        *segments: str,
        file_type: FileTypes,
        name: str | None = None,
        compression=None,
        format_timestamp: bool = True,
    ):
        base_extension = file_type.extension
        full_extension = Compressor.get_with_name(compression).compose_filetype(base_extension)
        filename_template = join_paths(*segments)
        if format_timestamp:
            timestamp = self.timestamp.strftime(DATETIME_FORMAT)
            date = self.timestamp.strftime(DATE_FORMAT)
        else:
            timestamp = "{timestamp}"
            date = "{date}"

        return filename_template.format(table=name, timestamp=timestamp, ext=full_extension, date=date)

    def path_exists(self, path: str, *, file_type: FileTypes, name: str | None = None, compression=None) -> bool:
        full_path = self.format_path(path, name=name, file_type=file_type, compression=compression)
        storage = self.choose_storage(full_path)
        return storage.path_exists(full_path)

    @contextlib.contextmanager
    def get_file_content(
        self,
        path: str,
        strategy: str,
        *,
        file_type: FileTypes,
        name: str | None = None,
        compression=None,
    ) -> Generator[FileObject, None, None]:
        full_path = self.format_path(
            path, name=name, file_type=file_type, compression=compression, format_timestamp=False
        )
        storage = self.choose_storage(full_path)
        selection_strategy = FileSelectionStrategy.by_name(strategy)

        cbuffer = None
        file_object = storage.get_file_content(full_path, selection_strategy)

        if file_object:
            cbuffer = Compressor.get_with_name(compression).extract(file_object.content)

        yield FileObject(path=full_path, content=cbuffer)
        if not cbuffer:
            return

        if name and self.record_stats:
            table_info = self.events.setdefault(name, TableInfo(name=name))
            table_info.note_file_type(file_type)

            if file_type == FileTypes.data:
                cbuffer.seek(0)
                row_count = len(list(csv.DictReader(io.TextIOWrapper(cbuffer))))
                table_info.rows = row_count

        if self.manifest and self.perform_writes and file_type == file_type.data and name and file_object:
            self.manifest.record(name, file_object.path)
        cbuffer.close()

    def print_stats(self, console: Console = default_console):
        table = Table(title="Stats")

        table.add_column("Table", justify="right", style="cyan", no_wrap=True)
        table.add_column("DDL", justify="right", style="green")
        table.add_column("Sequences", justify="right", style="green")
        table.add_column("Data", justify="right", style="green")
        table.add_column("Rows", justify="right")

        for row in self.events.values():
            table.add_row(
                row.name,
                "✓" if row.ddl else "",
                "✓" if row.sequences else "",
                "✓" if row.data else "",
                str(row.rows) if row.rows is not None else "",
            )

        console.print(table)


@dataclass
class TableInfo:
    name: str
    ddl: bool = False
    sequences: bool = False
    data: bool = False
    rows: int | None = None

    def note_file_type(self, file_type: FileTypes):
        if file_type == file_type.ddl:
            self.ddl = True
        elif file_type == file_type.sequences:
            self.sequences = True
        elif file_type == file_type.data:
            self.data = True


@dataclass(frozen=True)
class FileObject:
    path: str
    content: io.BytesIO | None


@dataclass(frozen=True)
class FileStat:
    path_str: str
    last_modified: datetime

    @property
    def path(self):
        return pathlib.PurePath(self.path_str)

    @classmethod
    def from_stat(cls, name, stat: os.stat_result):
        return cls(name, datetime.fromtimestamp(stat.st_mtime))


SelectionStrategy = Callable[[Iterable[FileStat]], Optional[FileStat]]


class FileSelectionStrategy:
    @classmethod
    def by_name(cls, name: str) -> SelectionStrategy:
        valid_strategies = {
            "use_latest_metadata": cls.use_metadata_strategy,
            "use_latest_filename": cls.use_filename_strategy,
        }
        return valid_strategies[name]

    @staticmethod
    def use_filename_strategy(available_objects: Iterable[FileStat]) -> FileStat | None:
        objects_by_filename = {obj.path.name: obj for obj in available_objects}
        ordered_filenames = sorted(objects_by_filename.keys(), reverse=True)

        if ordered_filenames:
            return objects_by_filename[ordered_filenames[0]]
        return None

    @staticmethod
    def use_metadata_strategy(available_objects: Iterable[FileStat]) -> FileStat | None:
        objects_by_last_modified_date = {s3_object.last_modified: s3_object for s3_object in available_objects}
        ordered_last_modified_dates = sorted(objects_by_last_modified_date.keys(), reverse=True)

        if ordered_last_modified_dates:
            return objects_by_last_modified_date[ordered_last_modified_dates[0]]
        return None


def match_path(path: str, pattern: str) -> bool:
    escaped_pattern = (
        re.escape(pattern)
        .replace(r"\{timestamp\}", r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d")
        .replace(r"\{date\}", r"\d\d\d\d-\d\d-\d\d")
    )
    return bool(re.match(escaped_pattern, path))
