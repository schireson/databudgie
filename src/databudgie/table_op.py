from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Sequence, TYPE_CHECKING, TypeVar

from sqlalchemy import inspect
from sqlalchemy.orm import Session

from databudgie.config import BackupTableConfig, RestoreTableConfig
from databudgie.match import expand_table_globs
from databudgie.output import Console, default_console
from databudgie.utils import parse_table

if TYPE_CHECKING:
    from databudgie.storage import StorageBackend

T = TypeVar("T", BackupTableConfig, RestoreTableConfig)


@dataclass
class SchemaOp(Generic[T]):
    name: str
    raw_conf: T

    def location(self) -> str:
        return self.raw_conf.location.format(table=self.name)


@dataclass
class TableOp(Generic[T]):
    """Represents an operation (backup/restore) being performed on a table.

    Fields:
     * `schema`: The expanded name of the table's schema. I.e. the default schema if left unspecified.
     * `table_name`: The raw table name without a schema.
     * `full_name`: The full table name, including the schema.
     * `raw_conf`: directly relates to the raw table config in a backup/restore config.

    Note all `TableOp` records should correspond to concrete tables. That is, given some
    globbed input table "public.*", a `TableOp` will only be produced for a concrete
    table matching that criteria.
    """

    schema: str
    table_name: str
    full_name: str
    raw_conf: T

    @classmethod
    def from_name(cls, full_name: str, raw_conf: T):
        schema, table_name = parse_table(full_name)
        return cls(schema=schema, table_name=table_name, full_name=full_name, raw_conf=raw_conf)

    def location(self) -> str:
        return self.raw_conf.location.format(table=self.full_name)

    def query(self) -> str:
        query = getattr(self.raw_conf, "query")  # RestoreTableConfig has no query attribute
        if query is None:
            query = "SELECT * FROM {table}"

        return query.format(table=self.full_name)

    def schema_op(self) -> SchemaOp:
        return SchemaOp(self.schema, self.raw_conf)


def expand_table_ops(
    session: Session,
    tables: Sequence[T],
    existing_tables: list[str],
    storage: StorageBackend,
    *,
    console: Console = default_console,
    warn_for_unused_tables: bool = False,
) -> list[TableOp[T]]:
    """Produce a full list of table operations to be performed.

    tables in the set of `tables` may be globbed and produce more concrete
    tables than initially specified in the input set.

    Additionally, tables may be filtered, either by the pre-existence of
    manifest data or explicit table exclusions.
    """
    # Avoid hardcoding things like "public", we hardcode this elsewhere, this
    # should probably be moved upstream.
    insp = inspect(session.connection())
    default_schema_name = insp.default_schema_name

    # expand table globs into fully qualified mappings to the config.
    matching_tables: dict[str, list[T]] = {}
    for table_conf in tables:
        pattern = table_conf.name
        if "." not in pattern:
            pattern = f"{default_schema_name}.{pattern}"

        expanded_tables = expand_table_globs(existing_tables, pattern)
        if warn_for_unused_tables and not expanded_tables:
            console.warn(f"Skipping table definition `{pattern}` which did not match any tables.")
            continue

        for table_name in expanded_tables:
            if storage.check_manifest(table_name):
                console.trace(f"Skipping {table_name}...")
                continue

            for exclusion_pattern in table_conf.exclude:
                exclusions = set(expand_table_globs(existing_tables, exclusion_pattern))
                if table_name in exclusions:
                    break

                # Breaking out of this loop implies we've identified that this
                # table should be excluded. Thus backup being gated on unbroken
                # iteration of this loop.
            else:
                matching_tables.setdefault(table_name, []).append(table_conf)

    # Notably, `existing_tables` is assumed to be sorted by table-fk dependencies,
    # which is why this collected separately from this loop, where we iterate
    # over unordered input tables.
    result = []
    for table in existing_tables:
        table_confs = matching_tables.get(table)
        if not table_confs:
            continue

        for table_conf in table_confs:
            table_op = TableOp.from_name(table, raw_conf=table_conf)
            result.append(table_op)

    return result
