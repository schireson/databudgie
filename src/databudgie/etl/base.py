from dataclasses import dataclass
from typing import Dict, Generic, List, Optional, Sequence, TypeVar

from setuplog import log
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from databudgie.config.models import BackupTableConfig, RestoreTableConfig
from databudgie.manifest.manager import Manifest
from databudgie.match import expand_table_globs
from databudgie.utils import parse_table

T = TypeVar("T", BackupTableConfig, RestoreTableConfig)


@dataclass
class SchemaOp(Generic[T]):
    name: str
    raw_conf: T

    @classmethod
    def from_table_op(cls, table_op: "TableOp") -> "SchemaOp":
        schema, _ = parse_table(table_op.table_name)
        return cls(schema, table_op.raw_conf)

    def location(self) -> str:
        return self.raw_conf.location.format(table=self.name)


@dataclass
class TableOp(Generic[T]):
    """Represents an operation (backup/restore) being performed on a table.

    * `table_name` is the expanded (fully qualified, globbed) name of the
       table in config. In the event of globbing, there may be more `TableOp`s
       produced than specified in config.
    * `raw_conf` directly relates to the raw table config in a backup/restore config.
    """

    table_name: str
    raw_conf: T

    def location(self) -> str:
        return self.raw_conf.location.format(table=self.table_name)

    def query(self) -> str:
        query = getattr(self.raw_conf, "query")  # RestoreTableConfig has no query attribute
        if query is None:
            query = "SELECT * FROM {table}"

        return query.format(table=self.table_name)

    def schema_op(self) -> SchemaOp:
        return SchemaOp.from_table_op(self)


def expand_table_ops(
    session: Session,
    tables: Sequence[T],
    existing_tables: List[str],
    *,
    manifest: Optional[Manifest] = None,
) -> List[TableOp[T]]:
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
    matching_tables: Dict[str, List[T]] = {}
    for table_conf in tables:
        pattern = table_conf.name
        if "." not in pattern:
            pattern = f"{default_schema_name}.{pattern}"

        for table_name in expand_table_globs(existing_tables, pattern):
            if manifest and table_name in manifest:
                log.info(f"Skipping {table_name}...")
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
            table_op = TableOp(table, raw_conf=table_conf)
            result.append(table_op)

    return result
