from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from setuplog import log
from sqlalchemy import inspect

from databudgie.manifest.manager import Manifest
from databudgie.match import collect_existing_tables, expand_table_globs


@dataclass
class TableOp:
    """Represents an operation (backup/restore) being performed on a table.

    * `table_name` is the expanded (fully qualified, globbed) name of the
       table in config. In the event of globbing, there may be more `TableOp`s
       produced than specified in config.
    * `raw_conf` directly relates to the raw table config in a backup/restore config.
    """

    table_name: str
    raw_conf: Dict[str, Any]

    def location(self, ref):
        return self.raw_conf["location"].format(table=self.table_name, ref=ref)

    def query(self, ref):
        query = self.raw_conf.get("query")
        if not query:
            return None

        return query.format(table=self.table_name, ref=ref)


def expand_table_ops(
    session, tables, *, manifest: Optional[Manifest] = None, existing_tables: Optional[List[str]] = None
) -> List[TableOp]:
    """Produce a full list of table operations to be performed.

    tables in the set of `tables` may be globbed and produce more concrete
    tables than initially specified in the input set.

    Additionally, tables may be filtered, either by the pre-existence of
    manifest data or explicit table exclusions.
    """
    existing_tables = existing_tables or collect_existing_tables(session)

    # Avoid hardcoding things like "public", we hardcode this elsewhere, this
    # should probably be moved upstream.
    insp = inspect(session.connection())
    default_schema_name = insp.default_schema_name

    # expand table globs into fully qualified mappings to the config.
    matching_tables = {}
    for pattern, table_conf in tables.items():
        if "." not in pattern:
            pattern = f"{default_schema_name}.{pattern}"

        for table_name in expand_table_globs(existing_tables, pattern):
            if manifest and table_name in manifest:
                log.info(f"Skipping {table_name}...")
                continue

            for exclusion_pattern in table_conf.get("exclude", []):
                exclusions = set(expand_table_globs(existing_tables, exclusion_pattern))
                if table_name in exclusions:
                    break

                # Breaking out of this loop implies we've identified that this
                # table should be excluded. Thus backup being gated on unbroken
                # iteration of this loop.
            else:
                matching_tables[table_name] = table_conf

    # Notably, `existing_tables` is assumed to be sorted by table-fk dependencies,
    # which is why this collected separately from this loop, where we iterate
    # over unordered input tables.
    result = []
    for table in existing_tables:
        table_conf = matching_tables.get(table)
        if not table_conf:
            continue

        table_op = TableOp(table, raw_conf=table_conf)
        result.append(table_op)
    return result
