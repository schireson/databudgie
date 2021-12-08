from dataclasses import dataclass
from typing import Iterator, Optional

from setuplog import log

from databudgie.manifest.manager import Manifest
from databudgie.match import collect_existing_tables, expand_table_globs


@dataclass
class TableOp:
    table_name: str
    query: str
    location: str


def expand_table_ops(session, tables, *, ref=None, manifest: Optional[Manifest] = None) -> Iterator[TableOp]:
    existing_tables = collect_existing_tables(session)

    for pattern, table_conf in tables.items():
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
                format_kwargs = dict(table=table_name, ref=ref)

                query = table_conf["query"].format(**format_kwargs)
                location = table_conf["location"].format(**format_kwargs)

                yield TableOp(table_name, query, location)
