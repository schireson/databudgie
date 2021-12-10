import fnmatch
from typing import Iterable

from sqlalchemy import inspect, MetaData
from sqlalchemy.orm.session import Session


def expand_table_globs(existing_tables: Iterable[str], pattern: str):
    """Expand a given pattern into the set of tables which apply to that pattern.

    Examples:
        >>> expand_table_globs(["foo", "bar", "food", "football"], "foo*")
        ['foo', 'food', 'football']
    """
    new_tables = []
    for fq_table_name in existing_tables:
        match = fnmatch.fnmatch(fq_table_name, pattern)
        if not match:
            continue

        new_tables.append(fq_table_name)

    return sorted(new_tables)


def collect_existing_tables(session: Session):
    """Find the set of all user-defined tables in a database."""
    connection = session.connection()

    metadata = MetaData()
    insp = inspect(connection)
    for schema in insp.get_schema_names():
        # Seems to be a generally cross-database compatible filter.
        if schema == "information_schema":
            continue
        metadata.reflect(bind=connection, schema=schema)

    return [table.fullname for table in metadata.sorted_tables]
