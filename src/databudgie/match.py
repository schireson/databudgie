import fnmatch
from typing import Iterable

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
    """Finds the set of all user-defined tables in a database."""
    connection = session.connection()
    dialect = connection.dialect

    fq_table_names = []

    schema_names = dialect.get_schema_names(session)
    for schema_name in schema_names:
        # Seems to be a generally cross-database compatible filter.
        if schema_name == "information_schema":
            continue

        table_names = dialect.get_table_names(session, schema=schema_name)

        for table_name in table_names:
            fq_table_names.append(f"{schema_name}.{table_name}")

    return sorted(fq_table_names)
