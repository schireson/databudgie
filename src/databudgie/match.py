import fnmatch
from typing import Iterable


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
