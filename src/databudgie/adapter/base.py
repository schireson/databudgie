import csv
import io
import warnings
from dataclasses import dataclass, replace
from typing import Any, Dict, Generator, List, Optional

import sqlalchemy
from sqlalchemy import inspect, MetaData, Table, text
from sqlalchemy.orm import Session

from databudgie.output import Console, default_console
from databudgie.table_op import TableOp
from databudgie.utils import join_paths, parse_table


@dataclass
class Adapter:
    """Root class designating a shared interface for operating on different databases.

    Any database-agnostic functionality should live here, while more optimized
    database-specific implementations should be implemented on specialized `Adaptor`
    subclasses.

    Uses native Python CSV methods with a lightweight/naive type conversion on insert.
    """

    session: Session

    @classmethod
    def get_adapter(cls, session: Session, dialect: Optional[str] = None) -> "Adapter":
        """Determine an interface based on the dialect name from the Session (or an explicit string).

        Examples:
            >>> from databudgie.adapter.postgres import PostgresAdapter
            >>> adapter = Adapter.get_adapter(None, "postgres")
            >>> isinstance(adapter, PostgresAdapter)
            True

            >>> from databudgie.adapter import Adapter
            >>> adapter = Adapter.get_adapter(None, "python")
            >>> isinstance(adapter, Adapter)
            True
        """
        if dialect is None:
            dialect = session.get_bind().dialect.name

        if dialect in ("postgres", "postgresql"):
            from databudgie.adapter.postgres import PostgresAdapter

            return PostgresAdapter(session)
        else:
            return cls(session)

    def export_query(self, query: str, dest: io.StringIO):
        def query_database(session: Session, query: str) -> Generator[List[Any], None, None]:
            cursor = session.execute(text(query))

            columns: List[str] = list(cursor.keys())
            yield columns

            row: List[Any]
            for row in cursor:
                yield row

        writer = csv.writer(dest, quoting=csv.QUOTE_MINIMAL)
        for row in query_database(self.session, query):
            writer.writerow(row)

    def import_csv(self, csv_file: io.TextIOBase, table: str):
        reader = csv.DictReader(csv_file, quoting=csv.QUOTE_MINIMAL)

        prepared_rows: List[dict] = []
        for row in reader:
            new_row: Dict[str, Any] = dict(row)
            for key, value in new_row.items():
                if value.lower() == "true":
                    new_row[key] = True
                elif value.lower() == "false":
                    new_row[key] = False
                elif value == "":
                    new_row[key] = None

            prepared_rows.append(new_row)

        schema, table_name = parse_table(table)

        engine = self.session.get_bind()
        metadata = MetaData()
        metadata.reflect(engine, schema=schema)
        table_ref = Table(table_name, metadata, autoload=True, autoload_with=engine, schema=schema)

        engine.execute(table_ref.insert(), prepared_rows)

    def export_schema_ddl(self, name: str):
        raise NotImplementedError()  # pragma: no cover

    def export_table_ddl(self, table_name: str, console: Console = default_console):
        raise NotImplementedError()  # pragma: no cover

    def truncate_table(self, table: str):
        try:
            self.session.execute(f"TRUNCATE TABLE {table} CASCADE")
            self.session.commit()
        except sqlalchemy.exc.ProgrammingError:
            self.session.rollback()

    def reset_database(self):
        """Reset the database in a database-backend agnostic way.

        Create a temp database, connect to it, drop the target database, and drop
        the temp database.

        This method suffers from being rather prone to failure, but is better
        than nothing!
        """
        raise NotImplementedError()

    def collect_existing_tables(self) -> List[str]:
        """Find the set of all user-defined tables in a database."""
        connection = self.session.connection()

        metadata = MetaData()
        insp = inspect(connection)
        for schema in insp.get_schema_names():
            # Seems to be a generally cross-database compatible filter.
            if schema in ("information_schema", "pg_catalog"):
                continue

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                metadata.reflect(bind=connection, schema=schema)

        return [table.fullname for table in metadata.sorted_tables]

    def collect_table_dependencies(self, table_op: TableOp, console: Console = default_console) -> List[str]:
        raise NotImplementedError()

    def collect_table_sequences(self) -> Dict[str, List[str]]:
        raise NotImplementedError()

    def collect_sequence_value(self, sequence_name: str) -> int:
        raise NotImplementedError()

    def restore_sequence_value(self, sequence_name: str, value: int) -> int:
        raise NotImplementedError()

    def materialize_table_dependencies(
        self,
        table_ops: List[TableOp],
        reverse: bool = False,
        console: Console = default_console,
    ) -> List[TableOp]:
        tables = set()
        dependent_table_ops = []
        for table_op in table_ops:
            tables.add(table_op.full_name)

            if not table_op.raw_conf.follow_foreign_keys:
                continue

            dependent_tables = self.collect_table_dependencies(table_op=table_op, console=console)
            for dependent_table in dependent_tables:
                if dependent_table not in tables:
                    table_location = join_paths(table_op.location(), "{table}")
                    conf = replace(table_op.raw_conf, name=dependent_table, location=table_location)

                    dependent_table_op = TableOp.from_name(dependent_table, conf)
                    tables.add(dependent_table)
                    dependent_table_ops.append(dependent_table_op)

        if reverse:
            # The original `table_ops` list comes to us already reverse
            # ordered, so we need to preserve it's original order, and
            # just invert the net-new tables and put them first.
            return list(reversed(dependent_table_ops)) + table_ops

        return table_ops + dependent_table_ops
