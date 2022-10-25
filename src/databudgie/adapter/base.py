import abc
import io
import warnings
from dataclasses import replace
from typing import Any, Dict, List, Union

import sqlalchemy
from sqlalchemy import inspect, MetaData
from sqlalchemy.orm import Session

from databudgie.etl.base import TableOp
from databudgie.output import Console, default_console
from databudgie.utils import join_paths


class Adapter(metaclass=abc.ABCMeta):
    """Root class designating a shared interface for operating on different databases."""

    @abc.abstractmethod
    def export_query(self, session: Session, query: str, dest: io.StringIO):
        raise NotImplementedError()  # pragma: no cover

    @abc.abstractmethod
    def import_csv(self, session: Session, csv_file: io.TextIOBase, table: str):
        raise NotImplementedError()  # pragma: no cover

    @staticmethod
    @abc.abstractmethod
    def export_schema_ddl(session: Session, name: str):
        raise NotImplementedError()  # pragma: no cover

    @staticmethod
    @abc.abstractmethod
    def export_table_ddl(session: Session, table_name: str):
        raise NotImplementedError()  # pragma: no cover

    def truncate_table(self, session, table: str):
        try:
            session.execute(f"TRUNCATE TABLE {table} CASCADE")
            session.commit()
        except sqlalchemy.exc.ProgrammingError:
            session.rollback()

    @staticmethod
    def get_adapter(dialect: Union[Session, Any]) -> "Adapter":
        """Determine an interface based on the dialect name from the Session (or an explicit string).

        Examples:
            >>> from databudgie.adapter.postgres import PostgresAdapter
            >>> adapter = Adapter.get_adapter("postgres")
            >>> isinstance(adapter, PostgresAdapter)
            True

            >>> from databudgie.adapter.fallback import PythonAdapter
            >>> adapter = Adapter.get_adapter("python")
            >>> isinstance(adapter, PythonAdapter)
            True
        """
        if isinstance(dialect, Session):
            dialect = dialect.get_bind().dialect.name

        if dialect in ("postgres", "postgresql"):
            from databudgie.adapter.postgres import PostgresAdapter

            return PostgresAdapter()
        else:
            from databudgie.adapter.fallback import PythonAdapter

            return PythonAdapter()

    @staticmethod
    def reset_database(session: Session):
        raise NotImplementedError()

    @staticmethod
    def collect_existing_tables(session: Session) -> List[str]:
        """Find the set of all user-defined tables in a database."""
        connection = session.connection()

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

    @staticmethod
    def collect_table_dependencies(
        session: Session, table_op: TableOp, console: Console = default_console
    ) -> List[str]:
        raise NotImplementedError()

    @staticmethod
    def collect_table_sequences(session: Session) -> Dict[str, List[str]]:
        raise NotImplementedError()

    @staticmethod
    def collect_sequence_value(session: Session, sequence_name: str) -> int:
        raise NotImplementedError()

    @staticmethod
    def restore_sequence_value(session: Session, sequence_name: str, value: int) -> int:
        raise NotImplementedError()

    @classmethod
    def materialize_table_dependencies(
        cls,
        session: Session,
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

            dependent_tables = cls.collect_table_dependencies(session, console=console, table_op=table_op)
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
