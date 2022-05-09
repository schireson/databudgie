import abc
import io
import warnings
from typing import Any, List, Union

import sqlalchemy
from setuplog import log, log_duration
from sqlalchemy import inspect, MetaData
from sqlalchemy.orm import Session


class Adapter(metaclass=abc.ABCMeta):
    """Root class designating a shared interface for operating on different databases."""

    @abc.abstractmethod
    def export_query(self, session: Session, query: str, dest: io.StringIO):
        raise NotImplementedError()  # pragma: no cover

    @abc.abstractmethod
    def import_csv(self, session: Session, csv_file: io.StringIO, table: str):
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
        log.info(f"Truncating {table}...")
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
    @log_duration("Collecting existing tables")
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
