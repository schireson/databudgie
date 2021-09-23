import abc
import io
from typing import Any, Union

from sqlalchemy.orm import Session


class Adapter(metaclass=abc.ABCMeta):
    """Root class designating a shared interface for operating on different databases.
    """

    @abc.abstractmethod
    def export_query(self, session: Session, query: str, dest: io.StringIO):
        raise NotImplementedError()  # pragma: no cover

    @abc.abstractmethod
    def import_csv(self, session: Session, csv_file: io.StringIO, table: str):
        raise NotImplementedError()  # pragma: no cover

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
