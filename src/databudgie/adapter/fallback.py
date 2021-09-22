import csv
import io
from typing import Generator, List, Tuple

from setuplog import log
from sqlalchemy import MetaData, Table, text
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.orm import Session

from databudgie.adapter.base import BaseAdapter


class PythonAdapter(BaseAdapter):
    """Fallback option for adapters we have not implemented.

    Uses native Python CSV methods with a lightweight/naive type conversion on insert.
    """

    def export_query(self, session: Session, query: str, dest: io.StringIO):
        writer = csv.writer(dest, quoting=csv.QUOTE_MINIMAL)
        for i, row in enumerate(self._query_database(session, query), start=1):
            writer.writerow(row)

            if i % 1000 == 0:
                log.info(f"Writing {i} rows...")

    def import_csv(self, session: Session, csv_file: io.StringIO, table: str):
        reader = csv.DictReader(csv_file, quoting=csv.QUOTE_MINIMAL)

        prepared_rows: List[dict] = []
        for i, row in enumerate(reader, start=1):
            for key, value in row.items():
                if value in ("true", "True"):
                    row[key] = True  # type: ignore
                elif value in ("false", "False"):
                    row[key] = False  # type: ignore
                elif value == "":
                    row[key] = None  # type: ignore

            prepared_rows.append(row)
            if i % 1000 == 0:
                log.info(f"Preparing {i} rows for {table}...")

        schema, table_name = self._parse_table(table)

        engine = session.get_bind()
        metadata = MetaData()
        metadata.reflect(engine, schema=schema)
        table_ref = Table(table_name, metadata, autoload=True, autoload_with=engine, schema=schema)

        engine.execute(table_ref.insert(), prepared_rows)
        log.info(f"Inserted {len(prepared_rows)} rows into {table}")

    def _query_database(self, session: Session, query: str, chunk_size: int = 1000) -> Generator[list, None, None]:
        cursor: CursorResult = session.execute(text(query))

        columns: List[str] = list(cursor.keys())
        yield columns

        row: list
        for row in cursor.yield_per(chunk_size):
            yield row

    @staticmethod
    def _parse_table(table: str) -> Tuple[str, str]:
        """Split a schema-qualified table name into two parts.

        Examples:
            >>> PythonAdapter._parse_table("myschema.foo")
            ('myschema', 'foo')

            >>> PythonAdapter._parse_table("bar")
            ('public', 'bar')

            >>> PythonAdapter._parse_table("...")  # doctest: +IGNORE_EXCEPTION_DETAIL
            Traceback (most recent call last):
            ValueError: Invalid table name: ...
        """
        parts = table.split(".")

        if len(parts) == 1:
            schema = "public"
            table = parts[0]
        elif len(parts) == 2:
            schema, table = parts
        else:
            raise ValueError(f"Invalid table name: {table}")

        return schema, table
