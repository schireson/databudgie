import csv
import io
from typing import Any, Dict, Generator, List, Tuple

from setuplog import log
from sqlalchemy import MetaData, Table, text
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.orm import Session

from databudgie.adapter.base import Adapter


class PythonAdapter(Adapter):
    """Fallback option for unimplemented database adapters.

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
            new_row: Dict[str, Any] = dict(row)
            for key, value in new_row.items():
                if value.lower() == "true":
                    new_row[key] = True
                elif value.lower() == "false":
                    new_row[key] = False
                elif value == "":
                    new_row[key] = None

            prepared_rows.append(new_row)
            if i % 1000 == 0:
                log.info(f"Preparing {i} rows for {table}...")

        schema, table_name = self._parse_table(table)

        engine = session.get_bind()
        metadata = MetaData()
        metadata.reflect(engine, schema=schema)
        table_ref = Table(table_name, metadata, autoload=True, autoload_with=engine, schema=schema)

        engine.execute(table_ref.insert(), prepared_rows)
        log.info(f"Inserted {len(prepared_rows)} rows into {table}")

    def _query_database(self, session: Session, query: str, chunk_size: int = 1000) -> Generator[List[Any], None, None]:
        cursor: CursorResult = session.execute(text(query))

        columns: List[str] = list(cursor.keys())
        yield columns

        row: List[Any]
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
