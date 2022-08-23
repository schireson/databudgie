import csv
import io
from typing import Any, Dict, Generator, List

from setuplog import log
from sqlalchemy import MetaData, Table, text
from sqlalchemy.orm import Session

from databudgie.adapter.base import Adapter
from databudgie.utils import parse_table


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

    def import_csv(self, session: Session, csv_file: io.TextIOBase, table: str):
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

        schema, table_name = parse_table(table)

        engine = session.get_bind()
        metadata = MetaData()
        metadata.reflect(engine, schema=schema)
        table_ref = Table(table_name, metadata, autoload=True, autoload_with=engine, schema=schema)

        engine.execute(table_ref.insert(), prepared_rows)
        log.info(f"Inserted {len(prepared_rows)} rows into {table}")

    def _query_database(self, session: Session, query: str, chunk_size: int = 1000) -> Generator[List[Any], None, None]:
        cursor = session.execute(text(query))

        columns: List[str] = list(cursor.keys())
        yield columns

        row: List[Any]
        for row in cursor.yield_per(chunk_size):
            yield row

    @staticmethod
    def export_schema_ddl(session: Session, name: str) -> bytes:
        raise NotImplementedError()

    @staticmethod
    def export_table_ddl(session: Session, table_name: str):
        raise NotImplementedError()

    @staticmethod
    def reset_database(session):
        """Reset the database in a database-backend agnostic way.

        Create a temp database, connect to it, drop the target database, and drop
        the temp database.

        This method suffers from being rather prone to failure, but is better
        than nothing!
        """
        raise NotImplementedError()
