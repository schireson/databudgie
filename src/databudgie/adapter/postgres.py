import io
from typing import List

import psycopg2.errors
from setuplog import log
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from databudgie.adapter.base import Adapter


# XXX: see if we can count rows in the transactions
class PostgresAdapter(Adapter):
    def export_query(self, session: Session, query: str, dest: io.StringIO):
        engine: Engine = session.get_bind()
        conn: Connection = engine.raw_connection()
        cursor: psycopg2.cursor = conn.cursor()

        log.debug("Exporting PostgreSQL query to buffer...")
        copy = "COPY ({}) TO STDOUT CSV HEADER".format(query)
        cursor.copy_expert(copy, dest)
        cursor.close()
        conn.close()

    def import_csv(self, session: Session, csv_file: io.StringIO, table: str):
        engine: Engine = session.get_bind()
        conn: Connection = engine.raw_connection()
        cursor: psycopg2.cursor = conn.cursor()

        # Reading the header line from the buffer removes it for the ingest
        columns: List[str] = csv_file.readline().strip().split(",")

        log.debug(f"Copying buffer to {table}...")
        copy = "COPY {table} ({columns}) FROM STDIN CSV".format(table=table, columns=",".join(columns))
        cursor.copy_expert(copy, csv_file)
        cursor.close()
        conn.commit()
        conn.close()
