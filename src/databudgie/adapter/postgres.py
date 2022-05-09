import io
import os
import shlex
import shutil
import subprocess  # nosec
from typing import List

import psycopg2.errors
from setuplog import log, log_duration
from sqlalchemy import text
from sqlalchemy.engine import Connection, create_engine, Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

from databudgie.adapter.base import Adapter
from databudgie.adapter.fallback import PythonAdapter


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
        columns: List[str] = [f'"{c}"' for c in csv_file.readline().strip().split(",")]

        log.debug(f"Copying buffer to {table}...")
        copy = "COPY {table} ({columns}) FROM STDIN CSV".format(table=table, columns=",".join(columns))
        cursor.copy_expert(copy, csv_file)
        cursor.close()
        conn.commit()
        conn.close()

    @staticmethod
    def export_schema_ddl(session: Session, name: str) -> bytes:
        if not shutil.which("pg_dump"):
            log.warning("Could not find pg_dump, falling back to SQLAlchemy implementation.")
            return PythonAdapter.export_schema_ddl(session, name)

        url = session.connection().engine.url
        result = pg_dump(url, f"--schema-only --schema={name} --exclude-table={name}.*")
        result = result.replace(
            f"CREATE SCHEMA {name};".encode("utf-8"), f"CREATE SCHEMA IF NOT EXISTS {name};".encode("utf-8")
        )
        return result

    @staticmethod
    def export_table_ddl(session: Session, table_name: str):
        if not shutil.which("pg_dump"):
            log.warning("Could not find pg_dump, falling back to SQLAlchemy implementation.")
            return PythonAdapter.export_table_ddl(session, table_name)

        url = session.connection().engine.url

        return pg_dump(url, f"--schema-only -t {table_name}")

    @staticmethod
    def reset_database(session: Session):
        """Attempt to kill the existing database and bring it back up."""
        connection = session.connection()
        url: URL = connection.engine.url
        database = url.database

        # "template1" is a *special* internal postgres database that we can be guaranteed
        # to connect to after having dropped (potentially) all other available databases.
        # "template0", used below is not allowed to be connected to.
        template_url = url.set(database="template1")

        template_engine = create_engine(template_url)
        with template_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            kill_pids = text("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = :database;")
            connection.execute(kill_pids, database=database)

            connection.execute(text(f"DROP DATABASE {database}"))

            # "template0" is an even more special database. You cannot create databases
            # from a template while there are active connections to it.
            connection.execute(text(f"CREATE DATABASE {database} template=template0"))

        session.invalidate()

    @staticmethod
    @log_duration("Collecting existing tables")
    def collect_existing_tables(session: Session) -> List[str]:
        """Find the set of all user-defined tables in a database."""

        if "ENABLE_EXPERIMENTAL_TABLE_COLLECTION" not in os.environ:
            log.info("Set ENABLE_EXPERIMENTAL_TABLE_COLLECTION to use faster experimental table collection.")
            return Adapter.collect_existing_tables(session)

        collect_tables = text(
            """
            WITH fkeys AS (
            SELECT

            c.conrelid AS table_id,
            c_fromtablens.nspname AS schemaname,
            c_fromtable.relname AS tablename,

            c.confrelid AS parent_id,
            c_totablens.nspname AS parent_schemaname,
            c_totable.relname AS parent_tablename

            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace

            JOIN pg_class c_fromtable ON c_fromtable.oid = c.conrelid
            JOIN pg_namespace c_fromtablens ON c_fromtablens.oid = c_fromtable.relnamespace

            JOIN pg_class c_totable ON c_totable.oid = c.confrelid
            JOIN pg_namespace c_totablens ON c_totablens.oid = c_totable.relnamespace
            WHERE
            c.contype = 'f'
            )

            SELECT
            t.schemaname || '.' ||  t.tablename as tablefullname

            FROM pg_tables t
            LEFT JOIN fkeys ON  t.schemaname = fkeys.schemaname AND
                                t.tablename =  fkeys.tablename
            WHERE
            t.schemaname NOT IN ('pg_catalog', 'information_schema');
            """
        )
        results = session.execute(collect_tables)
        return [row[0] for row in results]


def pg_dump(url: URL, rest: str = "", no_comments=True) -> bytes:
    parts = [f"pg_dump -h {url.host} -p {url.port} -U {url.username} -d {url.database} --no-password"]

    if no_comments:
        parts.append("--no-comments")

    parts.append(rest)

    raw_command = " ".join(parts)

    command = shlex.split(raw_command)

    try:
        result = subprocess.run(  # nosec
            command, capture_output=True, env={**os.environ, "PGPASSWORD": url.password}, check=True
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(e.stderr)

    return result.stdout
