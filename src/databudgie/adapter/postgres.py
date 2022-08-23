import io
import os
import shlex
import shutil
import subprocess  # nosec
from typing import Dict, List

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

    def import_csv(self, session: Session, csv_file: io.TextIOBase, table: str):
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

        # from https://stackoverflow.com/questions/51279588/sort-tables-in-order-of-dependency-postgres
        collect_tables = text(
            """
            with recursive fk_tree as (
                -- All tables not referencing anything else
                select t.oid      as reloid,
                    t.relname  as table_name,
                    s.nspname  as schema_name,
                    null::text as referenced_table_name,
                    null::text as referenced_schema_name,
                    1          as level
                from pg_class t
                join pg_namespace s on s.oid = t.relnamespace
                where relkind = 'r'
                and not exists(select * from pg_constraint where contype = 'f' and conrelid = t.oid)
                and s.nspname not in ('pg_catalog', 'information_schema')

                union all

                select ref.oid,
                    ref.relname,
                    rs.nspname,
                    p.table_name,
                    p.schema_name,
                    p.level + 1
                from pg_class ref
                join pg_namespace rs on rs.oid = ref.relnamespace
                join pg_constraint c on c.contype = 'f' and c.conrelid = ref.oid
                join fk_tree p on p.reloid = c.confrelid
                where ref.oid != p.reloid -- do not enter to tables referencing theirselves.
            ),
            all_tables as (
                -- this picks the highest level for each table
                select
                    schema_name,
                    table_name,
                    level,
                    row_number() over (partition by schema_name, table_name order by level desc) as last_table_row
                from fk_tree
            )
            select schema_name || '.' || table_name, level
            from all_tables at
            where last_table_row = 1
            order by level;
            """
        )
        results = session.execute(collect_tables)
        table_full_names = [row[0] for row in results]
        return table_full_names

    @staticmethod
    def collect_table_sequences(session: Session) -> Dict[str, List[str]]:
        sequences = session.execute(
            text(
                """
                SELECT
                    CASE
                        WHEN seq_ns.nspname = 'public' THEN seq.relname
                        ELSE concat(seq_ns.nspname, '.', seq.relname)
                    END AS fq_sequence_name,
                    concat(tab_ns.nspname, '.', tab.relname) AS fq_table_name
                FROM pg_class seq
                JOIN pg_namespace seq_ns ON seq.relnamespace = seq_ns.oid
                JOIN pg_depend d ON d.objid = seq.oid AND d.deptype = 'a'
                JOIN pg_class tab ON d.objid = seq.oid AND d.refobjid = tab.oid
                JOIN pg_namespace tab_ns on tab.relnamespace = tab_ns.oid
                WHERE seq.relkind = 'S'
                """
            )
        )
        result: Dict = {}
        for sequence in sequences:
            result.setdefault(sequence.fq_table_name, []).append(sequence.fq_sequence_name)
        return result

    @staticmethod
    def collect_sequence_value(session: Session, sequence_name: str) -> int:
        return session.execute(text(f"SELECT last_value from {sequence_name}")).scalar()  # nosec

    @staticmethod
    def restore_sequence_value(session: Session, sequence_name: str, value: int) -> int:
        return session.execute(text(f"SELECT setval('{sequence_name}', {value})")).scalar()


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
