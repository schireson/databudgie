import io
import os
import shlex
import shutil
import subprocess  # nosec
from typing import Dict, List

import psycopg2.errors
from sqlalchemy import text
from sqlalchemy.engine import Connection, create_engine, Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

from databudgie.adapter.base import Adapter
from databudgie.adapter.fallback import PythonAdapter
from databudgie.etl.base import TableOp
from databudgie.output import Console, default_console


def update_url(url, database=None):
    try:
        return url.set(database=database)
    except AttributeError:
        return URL(
            drivername=url.drivername,
            username=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
            database=database or url.database,
        )


# XXX: see if we can count rows in the transactions
class PostgresAdapter(Adapter):
    def export_query(self, session: Session, query: str, dest: io.StringIO):
        engine: Engine = session.get_bind()
        conn: Connection = engine.raw_connection()
        cursor: psycopg2.cursor = conn.cursor()

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

        copy = "COPY {table} ({columns}) FROM STDIN CSV".format(table=table, columns=",".join(columns))
        cursor.copy_expert(copy, csv_file)
        cursor.close()
        conn.commit()
        conn.close()

    @staticmethod
    def export_schema_ddl(session: Session, name: str, console: Console = default_console) -> bytes:
        if not shutil.which("pg_dump"):
            console.warn("Could not find pg_dump, falling back to SQLAlchemy implementation.")
            return PythonAdapter.export_schema_ddl(session, name)

        url = session.connection().engine.url
        result = pg_dump(url, f"--schema-only --schema={name} --exclude-table={name}.*")
        result = result.replace(
            f"CREATE SCHEMA {name};".encode("utf-8"), f"CREATE SCHEMA IF NOT EXISTS {name};".encode("utf-8")
        )
        return result

    @staticmethod
    def export_table_ddl(session: Session, table_name: str, console: Console = default_console):
        if not shutil.which("pg_dump"):
            console.warn("Could not find pg_dump, falling back to SQLAlchemy implementation.")
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
        template_url = update_url(url, database="template1")

        template_engine = create_engine(template_url)
        with template_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            kill_pids = text("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = :database;")
            connection.execute(kill_pids, database=database)

            connection.execute(text(f"DROP DATABASE {database}"))

            # "template0" is an even more special database. You cannot create databases
            # from a template while there are active connections to it.
            connection.execute(text(f"CREATE DATABASE {database} template=template0"))

        session.invalidate()

    @classmethod
    def collect_existing_tables(cls, session: Session, console: Console = default_console) -> List[str]:
        """Find the set of all user-defined tables in a database."""
        if "FALLBACK_SQLALCHEMY_TABLE_COLLECTION" in os.environ:
            console.warn("Using SQLAlchemy to collect tables.")
            return Adapter.collect_existing_tables(session)

        collect_tables = text(
            """
            with recursive fk_tree as (
                -- All tables not referencing anything else
                select t.oid      as reloid,
                    t.relname  as table_name,
                    s.nspname  as schema_name,
                    null::text COLLATE "C" as referenced_table_name,
                    null::text COLLATE "C" as referenced_schema_name,
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
        return [row[0] for row in results]

    @staticmethod
    def collect_table_dependencies(
        session: Session, table_op: TableOp, console: Console = default_console
    ) -> List[str]:
        """Find the set of tables dependent on the set of input tables."""
        collect_tables = text(
            """
            with recursive fk_tree as (
                 select pg_class.oid      as table_oid,
                     pg_namespace.nspname as schema_name,
                     pg_class.relname     as table_name,
                     1                    as level
                 from pg_class
                 join pg_namespace pg_namespace on pg_namespace.oid = pg_class.relnamespace
                 where pg_namespace.nspname = :schema and pg_class.relname = :table_name
                 union all
                 select pg_class.oid      as table_oid,
                     pg_namespace.nspname as schema_name,
                     pg_class.relname     as table_name,
                     fk_tree.level + 1          as level
                 from fk_tree
                 join pg_constraint on pg_constraint.contype = 'f' and pg_constraint.conrelid = fk_tree.table_oid
                 join pg_class on pg_class.oid = pg_constraint.confrelid
                 join pg_namespace on pg_namespace.oid = pg_class.relnamespace
                 where fk_tree.table_oid != pg_class.oid -- do not enter to tables referencing theirselves.
            ),
            all_tables as (
                -- this picks the **highest** level for each table
                select
                    schema_name,
                    table_name,
                    level,
                    row_number() over (partition by schema_name, table_name order by level desc) as last_table_row
                from fk_tree
            )
            select schema_name || '.' || table_name
            from all_tables at
            where last_table_row = 1
            order by level;
            """
        )

        results = session.execute(collect_tables, params=dict(schema=table_op.schema, table_name=table_op.table_name))

        return [row[0] for row in results]

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
