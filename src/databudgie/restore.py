import json
from typing import Optional, Sequence, TYPE_CHECKING, Union

import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.config import RestoreConfig
from databudgie.output import Console, default_console, Progress
from databudgie.storage import FileTypes, StorageBackend
from databudgie.table_op import expand_table_ops, SchemaOp, TableOp
from databudgie.utils import capture_failures, join_paths, parse_table, wrap_buffer

if TYPE_CHECKING:
    pass


def restore_all(
    session: Session,
    restore_config: RestoreConfig,
    storage: Optional[StorageBackend] = None,
    console: Console = default_console,
) -> None:
    """Perform restore on all tables in the config."""
    if storage is None:
        storage = StorageBackend.from_config(restore_config)

    adapter = Adapter.get_adapter(session, restore_config.adapter)

    if restore_config.ddl.clean:
        console.warn("Cleaning database")
        adapter.reset_database()

    restore_all_ddl(
        session,
        restore_config,
        storage=storage,
        adapter=adapter,
        console=console,
    )

    console.trace("Collecting existing tables")
    existing_tables = adapter.collect_existing_tables()

    table_ops = expand_table_ops(
        session,
        restore_config.tables,
        existing_tables,
        storage=storage,
        console=console,
        warn_for_unused_tables=True,
    )

    table_ops = adapter.materialize_table_dependencies(
        table_ops,
        console=console,
        reverse=True,
    )

    restore_sequences(
        session,
        table_ops,
        storage=storage,
        adapter=adapter,
        console=console,
    )
    truncate_tables(
        list(reversed(table_ops)),
        adapter=adapter,
        console=console,
    )
    restore_tables(
        session,
        table_ops,
        storage=storage,
        adapter=adapter,
        console=console,
    )


def restore_all_ddl(
    session: Session,
    restore_config: RestoreConfig,
    *,
    storage: StorageBackend,
    adapter: Adapter,
    console: Console = default_console,
):
    if not restore_config.ddl.enabled:
        return

    ddl_path = restore_config.ddl.location
    strategy = restore_config.ddl.strategy

    with storage.get_file_content(ddl_path, strategy, file_type=FileTypes.manifest) as file_object:
        if not file_object:
            console.info("Found no DDL manifest to restore")
            return

        tables = json.load(file_object.content)

    table_ops = expand_table_ops(
        session, restore_config.tables, storage=storage, existing_tables=tables, console=console
    )
    table_ops = adapter.materialize_table_dependencies(
        table_ops,
        console=console,
        reverse=True,
    )

    schema_names = set()
    schema_ops = []

    for table_op in table_ops:
        schema_op = table_op.schema_op()
        if schema_op.name in schema_names:
            continue

        if not table_op.raw_conf.ddl:
            continue

        schema_names.add(schema_op.name)
        schema_ops.append(schema_op)

    with Progress(console) as progress:
        total = len(schema_ops) + len(table_ops)
        task = progress.add_task("Restoring DDL", total=total)

        for schema_op in schema_ops:
            progress.update(task, description=f"Restoring schema DDL: {schema_op.name}")

            restore_ddl(session, schema_op, ddl_path, storage=storage, console=console)

        for table_op in table_ops:
            progress.update(task, description=f"Restoring DDL: {table_op.full_name}")

            restore_ddl(session, table_op, ddl_path, storage=storage, console=console)

    console.info("Finished Restoring DDL")


def restore_ddl(
    session: Session,
    op: Union[TableOp, SchemaOp],
    ddl_path: str,
    storage: StorageBackend,
    console: Console = default_console,
):
    location = op.location()
    strategy: str = op.raw_conf.strategy

    path = join_paths(ddl_path, location)
    with storage.get_file_content(path, strategy, file_type=FileTypes.ddl) as file_object:
        if not file_object:
            console.warn(f"Found no DDL backups under {path} to restore")
            return

        query = file_object.content.read().decode("utf-8")

    query = "\n".join(
        line
        for line in query.splitlines()
        # XXX: These should be being omitted at the backup stage, it's not the restore process' responsibility!
        if not line.startswith("--")
        and not line.startswith("SET")
        and not line.startswith("SELECT pg_catalog")
        and line
    )
    session.execute(sqlalchemy.text(query))
    session.commit()


def restore_sequences(
    session: Session,
    table_ops: Sequence[TableOp],
    adapter: Adapter,
    storage: StorageBackend,
    console: Console = default_console,
):
    with Progress(console) as progress:
        task = progress.add_task("Restoring sequence positions", total=len(table_ops))

        for table_op in table_ops:
            progress.update(task, description=f"Restoring sequence position: {table_op.full_name}")
            if not table_op.raw_conf.sequences:
                continue

            location = table_op.location()
            strategy: str = table_op.raw_conf.strategy

            path = join_paths(location, "sequences")
            with storage.get_file_content(path, strategy, file_type=FileTypes.sequences) as file_object:
                if not file_object:
                    continue

                sequences = json.load(file_object.content)

            for sequence, value in sequences.items():
                adapter.restore_sequence_value(sequence, value)

    console.info("Finished restoring sequence positions")
    session.commit()


def truncate_tables(table_ops: Sequence[TableOp], adapter: Adapter, console: Console):
    with Progress(console) as progress:
        task = progress.add_task("Truncating Tables", total=len(table_ops))

        for table_op in table_ops:
            data = table_op.raw_conf.data
            truncate = table_op.raw_conf.truncate
            if not data or not truncate:
                continue

            progress.update(task, description=f"[trace]Truncating {table_op.full_name}[/trace]", advance=1)
            adapter.truncate_table(table_op.full_name)

    console.info("Finished truncating tables")


def restore_tables(
    session: Session,
    table_ops: Sequence[TableOp],
    *,
    adapter: Adapter,
    storage: StorageBackend,
    console: Console = default_console,
) -> None:
    with Progress(console) as progress:
        task = progress.add_task("Restoring tables", total=len(table_ops))

        for table_op in table_ops:
            if not table_op.raw_conf.data:
                continue

            progress.update(task, description=f"Restoring table: {table_op.full_name}")

            with capture_failures(strict=table_op.raw_conf.strict):
                restore(
                    session,
                    table_op=table_op,
                    adapter=adapter,
                    storage=storage,
                    console=console,
                )

    console.info("Finished restoring tables")


def restore(
    session: Session,
    *,
    adapter: Adapter,
    storage: StorageBackend,
    table_op: TableOp,
    console: Console = default_console,
) -> None:
    """Restore a CSV file from S3 to the database."""
    # Force table_name to be fully qualified
    schema, table = parse_table(table_op.full_name)
    table_name = f"{schema}.{table}"

    strategy: str = table_op.raw_conf.strategy
    compression = table_op.raw_conf.compression

    with storage.get_file_content(
        table_op.location(),
        strategy,
        file_type=FileTypes.data,
        name=table_op.full_name,
        compression=compression,
    ) as file_object:
        if not file_object:
            console.warn(f"Found no backups for {table_name} to restore")
            return

        with wrap_buffer(file_object.content) as wrapper:
            try:
                adapter.import_csv(wrapper, table_op.full_name)
            except SQLAlchemyError:
                session.rollback()
            else:
                session.commit()

    console.trace(f"Restored {table_name} from {file_object.path}")
