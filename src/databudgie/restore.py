import json
from typing import Optional, Sequence, TYPE_CHECKING, Union

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from databudgie.adapter import Adapter
from databudgie.config import RestoreConfig
from databudgie.output import Console, default_console, Progress
from databudgie.storage import FileTypes, StorageBackend
from databudgie.table_op import expand_table_ops, SchemaOp, TableOp
from databudgie.utils import capture_failures, wrap_buffer

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

    ddl_path = restore_config.ddl.full_path()
    strategy = restore_config.ddl.strategy

    with storage.get_file_content(ddl_path, strategy, file_type=FileTypes.manifest) as file_object:
        if not file_object.content:
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
        if not schema_op or schema_op.name in schema_names:
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

            restore_ddl(adapter, schema_op, storage=storage, console=console)

        for table_op in table_ops:
            progress.update(task, description=f"Restoring DDL: {table_op.full_name}")

            restore_ddl(adapter, table_op, storage=storage, console=console)

    console.info("Finished Restoring DDL")


def restore_ddl(
    adapter: Adapter,
    op: Union[TableOp, SchemaOp],
    storage: StorageBackend,
    console: Console = default_console,
):
    strategy: str = op.raw_conf.strategy

    path = op.full_path("ddl")
    with storage.get_file_content(path, strategy, file_type=FileTypes.ddl, name=op.full_name) as file_object:
        if not file_object.content:
            console.warn(f"Found no DDL backups under {path} to restore")
            return

        query = file_object.content.read()

    adapter.execute_sql(query, commit=True)


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

            strategy: str = table_op.raw_conf.strategy

            path = table_op.full_path("sequences")
            with storage.get_file_content(
                path, strategy, file_type=FileTypes.sequences, name=table_op.full_name
            ) as file_object:
                if not file_object.content:
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
            if not data or not truncate or table_op.full_name is None:
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
            if not table_op.full_name or not table_op.raw_conf.data:
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
    assert table_op.full_name

    strategy: str = table_op.raw_conf.strategy
    compression = table_op.raw_conf.compression

    with storage.get_file_content(
        table_op.full_path(),
        strategy,
        file_type=FileTypes.data,
        name=table_op.full_name,
        compression=compression,
    ) as file_object:
        if not file_object.content:
            console.warn(f"Found no backups for {table_op.pretty_name} to restore")
            return

        with wrap_buffer(file_object.content) as wrapper:
            try:
                adapter.import_csv(wrapper, table_op.full_name)
            except SQLAlchemyError:
                session.rollback()
            else:
                session.commit()

    console.trace(f"Restored {table_op.pretty_name} from {file_object.path}")
