import abc
import functools
from typing import Optional

import sqlalchemy
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import Session

from databudgie.utils import parse_table


class Manifest(metaclass=abc.ABCMeta):
    def __init__(self, session: Session, table_name: str, action: str):
        self.table_name = table_name
        self.session = session
        self.action = action

        self._transaction_id: Optional[int] = None

    @functools.lru_cache()
    def manifest_table(self):
        schema, table = parse_table(self.table_name)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.session.get_bind(), schema=schema, only=[table])
        return Table(table, self.metadata, autoload=True, schema=schema)

    @functools.lru_cache()
    def transaction_id(self):
        if self._transaction_id is None:
            table = self.manifest_table()
            last_transaction = self.session.query(sqlalchemy.func.max(table.c.transaction)).scalar()

            if not last_transaction:
                last_transaction = 0

            return last_transaction + 1

        return self._transaction_id

    def __contains__(self, object) -> bool:
        """Return true if the table_name has an entry for the corresponding transaction id."""
        manifest_table = self.manifest_table()
        return bool(
            self.session.query(manifest_table)
            .filter(manifest_table.c.transaction == self.transaction_id(), manifest_table.c.table == object)
            .first()
        )

    def set_transaction_id(self, id: int):
        self._transaction_id = id

    def record(self, table_name: str, location: str):
        self.session.execute(
            self.manifest_table().insert(),
            [
                {
                    "transaction": self.transaction_id(),
                    "action": self.action,
                    "table": table_name,
                    "file_path": location,
                }
            ],
        )
        self.session.commit()


class BackupManifest(Manifest):
    def __init__(self, session: Session, table_name: str):
        super().__init__(session, table_name, "backup")


class RestoreManifest(Manifest):
    def __init__(self, session: Session, table_name: str):
        super().__init__(session, table_name, "restore")
