import abc

import sqlalchemy
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import Session

from databudgie.utils import capture_failures, parse_table


class Manifest(metaclass=abc.ABCMeta):
    def __init__(self, session: Session, table_name: str):
        self.table_name = table_name
        self.session = session

        schema, table = parse_table(table_name)
        self.metadata = MetaData(bind=session.get_bind())
        self.metadata.reflect(schema=schema, only=[table])
        self.manifest_table = Table(table, self.metadata, autoload=True, schema=schema)

        self.transaction_id = self._get_max_transaction()

    def __contains__(self, object) -> bool:
        """Return true if the table_name has an entry for the corresponding transaction id."""
        return bool(
            self.session.query(self.manifest_table)
            .filter(self.manifest_table.c.transaction == self.transaction_id, self.manifest_table.c.table == object)
            .first()
        )

    def _get_max_transaction(self) -> int:
        last_transaction = self.session.query(sqlalchemy.func.max(self.manifest_table.c.transaction)).scalar()

        if not last_transaction:
            return 1
        return last_transaction + 1

    def set_transaction_id(self, id: int):
        self.transaction_id = id

    @capture_failures()
    def record(self, table_name: str, location: str):
        self.session.execute(
            self.manifest_table.insert(),
            [{"transaction": self.transaction_id, "table": table_name, "file_path": location}],
        )
        self.session.commit()


class BackupManifest(Manifest):
    pass


class RestoreManifest(Manifest):
    pass
