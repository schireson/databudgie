from datetime import datetime

import sqlalchemy


def create_manifest_table(metadata, tablename, schema=None):
    return sqlalchemy.Table(
        tablename,
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True),
        sqlalchemy.Column("transaction", sqlalchemy.types.Integer, nullable=False, index=True),
        sqlalchemy.Column("action", sqlalchemy.types.String, nullable=False, index=True),  # backup or restore
        sqlalchemy.Column("table", sqlalchemy.types.String, nullable=False, index=True),
        sqlalchemy.Column("file_path", sqlalchemy.types.String, nullable=False),
        sqlalchemy.Column(
            "timestamp",
            sqlalchemy.types.DateTime(timezone=True),
            default=datetime.utcnow,
            server_default=sqlalchemy.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        schema=schema,
    )


class DatabudgieManifestMixin:
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    transaction = sqlalchemy.Column(sqlalchemy.types.Integer, nullable=False, index=True)
    action = sqlalchemy.Column(sqlalchemy.types.String, nullable=False, index=True)  # backup or restore
    table = sqlalchemy.Column(sqlalchemy.types.String, nullable=False, index=True)
    file_path = sqlalchemy.Column(sqlalchemy.types.String, nullable=False)
    timestamp = sqlalchemy.Column(
        sqlalchemy.types.DateTime(timezone=True),
        default=datetime.utcnow,
        server_default=sqlalchemy.text("CURRENT_TIMESTAMP"),
        nullable=False,
    )
