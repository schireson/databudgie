from datetime import datetime

import sqlalchemy
from sqlalchemy.orm import Mapped


class BackupManifestMixin:
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    transaction: Mapped[int] = sqlalchemy.Column(sqlalchemy.types.Integer, nullable=False, index=True)
    table: Mapped[str] = sqlalchemy.Column(sqlalchemy.types.String, nullable=False, index=True)
    file_path: Mapped[str] = sqlalchemy.Column(sqlalchemy.types.String, nullable=False)
    exported_at: Mapped[datetime] = sqlalchemy.Column(
        sqlalchemy.types.DateTime(timezone=True),
        default=datetime.utcnow,
        server_default=sqlalchemy.text("CURRENT_TIMESTAMP"),
        nullable=False,
    )


class RestoreManifestMixin:
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    transaction: Mapped[int] = sqlalchemy.Column(sqlalchemy.types.Integer, nullable=False, index=True)
    table: Mapped[str] = sqlalchemy.Column(sqlalchemy.types.String, nullable=False, index=True)
    file_path: Mapped[str] = sqlalchemy.Column(sqlalchemy.types.String, nullable=False)
    inserted_at: Mapped[datetime] = sqlalchemy.Column(
        sqlalchemy.types.DateTime(timezone=True),
        default=datetime.utcnow,
        server_default=sqlalchemy.text("CURRENT_TIMESTAMP"),
        nullable=False,
    )
