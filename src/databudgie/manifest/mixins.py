from datetime import datetime

import sqlalchemy
from sqlalchemy.orm import Mapped


class DatabudgieManifestMixin:
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    transaction: Mapped[int] = sqlalchemy.Column(sqlalchemy.types.Integer, nullable=False, index=True)
    action = sqlalchemy.Column(sqlalchemy.types.String, nullable=False, index=True)  # backup or restore
    table: Mapped[str] = sqlalchemy.Column(sqlalchemy.types.String, nullable=False, index=True)
    file_path: Mapped[str] = sqlalchemy.Column(sqlalchemy.types.String, nullable=False)
    timestamp: Mapped[datetime] = sqlalchemy.Column(
        sqlalchemy.types.DateTime(timezone=True),
        default=datetime.utcnow,
        server_default=sqlalchemy.text("CURRENT_TIMESTAMP"),
        nullable=False,
    )
