from sqlalchemy import Column, ForeignKey, MetaData, types, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class Advertiser(Base):  # type: ignore
    __tablename__ = "advertiser"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    name = Column(types.Unicode(255), nullable=False, unique=True)


class LineItem(Base):  # type: ignore
    __tablename__ = "line_item"
    __table_args__ = (UniqueConstraint("advertiser_id", "external_id"),)

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    advertiser_id = Column(types.Integer(), ForeignKey("advertiser.id"), nullable=False, index=True,)
    external_id = Column(types.Unicode(255), nullable=False)
    external_name = Column(types.Unicode(255), nullable=False)
    external_status = Column(types.Unicode(255), nullable=True)
    active = Column(types.Boolean(), default=True, nullable=False, server_default="true")


class GenericAd(Base):  # type: ignore
    __tablename__ = "ad_generic"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)

    external_id = Column(types.Unicode(255), nullable=False)
    advertiser_id = Column(types.Integer(), ForeignKey("advertiser.id", ondelete="CASCADE"))
    line_item_id = Column(types.Integer(), ForeignKey("line_item.id", ondelete="CASCADE"))
    external_name = Column(types.Unicode(255), nullable=False)
    primary_text = Column(types.Unicode(255), nullable=True)
    type = Column(types.Unicode(255), nullable=False)
    active = Column(types.Boolean(), default=True, nullable=False, server_default="true")
    external_status = Column(types.Unicode(255), nullable=True)
