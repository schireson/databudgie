from sqlalchemy import Column, ForeignKey, MetaData, types, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from databudgie.manifest import create_manifest_table

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class Store(Base):  # type: ignore
    __tablename__ = "store"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    name = Column(types.Unicode(255), nullable=False, unique=True)


class Product(Base):  # type: ignore
    __tablename__ = "product"
    __table_args__ = (UniqueConstraint("store_id", "external_id"),)

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    store_id = Column(
        types.Integer(),
        ForeignKey("store.id"),
        nullable=False,
        index=True,
    )
    external_id = Column(types.Unicode(255), nullable=False)
    external_name = Column(types.Unicode(255), nullable=False)
    external_status = Column(types.Unicode(255), nullable=True)
    active = Column(types.Boolean(), default=True, nullable=False, server_default="true")

    store = relationship("Store", uselist=False)


class Customer(Base):  # type: ignore
    __tablename__ = "customer"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)

    external_id = Column(types.Unicode(255), nullable=False)
    store_id = Column(types.Integer(), ForeignKey("store.id", ondelete="CASCADE"))
    product_id = Column(types.Integer(), ForeignKey("product.id", ondelete="CASCADE"))
    external_name = Column(types.Unicode(255), nullable=False)
    type = Column(types.Unicode(255), nullable=False)
    active = Column(types.Boolean(), default=True, nullable=False, server_default="true")
    external_status = Column(types.Unicode(255), nullable=True)

    store = relationship("Store", uselist=False)
    product = relationship("Product", uselist=False)


class Sale(Base):  # type: ignore
    """Contains a variety of fields for testing type conversion."""

    __tablename__ = "sales"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    external_id = Column(types.Unicode(255), nullable=False)
    store_id = Column(types.Integer(), nullable=False)
    product_id = Column(types.Integer(), nullable=False)
    sale_value = Column(types.Float(), nullable=False)
    sale_date = Column(types.Date(), nullable=False)
    active = Column(types.Boolean(), default=True, nullable=False, server_default="true")


class DatabudgieManifest(Base):  # type: ignore
    __table__ = create_manifest_table(Base.metadata, tablename="databudgie_manifest")
