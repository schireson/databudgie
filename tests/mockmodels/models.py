from sqlalchemy import Column, ForeignKey, MetaData, types, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from databudgie.manifest import DatabudgieManifestMixin

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class Advertiser(Base):  # type: ignore
    __tablename__ = "advertiser"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    name = Column(types.Unicode(255), nullable=False, unique=True)


class Product(Base):  # type: ignore
    __tablename__ = "product"
    __table_args__ = (UniqueConstraint("advertiser_id", "external_id"),)

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    advertiser_id = Column(types.Integer(), ForeignKey("advertiser.id"), nullable=False, index=True,)
    external_id = Column(types.Unicode(255), nullable=False)
    external_name = Column(types.Unicode(255), nullable=False)
    external_status = Column(types.Unicode(255), nullable=True)
    active = Column(types.Boolean(), default=True, nullable=False, server_default="true")

    advertiser = relationship("Advertiser", uselist=False)


class GenericAd(Base):  # type: ignore
    __tablename__ = "ad_generic"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)

    external_id = Column(types.Unicode(255), nullable=False)
    advertiser_id = Column(types.Integer(), ForeignKey("advertiser.id", ondelete="CASCADE"))
    product_id = Column(types.Integer(), ForeignKey("product.id", ondelete="CASCADE"))
    external_name = Column(types.Unicode(255), nullable=False)
    primary_text = Column(types.Unicode(255), nullable=True)
    type = Column(types.Unicode(255), nullable=False)
    active = Column(types.Boolean(), default=True, nullable=False, server_default="true")
    external_status = Column(types.Unicode(255), nullable=True)

    advertiser = relationship("Advertiser", uselist=False)
    product = relationship("Product", uselist=False)


class Sale(Base):  # type: ignore
    """Contains a variety of fields for testing type conversion."""

    __tablename__ = "sales"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    external_id = Column(types.Unicode(255), nullable=False)
    advertiser_id = Column(types.Integer(), nullable=False)
    product_id = Column(types.Integer(), nullable=False)
    sale_value = Column(types.Float(), nullable=False)
    sale_date = Column(types.Date(), nullable=False)
    active = Column(types.Boolean(), default=True, nullable=False, server_default="true")


class DatabudgieManifest(Base, DatabudgieManifestMixin):  # type: ignore
    __tablename__ = "databudgie_manifest"