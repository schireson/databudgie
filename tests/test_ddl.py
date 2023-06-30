import pytest
import sqlalchemy.exc
from pytest_mock_resources import create_postgres_fixture
from sqlalchemy import Column, ForeignKey, MetaData, types
from sqlalchemy.ext.declarative import declarative_base

from databudgie.backup import backup_all
from databudgie.config import RootConfig
from databudgie.restore import restore_all
from tests.utils import s3_config

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class Store(Base):  # type: ignore
    __tablename__ = "store"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    name = Column(types.Unicode(255), nullable=False, unique=True)


class Product(Base):  # type: ignore
    __tablename__ = "product"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    store_id = Column(
        types.Integer(),
        ForeignKey("store.id"),
        nullable=False,
        index=True,
    )


class Customer(Base):  # type: ignore
    __tablename__ = "customer"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)

    external_id = Column(types.Unicode(255), nullable=False)
    store_id = Column(types.Integer(), ForeignKey("store.id", ondelete="CASCADE"))
    product_id = Column(types.Integer(), ForeignKey("product.id", ondelete="CASCADE"))
    external_name = Column(types.Unicode(255), nullable=False)


class Sale(Base):  # type: ignore
    __tablename__ = "sales"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    external_id = Column(types.Unicode(255), nullable=False)
    store_id = Column(types.Integer(), nullable=False)


pg = create_postgres_fixture(Base, session=True)
empty_db = create_postgres_fixture(session=True)


def test_backup_ddl_disabled(pg, s3_resource):
    config = RootConfig.from_dict(
        {
            "location": "s3://sample-bucket/{table}",
            "strict": True,
            "ddl": {"enabled": False},
            "data": False,
            "sequences": False,
            "tables": ["public.*"],
            **s3_config,
        }
    )

    backup_all(pg, config.backup)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == []


def test_backup_ddl(pg, s3_resource):
    config = RootConfig.from_dict(
        {
            "location": "s3://sample-bucket/{table}",
            "ddl": {"enabled": True, "location": "s3://sample-bucket/ddl"},
            "tables": ["public.*"],
            "sequences": False,
            "strict": True,
            **s3_config,
        }
    )

    backup_all(pg, config.backup)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "ddl/2021-04-26T09:00:00.json",
        "public.customer/2021-04-26T09:00:00.csv",
        "public.customer/ddl/2021-04-26T09:00:00.sql",
        "public.product/2021-04-26T09:00:00.csv",
        "public.product/ddl/2021-04-26T09:00:00.sql",
        "public.sales/2021-04-26T09:00:00.csv",
        "public.sales/ddl/2021-04-26T09:00:00.sql",
        "public.store/2021-04-26T09:00:00.csv",
        "public.store/ddl/2021-04-26T09:00:00.sql",
        "public/ddl/2021-04-26T09:00:00.sql",
    ]


def test_restore_ddl(pg, empty_db, mf, s3_resource):
    mf.store.new()
    mf.store.new()

    # Prove that the database is in fact empty before we start.
    with pytest.raises(sqlalchemy.exc.ProgrammingError) as e:
        empty_db.query(Store).all()
    assert "does not exist" in str(e.value)
    empty_db.rollback()

    test_backup_ddl(pg, s3_resource)

    config = RootConfig.from_dict(
        {
            "location": "s3://sample-bucket/{table}",
            "ddl": {"enabled": True, "clean": False, "location": "s3://sample-bucket/ddl"},
            "tables": ["public.*"],
            "strict": True,
            "sequences": False,
            **s3_config,
        }
    )

    restore_all(empty_db, config.restore)
    empty_db.commit()

    rows = empty_db.query(Store).all()
    assert len(rows) == 2

    # Assert the rest of the tables exist
    empty_db.query(Product).all()
    empty_db.query(Customer).all()
    empty_db.query(Sale).all()


def test_restore_ddl_disabled(pg, empty_db, mf, s3_resource):
    mf.store.new()
    mf.store.new()

    test_backup_ddl(pg, s3_resource)

    config = RootConfig.from_dict(
        {
            "location": "s3://sample-bucket/{table}",
            "strict": True,
            "ddl": {"enabled": False},
            "tables": ["public.*"],
            **s3_config,
        }
    )

    restore_all(empty_db, config.restore)

    with pytest.raises(sqlalchemy.exc.ProgrammingError) as e:
        empty_db.query(Store).all()
    assert "does not exist" in str(e.value)
