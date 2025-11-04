from pytest_mock_resources import create_postgres_fixture
from sqlalchemy import Column, ForeignKey, MetaData, types
from sqlalchemy.ext.declarative import declarative_base

from databudgie.backup import backup_all
from databudgie.config import RootConfig
from databudgie.restore import restore_all
from tests.utils import mock_s3_csv, s3_config

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class Store(Base):  # type: ignore
    __tablename__ = "t0_store"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)


class Product(Base):  # type: ignore
    __tablename__ = "t1_product"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    store_id = Column(types.Integer(), ForeignKey("t0_store.id"), nullable=False)


class Customer(Base):  # type: ignore
    __tablename__ = "t0_customer"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)

    product_id = Column(types.Integer(), ForeignKey("t1_product.id"), nullable=False)


class Sale(Base):  # type: ignore
    __tablename__ = "t0_sales"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    store_id = Column(types.Integer(), ForeignKey("t0_store.id"), nullable=False)
    product_id = Column(types.Integer(), ForeignKey("t1_product.id"), nullable=False)


class Address(Base):  # type: ignore
    __tablename__ = "t0_address"

    id = Column(types.Integer(), autoincrement=True, primary_key=True)
    customer_id = Column(types.Integer(), ForeignKey("t0_customer.id"), nullable=False)
    store_id = Column(types.Integer(), ForeignKey("t0_store.id"), nullable=False)


pg = create_postgres_fixture(Base, session=True)


def test_backup_follow_foreign_keys(pg, s3_resource):
    """Assert `follow_foreign_keys` option functions.

    It should recursively collect tables which are foreignkey referenced
    to tables in "tables" list.
    """
    config = RootConfig.from_dict(
        {
            "location": "{table}",
            "tables": ["public.t0_address"],
            "follow_foreign_keys": True,
            "sequences": False,
            "root_location": "s3://sample-bucket/",
            "strict": True,
            **s3_config,
        }
    )

    backup_all(pg, config.backup)

    all_object_keys = [obj.key for obj in s3_resource.Bucket("sample-bucket").objects.all()]
    assert all_object_keys == [
        "public.t0_address/2021-04-26T09:00:00.csv",
        "public.t0_address/public.t0_customer/2021-04-26T09:00:00.csv",
        "public.t0_address/public.t0_store/2021-04-26T09:00:00.csv",
        "public.t0_address/public.t1_product/2021-04-26T09:00:00.csv",
    ]


def test_restore_follow_foreign_keys(pg, s3_resource):
    """Assert `follow_foreign_keys` option functions.

    It should recursively collect tables which are foreignkey referenced
    to tables in "tables" list.

    **Note** The names of the tables is significant to the test proving the store
    correctly orders the tables by foreign key. t0/t1 would cause an unsorted
    result set to fail due to the foreign keys being unfulfilled at time of restore.
    """
    mock_s3_csv(s3_resource, "public.t0_address/public.t0_store/2021-04-26T09:00:00.csv", [{"id": 1}])
    mock_s3_csv(s3_resource, "public.t0_address/public.t1_product/2021-04-26T09:00:00.csv", [{"id": 2, "store_id": 1}])
    mock_s3_csv(
        s3_resource, "public.t0_address/public.t0_customer/2021-04-26T09:00:00.csv", [{"id": 3, "product_id": 2}]
    )
    mock_s3_csv(s3_resource, "public.t0_address/2021-04-26T09:00:00.csv", [{"id": 4, "customer_id": 3, "store_id": 1}])

    config = RootConfig.from_dict(
        {
            "location": "{table}",
            "tables": ["public.t0_address"],
            "follow_foreign_keys": True,
            "sequences": False,
            "clean": True,
            "strict": True,
            "root_location": "s3://sample-bucket/",
            **s3_config,
        }
    )

    restore_all(
        pg,
        config.restore,
    )

    assert pg.query(Store).one().id == 1
    assert pg.query(Product).one().id == 2
    assert pg.query(Customer).one().id == 3
    assert pg.query(Address).one().id == 4
