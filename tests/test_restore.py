import os.path
import tempfile
import uuid
from datetime import datetime

import faker
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm.session import sessionmaker

from databudgie.config import RootConfig
from databudgie.restore import restore_all
from tests.mockmodels.models import Product, Store
from tests.utils import mock_csv, mock_s3_csv, s3_config

fake = faker.Faker()


def test_restore_all(pg, s3_resource):
    """Validate restore functionality for all tables in a config."""
    mock_store = {"id": 1, "name": fake.name()}
    mock_product = {
        "id": 1,
        "store_id": 1,
        "external_id": str(fake.unique.pyint()),
        "external_name": fake.name(),
        "external_status": "ACTIVE",
        "active": True,
    }

    mock_s3_csv(s3_resource, "public.store/2021-04-26T09:00:00.csv", [mock_store])
    mock_s3_csv(s3_resource, "public.product/2021-04-26T09:00:00.csv", [mock_product])

    config = RootConfig.from_dict(
        {
            **s3_config,
            "strict": True,
            "restore": {
                "tables": {
                    "public.store": {"location": "s3://sample-bucket/public.store", "strategy": "use_latest_filename"},
                    "public.product": {
                        "location": "s3://sample-bucket/public.product",
                        "strategy": "use_latest_metadata",
                    },
                },
            },
        }
    )
    restore_all(pg, config.restore)

    assert pg.query(Store).count() == 1
    assert pg.query(Product).count() == 1


def test_restore_one(pg, mf, s3_resource):
    """Validate restore functionality for a single table."""
    store = mf.store.new(name=fake.name())

    mock_products = [
        {
            "id": 1,
            "store_id": store.id,
            "external_id": str(fake.unique.pyint()),
            "external_name": fake.name(),
            "external_status": "ACTIVE",
            "active": True,
        },
        {
            "id": 2,
            "store_id": store.id,
            "external_id": str(fake.unique.pyint()),
            "external_name": fake.name(),
            "external_status": None,
            "active": False,
        },
    ]

    mock_s3_csv(s3_resource, "products/2021-04-26T09:00:00.csv", mock_products)

    config = RootConfig.from_dict({**s3_config, "tables": {"product": {"location": "s3://sample-bucket/products"}}})
    restore_all(pg, config.restore)

    products = pg.query(Product).all()
    assert len(products) == 2
    assert products[0].id == 1
    assert products[0].active is True
    assert products[0].external_id == mock_products[0]["external_id"]
    assert products[1].id == 2
    assert products[1].active is False
    assert products[1].external_id == mock_products[1]["external_id"]


def test_restore_all_overwrite_cascade(pg, mf, s3_resource):
    """Validate behavior for the cascading truncate option."""
    store = mf.store.new(name=fake.name())
    mf.product.new(store=store)
    mf.product.new(store=store)

    mock_product = {
        "id": 1,
        "store_id": store.id,
        "external_id": fake.unique.pyint(),
        "external_name": fake.name(),
        "external_status": "ACTIVE",
        "active": True,
    }

    mock_s3_csv(s3_resource, "products/2021-04-26T09:00:00.csv", [mock_product])

    config = RootConfig.from_dict(
        {
            "restore": {
                **s3_config,
                "tables": {"product": {"truncate": True, "location": "s3://sample-bucket/products"}},
            },
        }
    )
    restore_all(pg, restore_config=config.restore)

    stores = pg.query(Product).all()
    assert len(stores) == 1


def test_restore_all_local_files(pg, mf):
    """Validate behavior for the cascading truncate option."""
    store = mf.store.new(name=fake.name())
    mf.product.new(store=store)
    mf.product.new(store=store)

    mock_product = {
        "id": 1,
        "store_id": store.id,
        "external_id": fake.unique.pyint(),
        "external_name": fake.name(),
        "external_status": "ACTIVE",
        "active": True,
    }

    fake_file_data = mock_csv([mock_product]).read()
    with tempfile.TemporaryDirectory() as dir_name:
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        with open(os.path.sep.join([dir_name, f"{now}.csv"]), "wb") as f:
            f.write(fake_file_data)

        config = RootConfig.from_dict(
            {
                "restore": {"tables": {"product": {"truncate": True, "location": dir_name}}},
                "sequences": False,
                "strict": True,
            }
        )
        restore_all(pg, restore_config=config.restore)

    stores = pg.query(Product).all()
    assert len(stores) == 1


def test_restore_glob(pg, mf, s3_resource):
    """Validate restore composes with glob table specifications."""
    # Prove we truncate the rows
    store = mf.store.new(name=fake.name())
    mf.product.new(store=store)
    mf.product.new(store=store)

    mock_s3_csv(
        s3_resource,
        "public.store/2021-04-26T09:00:00.csv",
        [
            {"id": 1, "name": fake.name()},
            {"id": 2, "name": fake.name()},
        ],
    )
    mock_s3_csv(
        s3_resource,
        "public.product/2021-04-26T09:00:00.csv",
        [
            {
                "id": 1,
                "store_id": 1,
                "external_id": fake.unique.pyint(),
                "external_name": fake.name(),
                "external_status": "ACTIVE",
                "active": True,
            },
        ],
    )

    config = RootConfig.from_dict(
        {
            **s3_config,
            "restore": {
                "tables": {"public.*": {"location": "s3://sample-bucket/{table}", "truncate": True}},
                "strict": True,
            },
        }
    )

    restore_all(pg, config.restore)

    stores = pg.query(Store).all()
    assert len(stores) == 2

    stores = pg.query(Product).all()
    assert len(stores) == 1


def test_reset_database(pg):
    pmr_credentials = pg.pmr_credentials
    url = pmr_credentials.as_sqlalchemy_url()

    with create_engine(url).execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
        random = str(uuid.uuid4()).replace("-", "_")
        database = f"foo_{random}"
        conn.execute(text(f"CREATE DATABASE {database}"))

    pmr_credentials.database = database
    url = pmr_credentials.as_sqlalchemy_url()

    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE foo ();"))
        conn.execute(text("CREATE TABLE bar ();"))

    config = RootConfig.from_dict(
        {
            "restore": {
                "url": str(url),
                "ddl": {
                    "clean": True,
                },
                "tables": {},
            }
        }
    )

    Session = sessionmaker()
    session = Session(bind=engine)
    restore_all(session, config.restore)
    session.close()

    with pytest.raises(ProgrammingError) as e:
        with create_engine(url).begin() as conn:
            conn.execute(text("SELECT * FROM foo"))

    assert '"foo" does not exist' in str(e)


@pytest.mark.parametrize(
    "config",
    (
        {
            "compression": "gzip",
            "tables": {
                "public.store": {
                    "location": "s3://sample-bucket/public.store",
                    "strategy": "use_latest_filename",
                },
            },
        },
        {
            "tables": {
                "public.store": {
                    "location": "s3://sample-bucket/public.store",
                    "strategy": "use_latest_filename",
                    "compression": "gzip",
                },
            },
        },
    ),
)
def test_compression(pg, s3_resource, config):
    """Validate restore functionality with compression enabled."""
    mock_store = {"id": 1, "name": fake.name()}

    mock_s3_csv(s3_resource, "public.store/2021-04-26T09:00:00.csv.gz", [mock_store], gzipped=True)

    config = RootConfig.from_dict(
        {
            "restore": config,
            **s3_config,
            "strict": True,
        }
    )
    restore_all(pg, config.restore)

    assert pg.query(Store).count() == 1
