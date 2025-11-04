import datetime
import logging
from typing import Optional

from faker import Faker
from sqlalchemy_model_factory import autoincrement, register_at

from tests.mockmodels.models import Customer, DatabudgieManifest, Product, Sale, Store

logging.getLogger("faker").setLevel(logging.INFO)
fake = Faker()


@register_at("store")
def create_store(name: Optional[str] = None, *, id: Optional[int] = None):
    name = name or fake.company()
    return Store(id=id, name=name)


@register_at("product")
def create_product(
    external_id: Optional[int] = None,
    external_name: Optional[str] = None,
    external_status: Optional[str] = "ACTIVE",
    active: Optional[bool] = True,
    store: Optional[Store] = None,
):
    if not store:
        store = create_store()

    external_id = external_id or fake.unique.pyint()
    external_name = external_name or fake.name()

    return Product(
        external_id=external_id,
        external_name=external_name,
        external_status=external_status,
        active=active,
        store=store,
    )


@register_at("customer")
@autoincrement
def create_customer(
    autoincrement: int,
    external_id: Optional[int] = None,
    external_name: Optional[str] = None,
    type: str = "new",
    active: bool = True,
    external_status: str = "ACTIVE",
    store: Optional[Store] = None,
    product: Optional[Product] = None,
):
    if not store:
        store = create_store()

    if not product:
        product = create_product(store=store)

    external_id = external_id or fake.unique.pyint()
    external_name = external_name or fake.name()

    return Customer(
        id=autoincrement,
        external_id=external_id,
        external_name=external_name,
        type=type,
        active=active,
        external_status=external_status,
        store=store,
        product=product,
    )


@register_at("sale")
@autoincrement
def create_sale(
    autoincrement: int,
    external_id: Optional[str] = None,
    store_id: Optional[int] = None,
    product_id: Optional[int] = None,
    sale_value: Optional[float] = None,
    sale_date: Optional[datetime.date] = None,
    active: Optional[bool] = None,
):
    return Sale(
        id=autoincrement,
        external_id=external_id or str(fake.pyint()),
        store_id=store_id or fake.pyint(),
        product_id=product_id or fake.pyint(),
        sale_value=sale_value or fake.pydecimal(left_digits=5, right_digits=2, positive=True),
        sale_date=sale_date or fake.date(),
        active=active or fake.boolean(),
    )


@register_at("manifest", name="backup")
def create_manifest_row(
    transaction: Optional[int] = None,
    table: Optional[str] = None,
    file_path: Optional[str] = None,
    timestamp: Optional[str] = None,
):
    return DatabudgieManifest(
        transaction=transaction or fake.pyint(),
        action="backup",
        table=table or fake.word(),
        file_path=file_path or fake.file_path(),
        timestamp=timestamp or fake.date_time_this_year(),
    )
