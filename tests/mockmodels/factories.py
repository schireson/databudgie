import datetime
import logging

from faker import Faker
from sqlalchemy_model_factory import autoincrement, register_at

from tests.mockmodels.models import Advertiser, DatabudgieBackup, GenericAd, Product, Sale

logging.getLogger("faker").setLevel(logging.INFO)
fake = Faker()


@register_at("advertiser")
@autoincrement
def create_advertiser(autoincrement: int, name: str = None):
    name = name or fake.company()
    return Advertiser(id=autoincrement, name=name)


@register_at("product")
@autoincrement
def create_product(
    autoincrement: int,
    external_id: int = None,
    external_name: str = None,
    external_status: str = "ACTIVE",
    active: bool = True,
    advertiser: Advertiser = None,
):
    if not advertiser:
        advertiser = create_advertiser()

    external_id = external_id or fake.unique.pyint()
    external_name = external_name or fake.name()

    return Product(
        id=autoincrement,
        external_id=external_id,
        external_name=external_name,
        external_status=external_status,
        active=active,
        advertiser=advertiser,
    )


@register_at("generic_ad")
@autoincrement
def create_generic_ad(
    autoincrement: int,
    external_id: int = None,
    external_name: str = None,
    primary_text: str = None,
    type: str = "single_media",
    active: bool = True,
    external_status: str = "ACTIVE",
    advertiser: Advertiser = None,
    product: Product = None,
):

    if not advertiser:
        advertiser = create_advertiser()

    if not product:
        product = create_product(advertiser=advertiser)

    external_id = external_id or fake.unique.pyint()
    external_name = external_name or fake.name()
    primary_text = primary_text or fake.text()[:30]

    return GenericAd(
        id=autoincrement,
        external_id=external_id,
        external_name=external_name,
        primary_text=primary_text,
        type=type,
        active=active,
        external_status=external_status,
        advertiser=advertiser,
        product=product,
    )


@register_at("sale")
@autoincrement
def create_sale(
    autoincrement: int,
    external_id: str = None,
    advertiser_id: int = None,
    product_id: int = None,
    sale_value: float = None,
    sale_date: datetime.date = None,
    active: bool = None,
):
    return Sale(
        id=autoincrement,
        external_id=external_id or str(fake.pyint()),
        advertiser_id=advertiser_id or fake.pyint(),
        product_id=product_id or fake.pyint(),
        sale_value=sale_value or fake.pydecimal(left_digits=5, right_digits=2, positive=True),
        sale_date=sale_date or fake.date(),
        active=active or fake.boolean(),
    )


@register_at("manifest", name="backup")
def create_backup_manifest(
    transaction: int = None, table: str = None, file_path: str = None, exported_at: str = None,
):
    return DatabudgieBackup(
        transaction=transaction or fake.pyint(),
        table=table or fake.word(),
        file_path=file_path or fake.file_path(),
        exported_at=exported_at or fake.date_time_this_year(),
    )
