import datetime
import logging

from faker import Faker
from sqlalchemy_model_factory import autoincrement, register_at

from tests.mockmodels.models import Advertiser, GenericAd, Product, Sale

logging.getLogger("faker").setLevel(logging.INFO)
fake = Faker()


@register_at("advertiser")
def create_advertiser(id: int = None, name: str = None):
    name = name or fake.company()
    return Advertiser(id=id, name=name)


@register_at("product")
def create_product(
    id: int = None,
    advertiser_id: int = None,
    external_id: int = None,
    external_name: str = None,
    external_status: str = "ACTIVE",
    active: bool = True,
):
    if not advertiser_id:
        advertiser = create_advertiser()
        advertiser_id = advertiser.id

    external_id = external_id or fake.unique.pyint()
    external_name = external_name or fake.name()

    return Product(
        id=id,
        advertiser_id=advertiser_id,
        external_id=external_id,
        external_name=external_name,
        external_status=external_status,
        active=active,
    )


@register_at("facebook_ad")
def create_facebook_ad(
    id: int = None,
    external_id: int = None,
    advertiser_id: int = None,
    product_id: int = None,
    external_name: str = None,
    primary_text: str = None,
    type: str = "single_media",
    active: bool = True,
    external_status: str = "ACTIVE",
):

    if not advertiser_id:
        advertiser = create_advertiser()
        advertiser_id = advertiser.id

    if not product_id:
        product = create_product(advertiser_id=advertiser_id)
        product_id = product.id

    external_id = external_id or fake.unique.pyint()
    external_name = external_name or fake.name()
    primary_text = primary_text or fake.text()[:30]

    return GenericAd(
        id=id,
        external_id=external_id,
        advertiser_id=advertiser_id,
        product_id=product_id,
        external_name=external_name,
        primary_text=primary_text,
        type=type,
        active=active,
        external_status=external_status,
    )


@register_at("sale")
@autoincrement
def create_sale(
    autoincrement: int = None,
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
