import logging

from faker import Faker
from sqlalchemy_model_factory import register_at

from tests.mockmodels.models import Advertiser, GenericAd, LineItem

logging.getLogger("faker").setLevel(logging.INFO)
fake = Faker()


@register_at("advertiser")
def create_advertiser(id: int = None, name: str = None):
    name = name or fake.company()
    return Advertiser(id=id, name=name)


@register_at("line_item")
def create_line_item(
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

    return LineItem(
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
    line_item_id: int = None,
    external_name: str = None,
    primary_text: str = None,
    type: str = "single_media",
    active: bool = True,
    external_status: str = "ACTIVE",
):

    if not advertiser_id:
        advertiser = create_advertiser()
        advertiser_id = advertiser.id

    if not line_item_id:
        line_item = create_line_item(advertiser_id=advertiser_id)
        line_item_id = line_item.id

    external_id = external_id or fake.unique.pyint()
    external_name = external_name or fake.name()
    primary_text = primary_text or fake.text()[:30]

    return GenericAd(
        id=id,
        external_id=external_id,
        advertiser_id=advertiser_id,
        line_item_id=line_item_id,
        external_name=external_name,
        primary_text=primary_text,
        type=type,
        active=active,
        external_status=external_status,
    )
