from databudgie.adapter.base import Adapter
from databudgie.etl.backup import backup
from databudgie.etl.base import TableOp
from databudgie.etl.restore import restore_all
from tests.mockmodels.models import Sale
from tests.utils import make_config


def test_type_conversion(pg, mf, s3_resource):
    sales = [mf.sale.new() for _ in range(100)]
    original_sales = {
        s.id: dict(
            id=s.id,
            external_id=s.external_id,
            store_id=s.store_id,
            product_id=s.product_id,
            sale_value=s.sale_value,
            sale_date=s.sale_date,
            active=s.active,
        )
        for s in sales
    }

    location = "s3://sample-bucket/public.sales"

    backup(
        pg,
        s3_resource=s3_resource,
        adapter=Adapter.get_adapter(pg),
        config=make_config(backup={}),
        table_op=TableOp("sales", dict(location=location, query="select * from public.sales")),
    )

    restore_all(
        pg,
        config=make_config(restore={"tables": {"public.sales": {"truncate": True, "location": location}}}),
    )

    restored_sales = {s.id: s for s in pg.query(Sale).all()}

    for id, original_sale in original_sales.items():
        restored_sale = restored_sales[id]
        assert original_sale["id"] == restored_sale.id
        assert original_sale["external_id"] == restored_sale.external_id
        assert original_sale["store_id"] == restored_sale.store_id
        assert original_sale["product_id"] == restored_sale.product_id
        assert original_sale["sale_value"] == restored_sale.sale_value
        assert original_sale["sale_date"] == restored_sale.sale_date
        assert original_sale["active"] == restored_sale.active
