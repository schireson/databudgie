from sqlalchemy import MetaData, Table
from sqlalchemy.schema import CreateSchema

from databudgie.adapter.base import Adapter
from databudgie.backup import backup_all
from databudgie.config import RootConfig
from databudgie.restore import restore_all
from tests.mockmodels.models import Sale
from tests.utils import s3_config


def test_type_conversion(pg, mf, s3_resource):
    sales = [mf.sale.new() for _ in range(100)]
    original_sales = {
        s.id: {
            "id": s.id,
            "external_id": s.external_id,
            "store_id": s.store_id,
            "product_id": s.product_id,
            "sale_value": s.sale_value,
            "sale_date": s.sale_date,
            "active": s.active,
        }
        for s in sales
    }

    location = "s3://sample-bucket/public.sales"

    config = RootConfig.from_dict(
        {
            "tables": {"sales": {"location": location, "truncate": True}},
            **s3_config,
            "sequences": False,
        }
    )
    backup_all(pg, config.backup)
    restore_all(pg, restore_config=config.restore)

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


class Test_collect_existing_database_tables:
    def test_skips_information_schema(self, pg):
        result = Adapter.get_adapter(pg).collect_existing_tables(pg)
        for table_name in result:
            assert "information_schema" not in table_name
            assert "pg_catalog" not in table_name

    def test_collects_tables_from_schemas(self, pg):
        metadata = MetaData()

        Table("bar", metadata, schema="foo")
        Table("bar", metadata, schema="bar")

        connection = pg.connection()
        connection.execute(CreateSchema("foo"))
        connection.execute(CreateSchema("bar"))
        metadata.create_all(connection)

        result = Adapter.get_adapter(pg).collect_existing_tables(pg)
        assert all(table in result for table in ["bar.bar", "foo.bar"])

    def test_public_expanded(self, pg):
        metadata = MetaData()

        Table("bar", metadata)
        Table("baz", metadata)

        connection = pg.connection()
        metadata.create_all(connection)

        result = Adapter.get_adapter(pg).collect_existing_tables(pg)
        assert all(table in result for table in ["public.bar", "public.baz"])
