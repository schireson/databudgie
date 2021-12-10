import pytest
from pytest_mock_resources import create_postgres_fixture
from sqlalchemy import MetaData, Table
from sqlalchemy.schema import CreateSchema

from databudgie.match import collect_existing_tables, expand_table_globs

pg = create_postgres_fixture(session=True)


class Test_expand_table_globs:
    @pytest.mark.parametrize("glob", ("*", "*.*"))
    def test_unspecified_glob_includes_everything(self, glob):
        existing_tables = ["foo.bar", "wat.bar", "public.bar"]

        result = expand_table_globs(existing_tables, glob)
        assert result == ["foo.bar", "public.bar", "wat.bar"]

    def test_public_glob_includes_unspecified_schemas(self):
        existing_tables = ["foo.bar", "public.baz", "public.bar"]
        glob = "public.*"

        result = expand_table_globs(existing_tables, glob)
        assert result == ["public.bar", "public.baz"]

    def test_glob_excludes_other_schemas(self):
        existing_tables = ["foo.bar", "foo.baz", "public.bar"]
        glob = "foo.*"

        result = expand_table_globs(existing_tables, glob)
        assert result == ["foo.bar", "foo.baz"]

    def test_glob_schema_specific_table(self):
        existing_tables = ["foo.bar", "bar.bar", "public.bar", "public.baz"]
        glob = "*.bar"

        result = expand_table_globs(existing_tables, glob)
        assert result == ["bar.bar", "foo.bar", "public.bar"]


class Test_collect_existing_database_tables:
    def test_skips_information_schema(self, pg):
        result = collect_existing_tables(pg)
        assert result == []

    def test_collects_tables_from_schemas(self, pg):
        metadata = MetaData()

        Table("bar", metadata, schema="foo")
        Table("bar", metadata, schema="bar")

        connection = pg.connection()
        connection.execute(CreateSchema("foo"))
        connection.execute(CreateSchema("bar"))
        metadata.create_all(connection)

        result = collect_existing_tables(pg)
        assert result == ["bar.bar", "foo.bar"]

    def test_public_expanded(self, pg):
        metadata = MetaData()

        Table("bar", metadata)
        Table("baz", metadata)

        connection = pg.connection()
        metadata.create_all(connection)

        result = collect_existing_tables(pg)
        assert result == ["public.bar", "public.baz"]
