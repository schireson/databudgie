import pytest
from pytest_mock_resources import create_postgres_fixture

from databudgie.match import expand_table_globs

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
