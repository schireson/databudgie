from contextlib import contextmanager
from unittest.mock import mock_open, patch

import pytest

from databudgie.cli.config import load_configs, collect_env_config


@contextmanager
def file_content(content):
    open_patch = patch("builtins.open", mock_open(read_data=content.encode("utf-8")))
    exists_patch = patch("os.path.exists", return_value=True)
    with open_patch, exists_patch:
        yield


class Test_load_configs:
    @pytest.mark.parametrize("name", ("foo.yaml", "bar.yml"))
    def test_load_configs_yaml(self, name):
        with file_content("tables: \n  - foo"):
            configs = load_configs(name)

        assert configs[0]["tables"] == ["foo"]

    def test_load_configs_json(self):
        with file_content('{"tables": ["foo"]}'):
            configs = load_configs("foo.json")

        assert configs[0]["tables"] == ["foo"]

    def test_load_configs_toml(self):
        with file_content('tables = ["foo"]'):
            configs = load_configs("foo.toml")

        assert configs[0]["tables"] == ["foo"]

    def test_toml_config(self):
        content = """
        [[backup.tables]]
        name = 'foo'
        query = 'select * from table'

        [[backup.tables]]
        name = 'bar'
        """
        with file_content(content):
            configs = load_configs("foo.toml")

        expected_result = {"backup": {"tables": [{"name": "foo", "query": "select * from table"}, {"name": "bar"}]}}
        assert configs[0] == expected_result


class Test_collect_env_config:
    def test_none(self):
        config = collect_env_config({}, prefix="budgie_")
        assert config == {}

    def test_ignore_non_prefixed_vars(self):
        config = collect_env_config({"foo": "bar", "budgie_cat": "dog"}, prefix="budgie_")
        assert config == {"cat": "dog"}

    def test_nested_path(self):
        config = collect_env_config(
            {"budgie_cat__hair__color": "white", "budgie_conn__url": "http", "budgie_truncate": "true"},
            prefix="budgie_",
        )
        assert config == {
            "cat": {
                "hair": {"color": "white"},
            },
            "conn": {
                "url": "http",
            },
            "truncate": "true",
        }
