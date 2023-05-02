from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from unittest.mock import mock_open, patch

import pytest

from databudgie.cli.config import (
    CliConfig,
    collect_config,
    collect_env_config,
    load_file_configs,
)


@dataclass
class MockOpen:
    content: str | list[str]
    count = 0
    builtin_open = open

    def open(self, *args, **kwargs):
        # Apparently pdb opens something, so it makes debugging challenging unless we scope the mock.
        if args[0].endswith(("yml", "yaml", "json", "toml")):
            if isinstance(self.content, list):
                mopen = mock_open(read_data=self.content[self.count].encode("utf-8"))
                self.count += 1
            else:
                mopen = mock_open(read_data=self.content.encode("utf-8"))

            return mopen(*args, **kwargs)
        return self.builtin_open(*args, **kwargs)


@contextmanager
def environ(values: dict[str, str]):
    with patch("os.environ.items", return_value=values.items()):
        yield


@contextmanager
def file_content(content: str | list[str]):
    open_patch = patch("builtins.open", side_effect=MockOpen(content).open)
    exists_patch = patch("pathlib.Path.exists", return_value=True)
    with open_patch, exists_patch:
        yield


class Test_collect_config:
    def test_prefer_cli_most(self):
        content = """
        location: file
        tables:
         - foo
        """
        cli_config = CliConfig(location="cli")
        with environ({"DATABUDGIE_LOCATION": "env"}), file_content(content):
            root_config = collect_config(cli_config=cli_config)
        assert root_config.backup.tables[0].location == "cli"

    def test_prefer_env_over_file(self):
        content = """
        tables:
         - foo
        """
        with environ({"DATABUDGIE_LOCATION": "env"}), file_content(content):
            root_config = collect_config(cli_config=CliConfig())
        assert root_config.backup.tables[0].location == "env"

    def test_files_in_order(self):
        content = [
            """
            location: file1
            tables:
             - foo
            """,
            """
            location: file2
            tables:
             - foo
            """,
        ]
        with environ({}), file_content(content):
            root_config = collect_config(cli_config=CliConfig(), file_names=["file1.yml", "file2.yml"])
        assert root_config.backup.tables[0].location == "file1"

    def test_env_false_value_coerced(self):
        content = """
        tables:
         - foo
        """
        with environ({"DATABUDGIE_BACKUP__DDL": ""}), file_content(content):
            root_config = collect_config(cli_config=CliConfig())
        assert root_config.backup.tables[0].ddl is False
        assert root_config.restore.tables[0].ddl is True


class Test_load_file_configs:
    @pytest.mark.parametrize("name", ("foo.yaml", "bar.yml"))
    def test_load_file_configs_yaml(self, name):
        with file_content("tables: \n  - foo"):
            configs = load_file_configs(name)

        assert configs[0]["tables"] == ["foo"]

    def test_load_file_configs_json(self):
        with file_content('{"tables": ["foo"]}'):
            configs = load_file_configs("foo.json")

        assert configs[0]["tables"] == ["foo"]

    def test_load_file_configs_toml(self):
        with file_content('tables = ["foo"]'):
            configs = load_file_configs("foo.toml")

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
            configs = load_file_configs("foo.toml")

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
