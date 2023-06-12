import io
import shlex
import traceback
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from ruamel.yaml import YAML

from databudgie.cli import cli

yaml = YAML()


def run_command(command, assert_exit_code=0):
    command_list = shlex.split(command)
    runner = CliRunner()

    with patch("sys.stdin", new=io.StringIO()):
        result = runner.invoke(cli, command_list)
    if result.exit_code != 0:
        traceback.print_exception(*result.exc_info)

    assert result.exit_code == assert_exit_code
    return result


@pytest.mark.parametrize("command", ("backup", "restore"))
def test_no_default_file_warns_of_no_url(command):
    result = run_command(command, assert_exit_code=2)
    assert "did not resolve to a connection" in result.output


class TestConfigCommand:
    def test_config_command_works_without_url(self):
        result = run_command("config")
        assert "connection:" in result.output

    def test_cli_args_pass_through_to_config(self):
        result = run_command("--ddl --url=foo --location=bar --table=baz --table=public.* --exclude foo -x bar config")

        config = yaml.load(result.output)

        for part in ["backup", "restore"]:
            config_part = config[part]
            assert config_part["connection"] == "foo"
            assert config_part["ddl"]["enabled"] is True
            assert config_part["tables"][0]["location"] == "bar"
            assert config_part["tables"][0]["name"] == "baz"
            assert config_part["tables"][1]["location"] == "bar"
            assert config_part["tables"][1]["name"] == "public.*"

            assert config_part["tables"][0]["exclude"] == ["foo", "bar"]
            assert config_part["tables"][1]["exclude"] == ["foo", "bar"]

    def test_raw_config(self):
        result = run_command('--raw-config=\'{"tables": ["foo"]}\' config')

        config = yaml.load(result.output)
        tables = config["backup"]["tables"]
        assert len(tables) == 1
        assert tables[0]["name"] == "foo"

    def test_raw_config_format(self):
        result = run_command("--raw-config='tables:\n  - foo' --raw-config-format=yaml config")

        config = yaml.load(result.output)
        tables = config["backup"]["tables"]
        assert len(tables) == 1
        assert tables[0]["name"] == "foo"
