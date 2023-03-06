import pytest
from click.testing import CliRunner
from ruamel.yaml import YAML

from databudgie.cli import cli

yaml = YAML()


@pytest.mark.parametrize("command", ("backup", "restore"))
def test_no_default_file_warns_of_no_url(command):
    runner = CliRunner()
    result = runner.invoke(cli, [command])
    assert "No config found for 'url' field" in result.output
    assert result.exit_code == 2


def test_config_command_works_without_url():
    runner = CliRunner()
    result = runner.invoke(cli, ["config"])
    assert "connection:" in result.output
    assert result.exit_code == 0


def test_cli_args_pass_through_to_config():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--ddl",
            "--url=foo",
            "--location=bar",
            "--table=baz",
            "--table=public.*",
            "--exclude",
            "foo",
            "-x",
            "bar",
            "config",
        ],
    )

    config = yaml.load(result.output)

    for part in ["backup", "restore"]:
        config_part = config[part]
        assert config_part["connection"] == {"name": "default", "url": "foo"}
        assert config_part["ddl"]["enabled"] is True
        assert config_part["tables"][0]["location"] == "bar"
        assert config_part["tables"][0]["name"] == "baz"
        assert config_part["tables"][1]["location"] == "bar"
        assert config_part["tables"][1]["name"] == "public.*"

        assert config_part["tables"][0]["exclude"] == ["foo", "bar"]
        assert config_part["tables"][1]["exclude"] == ["foo", "bar"]
