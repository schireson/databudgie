from click.testing import CliRunner
from ruamel.yaml import YAML

from databudgie.cli import cli

yaml = YAML()


def test_no_default_file_warns_of_no_url():
    runner = CliRunner()
    result = runner.invoke(cli, ["config"])
    assert "field 'url' is required" in result.output
    assert result.exit_code == 2


def test_cli_args_pass_through_to_config():
    runner = CliRunner()
    result = runner.invoke(cli, ["--ddl", "--url=foo", "--location=bar", "--table=baz", "--table=public.*", "config"])

    config = yaml.load(result.output)

    for part in ["backup", "restore"]:
        config_part = config[part]
        assert config_part["url"] == "foo"
        assert config_part["ddl"]["enabled"] is True
        assert config_part["tables"][0]["location"] == "bar"
        assert config_part["tables"][0]["name"] == "baz"
        assert config_part["tables"][1]["location"] == "bar"
        assert config_part["tables"][1]["name"] == "public.*"
