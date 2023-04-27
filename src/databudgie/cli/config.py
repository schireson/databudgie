from __future__ import annotations
import click

import io
from dataclasses import asdict, dataclass
from pathlib import Path

from configly import Config, JsonLoader, TomlLoader, YamlLoader
from rich.console import Console
from rich.syntax import Syntax
from ruamel.yaml import YAML

from databudgie.config import Config as DatabudgieConfig

DEFAULT_CONFIG_FILES = ["databudgie.yml", "config.databudgie.yml", "databudgie.json", "databudgie.toml"]
_loaders = {
    ".json": JsonLoader,
    ".toml": TomlLoader,
    ".yaml": YamlLoader,
    ".yml": YamlLoader,
}


@dataclass
class CliConfig(DatabudgieConfig):
    tables: list[str] | None = None
    exclude: list[str] | None = None
    ddl: bool | None = None
    url: str | dict | None = None
    location: str | None = None
    adapter: str | None = None
    strict: bool | None = None

    def to_dict(self) -> dict:
        config = asdict(self)
        return {k: v for k, v in config.items() if v is not None}


def load_configs(*file_names):
    ignore_missing = False
    if not file_names:
        ignore_missing = True
        file_names = DEFAULT_CONFIG_FILES

    result = []
    for file_name in file_names:
        path = Path(file_name)
        if not path.exists() and ignore_missing:
            continue

        loader_cls = _loaders.get(path.suffix.lower())
        if loader_cls is None:
            raise click.UsageError(f"Unsupported file extension: {path.suffix}")

        config = Config.from_loader(loader_cls(), file_name)

        result.append(config.to_dict())
    return result


def pretty_print(config: DatabudgieConfig):
    """Pretty print a config model."""
    console = Console()
    buffer = io.StringIO()

    config_as_dict = config.to_dict()
    yaml = YAML()
    yaml.default_flow_style = False
    yaml.dump(config_as_dict, buffer)

    buffer.seek(0)
    data = buffer.read()
    syntax = Syntax(data, "yaml")
    console.print(syntax)
