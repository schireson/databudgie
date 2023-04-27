from __future__ import annotations

import io
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import MutableMapping

import click
from configly import Config, JsonLoader, TomlLoader, YamlLoader
from rich.console import Console
from rich.syntax import Syntax
from ruamel.yaml import YAML

from databudgie.config import Config as DatabudgieConfig
from databudgie.config import ConfigStack, RootConfig

DEFAULT_CONFIG_FILES = (
    "config.databudgie.yml",
    "databudgie.yml",
    "databudgie.yaml",
    "databudgie.json",
    "databudgie.toml",
)

_loaders: dict[str, type[JsonLoader | TomlLoader | YamlLoader]] = {
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


def collect_config(cli_config: CliConfig, *file_names):
    configs = load_configs(*file_names)
    env_config = collect_env_config()
    config_stack = ConfigStack(cli_config.to_dict(), env_config, *configs)
    return RootConfig.from_stack(config_stack)


def load_configs(*file_names: str):
    stop_when_found = False
    if not file_names:
        stop_when_found = True
        file_names = DEFAULT_CONFIG_FILES

    result = []
    for file_name in file_names:
        path = Path(file_name)
        if not path.exists() and stop_when_found:
            continue

        loader_cls = _loaders.get(path.suffix.lower())
        if loader_cls is None:
            raise click.UsageError(f"Unsupported file extension: {path.suffix}")

        config: Config = Config.from_loader(loader_cls(), file_name)  # type: ignore
        result.append(config.to_dict())

        if stop_when_found:
            break

    return result


def collect_env_config(environ: MutableMapping[str, str] = os.environ, prefix="DATABUDGIE_"):
    len_prefix = len(prefix)
    env_vars = {k[len_prefix:]: v for k, v in environ.items() if k.startswith(prefix)}

    config: dict = {}
    for env_var, value in env_vars.items():
        *segments, last_segment = env_var.split("__")
        context = config
        for segment in segments:
            context = context.setdefault(segment.lower(), {})

        context[last_segment.lower()] = value

    return config


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
