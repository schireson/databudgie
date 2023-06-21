from __future__ import annotations

import io
import os
import sys
from dataclasses import asdict, dataclass
from os import isatty
from pathlib import Path
from typing import Iterable, MutableMapping

import click
from configly import Config, loaders
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

file_loaders: dict[str, type] = {}
for ext, loader_name in [("yaml", "YamlLoader"), ("yml", "YamlLoader"), ("json", "JsonLoader"), ("toml", "TomlLoader")]:
    loader = getattr(loaders, loader_name)
    if loader:
        file_loaders[ext] = loader


@dataclass
class CliConfig(DatabudgieConfig):
    tables: list[str] | None = None
    exclude: list[str] | None = None
    ddl: bool | None = None
    url: str | dict | None = None
    location: str | None = None
    adapter: str | None = None
    strict: bool | None = None
    connection: str | None = None

    def to_dict(self) -> dict:
        config = asdict(self)
        return {k: v for k, v in config.items() if v is not None}


def collect_config(
    *,
    cli_config: CliConfig | None = None,
    file_names: Iterable[str] = (),
    raw_config: str | None = None,
    raw_config_format: str = "",
):
    """Collect configuration from various locations into a stacked `RootConfig`.

    Configuration is collected according to the following (descending) priority order:
        - CLI configuration options
        - Environment variable configuration
        - stdin/raw configuration content
        - file-based configuration

    """
    configs = []

    if cli_config:
        configs.append(cli_config.to_dict())

    env_config = collect_env_config()
    configs.append(env_config)

    stdin_config = collect_stdin_config(raw_config_format)
    if stdin_config:
        configs.append(stdin_config)

    if raw_config:
        configs.append(collect_raw_config(content=raw_config, format=raw_config_format))

    if file_names or (not stdin_config and not raw_config):
        # That is, if file-names were provided, always add them. Otherwise, load default files if
        # we're not otherwise receiving configuration through stdin/raw-config.
        configs.extend(load_file_configs(*file_names))

    config_stack = ConfigStack(*configs)
    return RootConfig.from_stack(config_stack)


def collect_env_config(environ: MutableMapping[str, str] = os.environ, prefix="DATABUDGIE_"):
    """Collect environment variables into a config structure."""
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


def collect_stdin_config(format: str) -> dict | None:
    """Collect stdin, if available, as a raw config structure."""
    try:
        is_pipe = not isatty(sys.stdin.fileno())
    except io.UnsupportedOperation:
        is_pipe = False

    if not is_pipe:
        return None

    content = sys.stdin.read()
    return collect_raw_config(content=content, format=format)


def load_file_configs(*file_names: str):
    """Collect configs from the set of input files.

    If no files are provided, falls back to the default file lookup heirarchy.
    """
    stop_when_found = False
    if not file_names:
        stop_when_found = True
        file_names = DEFAULT_CONFIG_FILES

    result = []
    for file_name in file_names:
        path = Path(file_name)
        if not path.exists() and stop_when_found:
            continue

        config = collect_raw_config(file=file_name, format=path.suffix.lower()[1:])
        result.append(config)

        if stop_when_found:
            break

    return result


def collect_raw_config(*, format: str, content: str | None = None, file: str | None = None) -> dict:
    loader_cls = file_loaders.get(format)
    if loader_cls is None:
        formats = ", ".join(file_loaders.keys())
        raise click.UsageError(f"File format must be one of: {formats}")

    config: Config = Config.from_loader(loader_cls(), file=file, content=content)
    return config.to_dict()


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
