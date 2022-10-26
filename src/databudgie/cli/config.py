from __future__ import annotations

import io
from dataclasses import asdict, dataclass
from typing import List, Optional

from configly import Config
from rich.console import Console
from rich.syntax import Syntax
from ruamel.yaml import YAML

from databudgie.config import Config as DatabudgieConfig

DEFAULT_CONFIG_FILE = "config.databudgie.yml"


@dataclass
class CliConfig(DatabudgieConfig):
    tables: Optional[List[str]] = None
    ddl: Optional[bool] = None
    url: Optional[str] = None
    location: Optional[str] = None
    adapter: Optional[str] = None
    strict: Optional[bool] = None

    def to_dict(self) -> dict:
        config = asdict(self)
        return {k: v for k, v in config.items() if v is not None}


def load_configs(file_names):
    result = []
    for file_name in file_names:
        if file_name == DEFAULT_CONFIG_FILE:
            try:
                config = Config.from_yaml(file_name)
            except FileNotFoundError:
                continue
        else:
            config = Config.from_yaml(file_name)

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
