from dataclasses import asdict, dataclass
from typing import List, Optional

from configly import Config

from databudgie.config import models

DEFAULT_CONFIG_FILE = "config.databudgie.yml"


@dataclass
class CliConfig(models.Config):
    tables: Optional[List[str]] = None
    ddl: Optional[bool] = None
    url: Optional[str] = None
    location: Optional[str] = None

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
