import os
from pathlib import Path

from configly import Config


def get_config():
    file_path = Path(__file__)
    repo_base = file_path.parent.parent.parent  # great grandparent
    config = Config.from_yaml(os.path.join(repo_base, "config.yml"))
    return config
