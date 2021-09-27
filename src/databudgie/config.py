import re
from typing import Dict, Optional, Union

from configly import Config


def populate_refs(config: Config) -> Config:
    """Replace property references with the referenced values.

    Traverse through the config object and replace values like `${ref:backup.table.mytable.location}`
    with the value found at the referenced location.
    """

    root_conf = config.to_dict()
    ref_regex = re.compile(r"\$\{ref:(.*)\}")  # matches `${ref:somevalue}`

    def recurse(conf: Dict[str, Union[str, dict]]) -> dict:
        for key, value in conf.items():
            if isinstance(value, str):  # if value is string, check and perform replacement
                match: Optional[re.Match] = ref_regex.match(value)
                if match:
                    source_path = match.group(1)
                    source_value = extract_ref_value(source_path, root_conf)
                    conf[key] = source_value
            elif isinstance(value, dict):  # if value is config, recurse
                conf[key] = recurse(value)
        return conf

    updated_conf = recurse(root_conf)
    return Config(updated_conf)


def extract_ref_value(ref: str, root_dict: dict) -> Union[str, dict]:
    """Traverse a dict to extract referenced value."""
    ref_parser = re.compile(r'(\w+)|"([\w\.]*)"')
    ref_value = root_dict

    # the regex pattern returns tuples where keys with dots are the second item,
    # e.g [("backup", None), ("table", None), (None, "my.table"), ("location", None)]
    ref_path = [x or y for x, y in ref_parser.findall(ref)]

    try:
        for part in ref_path:
            ref_value = ref_value[part]
        return ref_value
    except KeyError:
        raise KeyError(f"Referenced value not found: {ref}")
