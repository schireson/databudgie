from typing import Dict, Iterable, List, Mapping, Optional, Tuple, Union

from configly import Config

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


class TableConf(TypedDict, total=False):
    location: str
    name: Optional[str]
    query: Optional[str]
    exclude: Optional[List[str]]
    strategy: Optional[str]
    truncate: Optional[bool]


def pretty_print(config: Config, indent: int = 0, increment: int = 2):
    """Pretty print a config object."""

    current_indent = " " * indent
    for key, value in config:
        if isinstance(value, Config):
            print(f"{current_indent}{key}:")
            pretty_print(value, indent + increment, increment)
        else:
            if isinstance(value, str):
                value = value.strip()
                value = value.replace("\n", "")
            print(f"{current_indent}{key}: {value}")


def compose_value(config: Config, *path, value=None, default=None) -> Config:
    """Compose the given path to a config value into a value for a **new** config.

    Given an input `config` and a series of string `path` fragments to navigate
    to, ensure given value exists.

    If `value` is set and there is not currently a value in the `config`,
    override the value with the given `value`. If `value` itself is `None`, fall
    back to `default` instead.

    Examples:
        >>> config = Config({'foo': {'bar': {'baz': False}}})
        >>> new_config = compose_value(config, 'foo', 'bar', 'baz', value=True)
        >>> assert config.foo.bar.baz is False, config
        >>> assert new_config.foo.bar.baz is True, new_config

        >>> config = Config({'foo': {'bar': {'baz': False}}})
        >>> new_config = compose_value(config, 'foo', 'bar', 'baz', value=None)
        >>> assert config.foo.bar.baz is False, config
        >>> assert new_config.foo.bar.baz is False, new_config

        >>> config = Config({'foo': {'bar': {}}})
        >>> new_config = compose_value(config, 'foo', 'bar', 'baz', value=True)
        >>> assert 'baz' not in config.foo.bar, config
        >>> assert new_config.foo.bar.baz is True, new_config

        >>> config = Config({'foo': {'bar': {}}})
        >>> new_config = compose_value(config, 'foo', 'bar', 'baz', value=None, default=True)
        >>> assert 'baz' not in config.foo.bar, config
        >>> assert new_config.foo.bar.baz is True, new_config

    """
    raw_config = config.to_dict()

    *path_parts, final_key = path
    context = raw_config
    for item in path_parts:
        if item not in context:
            context[item] = {}

        context = context[item]

    if value is not None:
        context[final_key] = value

    if final_key not in context:
        context[final_key] = default

    return Config(raw_config)


def normalize_table_config(tables: Union[Dict[str, TableConf], List[TableConf]]) -> Iterable[Tuple[str, TableConf]]:
    if isinstance(tables, dict):
        for name, table_conf in tables.items():
            yield (name, table_conf)

    else:
        for table_conf in tables:
            name = table_conf.get("name") or ""
            yield (name, table_conf)


def fallback_config_value(*configs: Mapping, key: str, default=None):
    """Compose the values between two pieces of `Config`, falling back to the.

    This can be used to define configuration at descending levels of specificity,
    in order to choose the most specific value given. the `configs` value should
    be given in ascending level of specificity.

    Examples:
        >>> from configly import Config

        >>> global_config = Config({'foo': 'global_foo', 'bar': 'global_bar', 'baz': 'global_baz'})
        >>> backup_config = Config({'bar': None})
        >>> table_config = Config({'foo': 'table_foo'})

        Choses the most specific value available.
        >>> fallback_config_value(global_config, backup_config, table_config, key='foo')
        'table_foo'

        If a value is explicitly provided, even if `None`, it should be used
        >>> fallback_config_value(global_config, backup_config, table_config, key='bar')

        >>> fallback_config_value(global_config, backup_config, table_config, key='baz')
        'global_baz'

        And finally, if there's no value at any level, use the default.
        >>> fallback_config_value(global_config, backup_config, table_config, key='random', default=4)
        4
    """
    for config in reversed(configs):
        try:
            return config[key]
        except KeyError:
            pass
    return default
