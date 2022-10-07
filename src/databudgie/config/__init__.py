import io
from typing import Mapping

from configly import Config
from rich.console import Console
from rich.syntax import Syntax
from ruamel.yaml import YAML

from databudgie.config.models import Config as DatabudgieConfig


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
