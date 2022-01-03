from configly import Config


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
