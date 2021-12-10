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
