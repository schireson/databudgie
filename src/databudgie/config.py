from __future__ import annotations

import abc
import typing
from dataclasses import dataclass, field, fields
from typing import Any

from databudgie.utils import join_paths

T = typing.TypeVar("T", "BackupTableConfig", "RestoreTableConfig")
F = typing.TypeVar("F")


class ConfigError(ValueError):
    """Raise for invalid or incomplete config."""


class ConfigStack:
    def __init__(self, *configs: dict):
        self.configs: tuple[dict, ...] = configs

    def __getitem__(self, key):
        for config in self.configs:
            if config and key in config:
                return config[key]
        raise KeyError(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        for config in self.configs:
            if config and key in config:
                return True
        return False

    def push(self, config: dict):
        return ConfigStack(config, *self.configs)


class Config(metaclass=abc.ABCMeta):
    def to_dict(self) -> dict:
        result = {}
        for f in fields(self):
            v = getattr(self, f.name)
            if isinstance(v, Config):
                value: Any = v.to_dict()
            elif isinstance(v, list):
                value = [v.to_dict() if isinstance(v, Config) else v for v in v]
            elif isinstance(v, dict):
                value = {k: v.to_dict() if isinstance(v, Config) else v for k, v in v.items()}
            else:
                value = v

            result[f.name] = value
        return result


def from_partial(cls: typing.Callable[..., F], **kwargs) -> F:
    """Create a new instance of cls with the given kwargs.

    This is useful for creating a new instance of a class with only some of the
    fields set.
    """
    cleaned_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return cls(**cleaned_kwargs)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Optional Configs which have default values built-in
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


@dataclass
class DDLConfig(Config):
    enabled: bool = False
    location: str = "backups/ddl"
    clean: bool = False
    strategy: str = "use_latest_filename"

    @classmethod
    def from_dict(cls, ddl_config: dict | bool, root_location: str | None = None):
        if isinstance(ddl_config, bool):
            expanded_ddl_config: dict[str, Any] = {"enabled": ddl_config}
        else:
            expanded_ddl_config = ddl_config

        location = compose_root_location(root_location, expanded_ddl_config.get("location"), default="backups/ddl")

        # Splat into a new dict so we can override `location` without mutating
        # the original input (which may be re-read later in config parsing)
        final_ddl_config = {**expanded_ddl_config, "location": location}

        return from_partial(cls, **final_ddl_config)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Core configuration models
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


@dataclass
class RootConfig(Config):
    backup: BackupConfig
    restore: RestoreConfig

    @classmethod
    def from_dict(cls, raw_config: dict):
        config = ConfigStack(raw_config)
        return cls.from_stack(config)

    @classmethod
    def from_stack(cls, stack: ConfigStack):
        backup_config = stack.get("backup", {})
        backup = BackupConfig.from_stack(stack.push(backup_config))

        restore_config = stack.get("restore", {})
        restore = RestoreConfig.from_stack(stack.push(restore_config))
        return cls(backup=backup, restore=restore)


@dataclass
class TableParentConfig(typing.Generic[T], Config):
    tables: list[T]
    connections: dict[str, Connection]
    ddl: DDLConfig

    connection: Connection | None = None
    manifest: str | None = None

    s3: S3Config | None = None
    sentry: SentryConfig | None = None
    root_location: str | None = None
    adapter: str | None = None

    @classmethod
    @abc.abstractmethod
    def get_child_class(cls):
        pass

    @classmethod
    def from_stack(cls, stack: ConfigStack):
        connection = Connection.from_raw(stack.get("url") or stack.get("connection"), name="default")
        root_location = stack.get("root_location")

        tables_config: list = normalize_table_config(stack.get("tables", []))
        table_class = cls.get_child_class()
        tables = [table_class.from_stack(stack.push(tbl_conf), root_location) for tbl_conf in tables_config]

        # manifest defauls to None
        manifest: str | None = stack.get("manifest")

        ddl = DDLConfig.from_dict(stack.get("ddl", {}), root_location)

        # Optional integration configs
        s3 = S3Config.from_dict(stack.get("s3"))
        sentry = SentryConfig.from_dict(stack.get("sentry"))

        adapter = stack.get("adapter")

        connections = Connection.from_collection(stack.get("connections"))

        return cls(
            connection=connection,
            tables=tables,
            manifest=manifest,
            s3=s3,
            sentry=sentry,
            ddl=ddl,
            root_location=root_location,
            adapter=adapter,
            connections=connections,
        )


@dataclass
class Connection(Config):
    name: str
    url: str | dict

    @classmethod
    def from_raw(cls, raw: str | dict | None, *, name: str | None = None):
        if raw is None:
            return None

        if isinstance(raw, str):
            if name is None:
                raise ConfigError(f"Connection '{raw}' requires a name")
            return cls(name="default", url=raw)

        if name is None:
            raise ConfigError(f"Connection '{raw}' requires a name")

        if "url" in raw:
            url = raw["url"]
        else:
            url = {k: v for k, v in raw.items() if k != "name"}

        return cls(name=name or "default", url=url)

    @classmethod
    def from_collection(cls, collection: list | dict | None) -> dict[str, Connection]:
        if collection is None:
            return {}

        if isinstance(collection, list):
            connections = []
            names = set()
            for c in collection:
                connection = Connection.from_raw(c, name=c.get("name"))
                assert connection is not None

                if connection.name in names:
                    raise ConfigError(f"Detected more than one connection with the same name: {connection.name}")
                names.add(connection.name)
                connections.append(connection)

            return {c.name: c for c in connections}

        return {k: Connection.from_raw(c, name=k) for k, c in collection.items()}


@dataclass
class BackupTableConfig(Config):
    name: str
    location: str = "backups/{table}"
    query: str = "select * from {table}"
    compression: str | None = None
    exclude: list = field(default_factory=list)
    ddl: bool = True
    sequences: bool = True
    data: bool = True
    follow_foreign_keys: bool = False
    strict: bool = False

    @classmethod
    def from_stack(cls, stack: ConfigStack, root_location: str | None = None):
        ddl = stack.get("ddl", True)
        if isinstance(ddl, dict):
            ddl = ddl["enabled"]

        location = compose_root_location(root_location, stack.get("location"), default="backups/{table}")

        return from_partial(
            cls,
            name=stack.get("name"),
            location=location,
            query=stack.get("query"),
            compression=stack.get("compression"),
            exclude=stack.get("exclude"),
            sequences=stack.get("sequences", True),
            data=stack.get("data", True),
            ddl=stack.get("ddl", True),
            follow_foreign_keys=stack.get("follow_foreign_keys", False),
            strict=stack.get("strict", False),
        )


@dataclass
class BackupConfig(TableParentConfig[BackupTableConfig]):
    @classmethod
    def get_child_class(cls):
        return BackupTableConfig


@dataclass
class RestoreTableConfig(Config):
    name: str
    location: str = "backups/{table}"
    strategy: str = "use_latest_filename"
    compression: str | None = None
    exclude: list = field(default_factory=list)
    ddl: bool = True
    sequences: bool = True
    truncate: bool = False
    data: bool = True
    follow_foreign_keys: bool = False
    strict: bool = False

    @classmethod
    def from_stack(cls, stack: ConfigStack, root_location: str | None = None):
        ddl = stack.get("ddl", True)
        if isinstance(ddl, dict):
            ddl = ddl["enabled"]

        location = compose_root_location(root_location, stack.get("location"), default="backups/{table}")

        return from_partial(
            cls,
            name=stack.get("name"),
            location=location,
            strategy=stack.get("strategy"),
            truncate=stack.get("truncate"),
            compression=stack.get("compression"),
            exclude=stack.get("exclude"),
            sequences=stack.get("sequences", True),
            data=stack.get("data", True),
            ddl=stack.get("ddl", True),
            follow_foreign_keys=stack.get("follow_foreign_keys", False),
            strict=stack.get("strict", False),
        )


@dataclass
class RestoreConfig(TableParentConfig[RestoreTableConfig]):
    @classmethod
    def get_child_class(cls):
        return RestoreTableConfig


def normalize_table_config(tables_config: list | dict) -> list:
    """Convert the dict-style table declaration into list style.

    from: {"name": {"location": "backups/{table}"}}
    to: [{"name": "blah", "location": "backups/{table}"}]

    alternatively:
    from: ["name1", "name2"]
    to: [{"name": "name1"}, {"name": "name2"}]
    """
    if isinstance(tables_config, dict):
        return [{"name": k, **v} for k, v in tables_config.items()]

    if isinstance(tables_config, list):
        return [{"name": c} if isinstance(c, str) else c for c in tables_config]

    return tables_config


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Optional Integration configs which do NOT have default values built-in
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


@dataclass
class SentryConfig(Config):
    """Configuration for Sentry integration.

    SentryConfig does not inherit keys from the root config.
    """

    dsn: str | None = None
    environment: str | None = None
    version: str | None = None

    @classmethod
    def from_dict(cls, sentry_config: dict | None) -> SentryConfig | None:
        if sentry_config is None:
            return None

        return cls(
            dsn=sentry_config.get("dsn"),
            environment=sentry_config.get("environment"),
            version=sentry_config.get("version"),
        )


@dataclass
class S3Config(Config):
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    region: str | None = None
    profile: str | None = None

    @classmethod
    def from_dict(cls, s3_config: dict | None) -> S3Config | None:
        if s3_config is None:
            return None
        return cls(
            aws_access_key_id=s3_config.get("aws_access_key_id"),
            aws_secret_access_key=s3_config.get("aws_secret_access_key"),
            region=s3_config.get("region"),
            profile=s3_config.get("profile"),
        )


def compose_root_location(root_location, location, *, default):
    if root_location is None:
        return location or default

    return join_paths(root_location, location or default)
