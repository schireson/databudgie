from __future__ import annotations

import abc
import typing
from dataclasses import asdict, dataclass, field
from typing import Any, Optional, Union

from databudgie.utils import join_paths

T = typing.TypeVar("T", "BackupTableConfig", "RestoreTableConfig")
F = typing.TypeVar("F")


class ConfigError(ValueError):
    """Raise for invalid or incomplete config."""


class ConfigStack:
    def __init__(self, *configs: dict):
        self.configs: typing.Tuple[dict, ...] = configs

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
        return asdict(self)


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
    location: str = "ddl"
    clean: bool = False
    strategy: str = "use_latest_filename"

    @classmethod
    def from_dict(cls, ddl_config: Union[dict, bool], root_location: Optional[str] = None):
        if isinstance(ddl_config, bool):
            expanded_ddl_config: dict[str, Any] = {"enabled": ddl_config}
        else:
            expanded_ddl_config = ddl_config

        location = join_paths(root_location, expanded_ddl_config.get("location"))

        # Splat into a new dict so we can override `location` without mutating
        # the original input (which may be re-read later in config parsing)
        final_ddl_config = {**expanded_ddl_config, "location": location}

        return from_partial(cls, **final_ddl_config)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Core configuration models
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


@dataclass
class RootConfig(Config):
    backup: Optional[BackupConfig] = None
    restore: Optional[RestoreConfig] = None

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

    def to_dict(self) -> dict:
        return {
            "backup": self.backup.to_dict() if self.backup else None,
            "restore": self.restore.to_dict() if self.restore else None,
        }


@dataclass
class TableParentConfig(typing.Generic[T], Config):
    url: typing.Union[str, dict]
    tables: typing.List[T]

    ddl: DDLConfig
    manifest: Optional[str] = None

    s3: Optional[S3Config] = None
    sentry: Optional[SentryConfig] = None
    root_location: Optional[str] = None
    adapter: Optional[str] = None

    @classmethod
    @abc.abstractmethod
    def get_child_class(cls):
        pass

    @classmethod
    def from_stack(cls, stack: ConfigStack):
        url: str = stack.get("url")
        root_location = stack.get("root_location")

        tables_config: list = normalize_table_config(stack.get("tables", []))
        table_class = cls.get_child_class()
        tables = [table_class.from_stack(stack.push(tbl_conf), root_location) for tbl_conf in tables_config]

        # manifest defauls to None
        manifest: Optional[str] = stack.get("manifest")

        ddl = DDLConfig.from_dict(stack.get("ddl", {}), root_location)

        # Optional integration configs
        s3 = S3Config.from_dict(stack.get("s3"))
        sentry = SentryConfig.from_dict(stack.get("sentry"))

        adapter = stack.get("adapter")

        return cls(
            url=url,
            tables=tables,
            manifest=manifest,
            s3=s3,
            sentry=sentry,
            ddl=ddl,
            root_location=root_location,
            adapter=adapter,
        )

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "tables": [table.to_dict() for table in self.tables],
            "manifest": self.manifest,
            "ddl": self.ddl.to_dict(),
            "s3": self.s3.to_dict() if self.s3 else None,
            "sentry": self.sentry.to_dict() if self.sentry else None,
            "root_location": self.root_location,
            "adapter": self.adapter,
        }


@dataclass
class BackupTableConfig(Config):
    name: str
    location: str = "backups/{table}"
    query: str = "select * from {table}"
    compression: Optional[str] = None
    exclude: list = field(default_factory=list)
    ddl: bool = True
    sequences: bool = True
    data: bool = True
    follow_foreign_keys: bool = False
    strict: bool = False

    @classmethod
    def from_stack(cls, stack: ConfigStack, root_location: Optional[str] = None):
        ddl = stack.get("ddl", True)
        if isinstance(ddl, dict):
            ddl = ddl["enabled"]

        location = join_paths(root_location, stack.get("location")) or None

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
    compression: Optional[str] = None
    exclude: list = field(default_factory=list)
    ddl: bool = True
    sequences: bool = True
    truncate: bool = False
    data: bool = True
    follow_foreign_keys: bool = False
    strict: bool = False

    @classmethod
    def from_stack(cls, stack: ConfigStack, root_location: Optional[str] = None):
        ddl = stack.get("ddl", True)
        if isinstance(ddl, dict):
            ddl = ddl["enabled"]

        location = join_paths(root_location, stack.get("location"))

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


def normalize_table_config(tables_config: typing.Union[list, dict]) -> list:
    """Convert the dict-style table declaration into list style.

    from: {"name": {"location": "backups/{table}"}}
    to: [{"name": "blah", "location": "backups/{table}"}]

    alternatively:
    from: ["name1", "name2"]
    to: [{"name": "name1"}, {"name": "name2"}]
    """
    if isinstance(tables_config, dict):
        tables_config = [{"name": k, **v} for k, v in tables_config.items()]
    elif isinstance(tables_config, list):
        tables_config = [{"name": c} if isinstance(c, str) else c for c in tables_config]

    return tables_config


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Optional Integration configs which do NOT have default values built-in
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


@dataclass
class SentryConfig(Config):
    """Configuration for Sentry integration.

    SentryConfig does not inherit keys from the root config.
    """

    dsn: Optional[str] = None
    environment: Optional[str] = None
    version: Optional[str] = None

    @classmethod
    def from_dict(cls, sentry_config: Optional[dict]) -> Optional[SentryConfig]:
        if sentry_config is None:
            return None

        return cls(
            dsn=sentry_config.get("dsn"),
            environment=sentry_config.get("environment"),
            version=sentry_config.get("version"),
        )


@dataclass
class S3Config(Config):
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region: Optional[str] = None
    profile: Optional[str] = None

    @classmethod
    def from_dict(cls, s3_config: Optional[dict]) -> Optional[S3Config]:
        if s3_config is None:
            return None
        return cls(
            aws_access_key_id=s3_config.get("aws_access_key_id"),
            aws_secret_access_key=s3_config.get("aws_secret_access_key"),
            region=s3_config.get("region"),
            profile=s3_config.get("profile"),
        )
