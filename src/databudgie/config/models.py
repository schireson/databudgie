from __future__ import annotations

import abc
import typing
from dataclasses import asdict, dataclass, field
from typing import Optional

T = typing.TypeVar("T", "BackupTableConfig", "RestoreTableConfig")
F = typing.TypeVar("F")


class ConfigStack:
    def __init__(self, *configs):
        self.configs: typing.Tuple[dict] = configs

    def __getitem__(self, key):
        for config in self.configs:
            if config and key in config:
                return config[key]
        return None  # TODO: maybe error

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
        # Create a Backup/Restore config only if the config has a backup/restore section OR tables are declared at the root level.
        backup, restore = None, None

        if "backup" in stack or "tables" in stack:
            backup_config = stack["backup"]
            backup = BackupConfig.from_stack(stack.push(backup_config))

        if "restore" in stack or "tables" in stack:
            restore_config = stack["restore"]
            restore = RestoreConfig.from_stack(stack.push(restore_config))

        return cls(backup=backup, restore=restore)

    def to_dict(self) -> dict:
        return {
            "backup": self.backup.to_dict() if self.backup else None,
            "restore": self.restore.to_dict() if self.restore else None,
        }


@dataclass  # type: ignore
class TableParentConfig(typing.Generic[T], Config):
    url: str
    tables: typing.List[T]

    ddl: DDLConfig
    logging: LoggingConfig
    manifest: Optional[str] = None

    s3: Optional[S3Config] = None
    sentry: Optional[SentryConfig] = None

    @classmethod
    @abc.abstractmethod
    def get_child_class(cls):
        pass

    @classmethod
    def from_stack(cls, stack: ConfigStack):
        url: str = stack["url"]

        tables_config: list = normalize_table_config(stack["tables"])
        table_class = cls.get_child_class()
        try:
            tables = [table_class.from_stack(stack.push(tbl_conf)) for tbl_conf in tables_config]
        except TypeError as err:
            raise ValueError("Must include a `tables` section in the config.") from err

        # manifest defauls to None
        manifest: Optional[str] = stack["manifest"]

        # DDL and Logging have global defaults; also, from_dict requires a non-null dict
        ddl = DDLConfig.from_dict(stack["ddl"] or {})
        logging = LoggingConfig.from_dict(stack["logging"] or {})

        # Optional integration configs
        s3 = S3Config.from_dict(stack["s3"]) if stack["s3"] else None
        sentry = SentryConfig.from_dict(stack["sentry"]) if stack["sentry"] else None

        return cls(
            url=url,
            tables=tables,
            manifest=manifest,
            ddl=ddl,
            s3=s3,
            sentry=sentry,
            logging=logging,
        )

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "tables": [table.to_dict() for table in self.tables],
            "manifest": self.manifest,
            "ddl": self.ddl.to_dict(),
            "logging": self.logging.to_dict(),
            "s3": self.s3.to_dict() if self.s3 else None,
            "sentry": self.sentry.to_dict() if self.sentry else None,
        }


@dataclass
class BackupTableConfig(Config):
    name: str
    location: str = "backups/{table}"
    query: str = "select * from {table}"
    compression: Optional[str] = None
    exclude: list = field(default_factory=list)

    @classmethod
    def from_stack(cls, stack: ConfigStack):
        return from_partial(
            cls,
            name=stack["name"],
            location=stack["location"],
            query=stack["query"],
            compression=stack["compression"],
            exclude=stack["exclude"],
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
    truncate: bool = False
    compression: Optional[str] = None
    exclude: list = field(default_factory=list)

    @classmethod
    def from_stack(cls, stack: ConfigStack):
        return from_partial(
            cls,
            name=stack["name"],
            location=stack["location"],
            strategy=stack["strategy"],
            truncate=stack["truncate"],
            compression=stack["compression"],
            exclude=stack["exclude"],
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
# Optional Configs which have default values built-in
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


@dataclass
class DDLConfig(Config):
    enabled: bool = False
    location: str = "ddl"
    clean: bool = False
    strategy: str = "use_latest_filename"

    @classmethod
    def from_dict(cls, ddl_config: dict):
        return from_partial(cls, **ddl_config)


@dataclass
class LoggingConfig(Config):
    enabled: bool = False
    level: str = "INFO"

    @classmethod
    def from_dict(cls, logging_config: dict):
        return from_partial(cls, **logging_config)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Optional Integration configs which do NOT have default values built-in
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


@dataclass
class SentryConfig(Config):
    """Configuration for Sentry integration.

    SentryConfig does not inherit keys from the root config.
    """

    dsn: str
    environment: str
    version: Optional[str] = None

    @classmethod
    def from_dict(cls, sentry_config: dict):
        return cls(
            dsn=sentry_config["dsn"],
            environment=sentry_config["environment"],
            version=sentry_config.get("version"),
        )


@dataclass
class S3Config(Config):
    aws_access_key_id: str
    aws_secret_access_key: str
    region: str
    profile: Optional[str] = None

    @classmethod
    def from_dict(cls, s3_config: dict):
        return cls(
            aws_access_key_id=s3_config["aws_access_key_id"],
            aws_secret_access_key=s3_config["aws_secret_access_key"],
            region=s3_config["region"],
            profile=s3_config.get("profile"),
        )
