import click
import sqlalchemy.orm
import strapp.click
import strapp.logging
from configly import Config
from mypy_boto3_s3 import S3ServiceResource
from setuplog import log
from sqlalchemy.orm import Session


def config():
    return Config.from_yaml("config.yml")


def _create_postgres_session(url):
    engine = sqlalchemy.create_engine(url)
    session = sqlalchemy.orm.scoping.scoped_session(sqlalchemy.orm.session.sessionmaker(bind=engine))()
    return session


def backup_db(config):
    return _create_postgres_session(config.backup.url)


def restore_db(config):
    return _create_postgres_session(config.restore.url)


def s3_resource(config) -> S3ServiceResource:
    import boto3

    session = boto3.session.Session(
        aws_access_key_id=config.s3.aws_access_key_id,
        aws_secret_access_key=config.s3.aws_secret_access_key,
        profile_name=config.s3.profile,
        region_name=config.s3.region,
    )
    s3: S3ServiceResource = session.resource("s3")
    return s3


resolver = strapp.click.Resolver(config=config, backup_db=backup_db, restore_db=restore_db, s3_resource=s3_resource)


@resolver.group()
@click.option("-v", "--verbose", count=True, default=0)
@click.option("--strict/--no-strict", is_flag=True, default=None)
def cli(config: Config, verbose: int, strict: bool):
    from databudgie.cli.setup import setup

    setup(config, verbosity=verbose)
    resolver.register_values(verbosity=verbose, strict=strict)


@resolver.command(cli, "backup")
def backup(config: Config, backup_db: Session, s3_resource: S3ServiceResource, strict: bool):
    """Perform backup."""
    from databudgie.backup import backup_all

    log.info("Performing backup! (environment: %s)", config.environment)

    backup_all(backup_db, s3_resource, config.backup.tables, strict=strict)


@resolver.command(cli, "restore")
def restore(config: Config):
    """Perform restore."""

    log.info("Performing restore! (environment: %s)", config.environment)
