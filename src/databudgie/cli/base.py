import click
import strapp.click
import strapp.logging
from configly import Config


def config():
    return Config.from_yaml("config.yml")


resolver = strapp.click.Resolver(config=config)


@resolver.group()
@click.option("-v", "--verbose", count=True, default=0)
def cli(config: Config, verbose):
    from databudgie.cli.setup import setup

    setup(config, verbosity=verbose)
    resolver.register_values(verbosity=verbose)
