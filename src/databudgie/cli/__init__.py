# flake8: noqa
from databudgie.cli import backup, base, restore

base.cli.add_command(backup.backup)
base.cli.add_command(restore.restore)


def run():
    base.cli()
