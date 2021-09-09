from configly import Config

from databudgie.config import substitute_locations


def test_substitute_locations():
    config = Config(
        {
            "backup": {"tables": {"public.ad_facebook": {"location": "s3://media-activation-db-dump/databudgie/dev/"}}},
            "restore": {"tables": {"facebook.ad": {"location": "${location:public.ad_facebook}"}}},
        }
    )

    new_tables = substitute_locations(targets=config.restore.tables, sources=config.backup.tables)

    assert (
        new_tables["facebook.ad"]["location"] == "s3://media-activation-db-dump/databudgie/dev/public.ad_facebook.csv"
    )
