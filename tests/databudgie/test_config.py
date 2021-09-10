import pytest
from configly import Config

from databudgie.config import populate_refs


def test_populate_refs():
    sample_config = Config(
        {
            "backup": {
                "tables": {
                    "public.ad_generic": {"location": "s3://sample-bucket/databudgie/test/public.ad_generic.csv"},
                    "public.line_item": {"location": '${ref:restore.tables."facebook.line_item".location}'},
                }
            },
            "restore": {
                "tables": {
                    "facebook.ad": {"location": '${ref:backup.tables."public.ad_generic".location}'},
                    "facebook.line_item": {"location": "s3://sample-bucket/databudgie/test/facebook.line_item.csv"},
                }
            },
        }
    )

    populated_config: Config = populate_refs(sample_config)

    assert (
        populated_config.restore.tables["facebook.ad"].location
        == "s3://sample-bucket/databudgie/test/public.ad_generic.csv"
    )

    assert (
        populated_config.backup.tables["public.line_item"].location
        == "s3://sample-bucket/databudgie/test/facebook.line_item.csv"
    )


def test_bad_ref():
    incomplete_config = Config(
        {
            "backup": {
                "tables": {
                    "public.ad_generic": {"location": "s3://sample-bucket/databudgie/test/public.ad_generic.csv"},
                    "public.line_item": {"location": '${ref:restore.tables."facebook.line_item".location}'},
                }
            },
            "restore": {"tables": {}},
        }
    )

    with pytest.raises(KeyError):
        populate_refs(incomplete_config)
