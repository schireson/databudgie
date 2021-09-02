import pytest
from configly import Config

from databudgie.config import populate_refs


def test_populate_refs():
    sample_config = Config(
        {
            "backup": {
                "tables": {
                    "public.ad_generic": {"location": "s3://sample-bucket/databudgie/test/public.ad_generic.csv"},
                    "public.product": {"location": '${ref:restore.tables."generic.product".location}'},
                }
            },
            "restore": {
                "tables": {
                    "generic.ad": {"location": '${ref:backup.tables."public.ad_generic".location}'},
                    "generic.product": {"location": "s3://sample-bucket/databudgie/test/generic.product.csv"},
                },
            },
        }
    )

    populated_config: Config = populate_refs(sample_config)

    assert (
        populated_config.restore.tables["generic.ad"].location
        == "s3://sample-bucket/databudgie/test/public.ad_generic.csv"
    )

    assert (
        populated_config.backup.tables["public.product"].location
        == "s3://sample-bucket/databudgie/test/generic.product.csv"
    )


def test_bad_ref():
    incomplete_config = Config(
        {
            "backup": {
                "tables": {
                    "public.ad_generic": {"location": "s3://sample-bucket/databudgie/test/public.ad_generic.csv"},
                    "public.product": {"location": '${ref:restore.tables."generic.product".location}'},
                }
            },
            "restore": {"tables": {}},
        }
    )

    with pytest.raises(KeyError):
        populate_refs(incomplete_config)
