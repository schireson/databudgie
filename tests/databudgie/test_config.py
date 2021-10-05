from unittest.mock import call, patch

import pytest
from configly import Config

from databudgie.config import populate_refs, pretty_print


def test_populate_refs():
    sample_config = Config(
        {
            "backup": {
                "tables": {
                    "public.customer": {"location": "s3://sample-bucket/databudgie/test/public.customer.csv"},
                    "public.product": {"location": '${ref:restore.tables."generic.product".location}'},
                }
            },
            "restore": {
                "tables": {
                    "generic.customer": {"location": '${ref:backup.tables."public.customer".location}'},
                    "generic.product": {"location": "s3://sample-bucket/databudgie/test/generic.product.csv"},
                },
            },
        }
    )

    populated_config: Config = populate_refs(sample_config)

    assert (
        populated_config.restore.tables["generic.customer"].location
        == "s3://sample-bucket/databudgie/test/public.customer.csv"
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
                    "public.customer": {"location": "s3://sample-bucket/databudgie/test/public.customer.csv"},
                    "public.product": {"location": '${ref:restore.tables."generic.product".location}'},
                }
            },
            "restore": {"tables": {}},
        }
    )

    with pytest.raises(KeyError):
        populate_refs(incomplete_config)


@patch("databudgie.config.print")
def test_pretty_print(mock_print, sample_config):
    output = [
        "backup:",
        "  tables:",
        "    public.store:",
        "      location: s3://sample-bucket/databudgie/test/public.store.csv",
        "      query: select * from public.store",
        "    public.customer:",
        "      location: s3://sample-bucket/databudgie/test/public.customer.csv",
        "      query: select * from public.customer",
        "restore:",
        "  tables:",
        "    public.store:",
        "      location: s3://sample-bucket/public.store.csv",
        "      strategy: use_latest",
        "    public.product:",
        "      location: s3://sample-bucket/public.product.csv",
        "      strategy: use_latest",
    ]

    pretty_print(sample_config)

    mock_print.assert_has_calls([call(line) for line in output])
