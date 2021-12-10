from unittest.mock import call, patch

from databudgie.config import pretty_print


@patch("databudgie.config.print")
def test_pretty_print(mock_print, sample_config):
    output = [
        "backup:",
        "  tables:",
        "    public.store:",
        "      location: s3://sample-bucket/databudgie/test/public.store",
        "      query: select * from public.store",
        "    public.customer:",
        "      location: s3://sample-bucket/databudgie/test/public.customer",
        "      query: select * from public.customer",
        "restore:",
        "  tables:",
        "    public.store:",
        "      location: s3://sample-bucket/public.store",
        "      strategy: use_latest_filename",
        "    public.product:",
        "      location: s3://sample-bucket/public.product",
        "      strategy: use_latest_metadata",
    ]

    pretty_print(sample_config)

    mock_print.assert_has_calls([call(line) for line in output])
