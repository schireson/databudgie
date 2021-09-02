import os
from unittest import mock

import databudgie


@mock.patch.dict(os.environ, {"ENVIRONMENT": "chill"})
def test_config_example():
    config = databudgie.get_config()

    assert "chill" == config.environment
