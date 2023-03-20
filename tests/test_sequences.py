import io
import json

import pytest
from mypy_boto3_s3.service_resource import S3ServiceResource
from sqlalchemy import text

from databudgie.backup import backup_all
from databudgie.config import RootConfig
from databudgie.restore import restore_all
from tests.utils import s3_config


def mock_s3_json(s3_resource: S3ServiceResource, key: str, data: dict):
    bucket = s3_resource.Bucket("sample-bucket")
    buffer = io.BytesIO(json.dumps(data).encode("utf-8"))
    bucket.put_object(Key=key, Body=buffer)


table_config = {
    "tables": {
        "public.product": {
            "location": "s3://sample-bucket/{table}",
        },
    }
}


def test_backup_without_sequences(pg, s3_resource):
    config = RootConfig.from_dict(
        {
            **table_config,
            **s3_config,
            "sequences": False,
            "strict": True,
        }
    )

    backup_all(pg, config.backup)

    all_objects = list(s3_resource.Bucket("sample-bucket").objects.all())
    assert len(all_objects) == 1
    assert all_objects[0].key == "public.product/2021-04-26T09:00:00.csv"


# Test the default sequence value of True, in addition to the explicitly set True value.
@pytest.mark.parametrize("sequence_config", ({}, {"sequences": True}))
def test_backup_with_sequences(pg, s3_resource, sequence_config, mf):
    config = RootConfig.from_dict(
        {
            **table_config,
            **s3_config,
            **sequence_config,
            "strict": True,
        }
    )

    mf.product.new()
    mf.product.new()
    mf.product.new()

    backup_all(pg, config.backup)

    all_objects = list(s3_resource.Bucket("sample-bucket").objects.all())
    assert len(all_objects) == 2
    assert all_objects[0].key == "public.product/2021-04-26T09:00:00.csv"
    assert all_objects[1].key == "public.product/sequences/2021-04-26T09:00:00.json"

    content = all_objects[1].get()["Body"].read()
    sequences = json.loads(content)

    assert sequences == {"product_id_seq": 3}


def test_restore_without_sequences(pg, s3_resource):
    config = RootConfig.from_dict(
        {
            **table_config,
            **s3_config,
            "sequences": False,
            "strict": True,
        }
    )

    mock_s3_json(s3_resource, "public.product/sequences/2021-04-26T09:00:00.json", {"product_id_seq": 91})

    restore_all(pg, config.restore)

    sequence_value = pg.execute(text("SELECT LAST_VALUE from product_id_seq")).scalar()
    assert sequence_value == 1


# Test the default sequence value of True, in addition to the explicitly set True value.
@pytest.mark.parametrize("sequence_config", ({}, {"sequences": True}))
def test_restore_with_sequences(pg, s3_resource, sequence_config, mf):
    config = RootConfig.from_dict(
        {
            **table_config,
            **s3_config,
            **sequence_config,
            "strict": True,
        }
    )

    mock_s3_json(s3_resource, "public.product/sequences/2021-04-26T09:00:00.json", {"product_id_seq": 91})

    restore_all(pg, config.restore)

    sequence_value = pg.execute(text("SELECT LAST_VALUE from product_id_seq")).scalar()
    assert sequence_value == 91

    product = mf.product.new()
    assert product.id == 92
