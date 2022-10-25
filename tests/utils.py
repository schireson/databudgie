import csv
import gzip
import io
from typing import List

from mypy_boto3_s3.service_resource import S3ServiceResource

from databudgie.utils import wrap_buffer

s3_config = {
    "s3": {
        "aws_access_key_id": "foo",
        "aws_secret_access_key": "foo",
        "region": "foo",
    }
}


def mock_csv(data: List[dict], gzipped=False):
    buffer = io.BytesIO()

    with wrap_buffer(buffer) as wrapper:
        writer: csv.DictWriter = csv.DictWriter(wrapper, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    buffer.seek(0)

    if gzipped:
        return io.BytesIO(gzip.compress(buffer.read()))
    return buffer


def mock_s3_csv(s3_resource: S3ServiceResource, key: str, data: List[dict], gzipped=False):
    bucket = s3_resource.Bucket("sample-bucket")
    buffer = mock_csv(data, gzipped=gzipped)
    bucket.put_object(Key=key, Body=buffer)
