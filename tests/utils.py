import csv
import gzip
import io
import logging
from pathlib import Path
from typing import List

from botocore.exceptions import ClientError
from mypy_boto3_s3.service_resource import S3ServiceResource

from databudgie.s3 import is_s3_path, S3Location
from databudgie.utils import wrap_buffer

log = logging.getLogger(__name__)

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


def get_file_buffer(filename, s3_resource=None):
    buffer = io.BytesIO()

    if is_s3_path(filename):
        assert s3_resource
        location = S3Location(filename)
        uploaded_object = s3_resource.Object("sample-bucket", location.key)

        try:
            uploaded_object.download_fileobj(buffer)
        except ClientError:
            log.info(str(list(s3_resource.Bucket("sample-bucket").objects.all())))

            raise
    else:
        try:
            with open(filename, "rb") as f:
                buffer.write(f.read())
        except FileNotFoundError:
            # For better error messaging, if the file doesnt exist
            path = Path(filename)
            suffix = path.suffix
            parent = path.parent
            log.info(list(parent.glob(f"*.{suffix}")))

            raise

    buffer.seek(0)

    return buffer
