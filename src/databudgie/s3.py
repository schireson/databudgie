import urllib.parse
from typing import Optional, TYPE_CHECKING

from configly import Config

from databudgie.config import normalize_table_config

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource


def optional_s3_resource(config: Config) -> Optional["S3ServiceResource"]:
    if config_uses_s3(config):
        return s3_resource(config)
    return None


def s3_resource(config) -> "S3ServiceResource":
    try:
        import boto3
    except ImportError:
        raise RuntimeError('Use of S3 requires the "s3" python extra')

    # Boto loads all config as environment variables by default, this config
    # section can be entirely optional.
    s3_config = config.get("s3", {})
    session = boto3.session.Session(
        aws_access_key_id=s3_config.get("aws_access_key_id"),
        aws_secret_access_key=s3_config.get("aws_secret_access_key"),
        profile_name=s3_config.get("profile"),
        region_name=s3_config.get("region"),
    )

    s3: "S3ServiceResource" = session.resource("s3")
    return s3


def config_uses_s3(config: Config):
    for namespace in [config.get("backup"), config.get("restore")]:
        if not namespace:
            continue

        for _, table_config in normalize_table_config(namespace["tables"]):
            location = table_config["location"]
            if is_s3_path(location):
                return True

    return False


def is_s3_path(path: str):
    return path.startswith("s3://")


class S3Location:
    """Easily parse an S3 URL into Bucket and Key.

    Example:
        >>> loc = S3Location("s3://my-s3-bucket/raw_upload/sample.csv")
        >>> loc.bucket
        'my-s3-bucket'
        >>> loc.key
        'raw_upload/sample.csv'
    """

    def __init__(self, url: str):
        self._parsed = urllib.parse.urlparse(url)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        return self._parsed.path.lstrip("/")
