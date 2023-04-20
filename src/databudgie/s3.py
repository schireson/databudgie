from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource

    from databudgie.config import BackupConfig, RestoreConfig, S3Config


def optional_s3_resource(config: BackupConfig | RestoreConfig) -> S3ServiceResource | None:
    if config_uses_s3(config):
        return s3_resource(config.s3)
    return None


def s3_resource(config: S3Config | None = None) -> S3ServiceResource:
    try:
        import boto3
    except ImportError:
        raise RuntimeError('Use of S3 requires the "s3" python extra')

    # Boto loads all config as environment variables by default, this config
    # section can be entirely optional.
    if not config:
        session = boto3.session.Session()
    else:
        session = boto3.session.Session(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            profile_name=config.profile,
            region_name=config.region,
        )

    s3: S3ServiceResource = session.resource("s3")
    return s3


def config_uses_s3(config: BackupConfig | RestoreConfig):
    if config.root_location and is_s3_path(config.root_location):
        return True

    for table_config in config.tables:
        if is_s3_path(table_config.location):
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
