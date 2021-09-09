import contextlib
import urllib.parse
from os import path

from setuplog import log


@contextlib.contextmanager
def capture_failures(ignore=(), strict=False):
    try:
        yield
    except ignore:
        raise
    except Exception as err:
        if strict:
            raise
        log.info(err, exc_info=True)


class S3Location:
    """Easily parse an S3 URL into Bucket and Key.

    Example:
        >>> loc = S3Location("s3://media-science-platform-automation/raw_upload/sample.csv")
        >>> loc.bucket
        'media-science-platform-automation'
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


def csv_path(path_prefix: str, table_name: str) -> str:
    return path.join(path_prefix, f"{table_name}.csv")
