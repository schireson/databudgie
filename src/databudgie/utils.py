import contextlib
import io
import urllib.parse
from typing import Tuple

from setuplog import log


@contextlib.contextmanager
def capture_failures(ignore=(), strict=False):
    """Prevent exceptions from interrupting execution.

    Examples:
        This exception is captured and not propogated:
        >>> with capture_failures():
        ...     raise Exception('foo')

        This exception is raised:
        >>> with capture_failures(strict=True): # doctest: +IGNORE_EXCEPTION_DETAIL
        ...     raise Exception('foo')
        Traceback (most recent call last):
        Exception: foo

        This exception is also raised:
        >>> with capture_failures(ignore=Exception): # doctest: +IGNORE_EXCEPTION_DETAIL
        ...     raise Exception('foo')
        Traceback (most recent call last):
        Exception: foo

    """
    try:
        yield
    except ignore:
        raise
    except Exception as err:
        if strict:
            raise
        log.info(err, exc_info=True)


@contextlib.contextmanager
def wrap_buffer(buffer: io.BytesIO):
    wrapper = io.TextIOWrapper(buffer)
    yield wrapper
    wrapper.detach()
    buffer.seek(0)


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


def parse_table(table: str) -> Tuple[str, str]:
    """Split a schema-qualified table name into two parts.

    Examples:
        >>> parse_table("myschema.foo")
        ('myschema', 'foo')

        >>> parse_table("bar")
        ('public', 'bar')

        >>> parse_table("...")  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValueError: Invalid table name: ...
    """
    parts = table.split(".")

    if len(parts) == 1:
        schema = "public"
        table = parts[0]
    elif len(parts) == 2:
        schema, table = parts
    else:
        raise ValueError(f"Invalid table name: {table}")

    return schema, table