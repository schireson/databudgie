import contextlib
import io
import os
from typing import Optional, Tuple

from databudgie.output import Console, default_console
from databudgie.s3 import is_s3_path, S3Location


@contextlib.contextmanager
def capture_failures(ignore=(), strict=False, console: Console = default_console):
    """Prevent exceptions from interrupting execution.

    Examples:
        This exception is captured and not propogated:
        >> with capture_failures():
        ...     raise Exception('foo')

        This exception is raised:
        >> with capture_failures(strict=True): # doctest: +IGNORE_EXCEPTION_DETAIL
        ...     raise Exception('foo')
        Traceback (most recent call last):
        Exception: foo

        This exception is also raised:
        >> with capture_failures(ignore=Exception): # doctest: +IGNORE_EXCEPTION_DETAIL
        ...     raise Exception('foo')

    """
    try:
        yield
    except ignore:
        raise
    except Exception as e:
        console.exception(e)
        if strict:
            raise


@contextlib.contextmanager
def wrap_buffer(buffer: io.BytesIO):
    wrapper = io.TextIOWrapper(buffer)
    yield wrapper
    wrapper.detach()
    buffer.seek(0)


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


def join_paths(*components: Optional[str]) -> str:
    real_components = [c for c in components if c is not None]

    if not real_components:
        return ""

    if len(real_components) == 1:
        return real_components[0]

    bucket = None
    normalized_components = []
    for index, c in enumerate(real_components):
        normalized_c = c
        if is_s3_path(c):
            s3_location = S3Location(c)
            if bucket is None:
                bucket = s3_location.bucket
            else:
                if bucket != s3_location.bucket:
                    raise ValueError(f"Path contains two different buckets: {bucket}, {s3_location.bucket}")
            normalized_c = s3_location.key
        else:
            if index != 0:
                normalized_c = c.lstrip("/")

        normalized_components.append(normalized_c)

    if bucket:
        normalized_components.insert(0, f"s3://{bucket}")

    return os.path.join(*normalized_components)
