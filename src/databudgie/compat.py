# flake8: noqa

import sys

# Keep TypedDict import idiom DRY, other modules can import from here.
if sys.version_info >= (3, 8):
    from typing import TypedDict  # pragma: no cover
else:
    from typing_extensions import TypedDict
