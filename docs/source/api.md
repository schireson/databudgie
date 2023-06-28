# API

Databudgie has a simple programmatic API, enabling one to invoke databudgie
directly from python. The CLI interface is a thin wrapper around this API, and
should generally allow much the same functionality and options.

```{eval-rst}
.. autoapimodule:: databudgie.api
   :members: root_config, backup, restore
```

## Example

```python
from databudgie.api import backup, root_config
from sqlalchemy.orm import Session


def perform_backup(pg: Session):
    config = root_config(
        raw_config="""{
            "tables": []
        }""",
    )
    backup(pg, config.backup)
```
