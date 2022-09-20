# Manifests

The "manifest" table is an optional feature of Databudgie. It will record an audit
log of backup/restore operations that are performed.

Add manifest config options to your `backup` and `restore` sections:

```yaml
backup:
  manifest: public.databudgie_manifest
```

Both the `backup` and `restore` commands accept a `--backup-id` or `--restore-id` option to continue a transaction which may have previously crashed in progress. Tables which already have manifest entries for the transaction id will be skipped.

We provide a convenience function to automatically define the manifest table on your
metadata (which you can wrap in a declarative model, if you need to), so that, for
example alembic can automatically create it for you.

Alternatively, you can minimally define a table with (at least) the columns:

- transaction (Integer)
- action (String)
- table (String)
- file_path (String)

```python
from sqlalchemy import MetaData

from databudgie.manifest import create_manifest_table

metadata = MetaData()
manifest_table = create_manifest_table(metadata, 'databudgie_manifest')

# alternatively

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
manifest_table = create_manifest_table(Base.metadata, 'databudgie_manifest')

# even more alternatively

class DatabudgieManifest(Base):  # type: ignore
    __table__ = create_manifest_table(Base.metadata, tablename="databudgie_manifest")
```
