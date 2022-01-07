![databudgie](databudgie.png)

# databudgie

standalone library/cli tool for database backup/restore

## Installation

```bash
$ poetry add databudgie
 OR
$ pip install databudgie --index-url "https://artifactory.schireson.com/artifactory/api/pypi/pypi/simple"
```

## Usage

databudgie has two primary functions:

1. Dumping postgres query results to a CSV file (optionally in an S3 bucket).
1. Restoring a CSV file (optionally from an S3 bucket) into a postgres table.

## Backup

```bash
$ databudgie [--strict] backup
```

The backup command will query a postgres database specified by the `backup.url` connection string. databudgie will then iterate over `backup.tables`, run the queries against the database, and save the results to CSVs and path defined by the `.location` options. For `public.product` below, the file `s3://my-s3-bucket/databudgie/dev/public.product/2021-04-26T09:00:00.csv` will be created (with the timestamp matching the current date and time).

The `--strict` option will cause databudgie to exit if it encounters an error backing up a specific table, otherwise it will attempt to proceed to other tables.

Sample backup configuration:

```yaml
backup:
  url: postgresql://postgres:postgres@localhost:5432/postgres
  manifest: public.databudgie_manifest
  tables:
    public.product:
      location: s3://my-s3-bucket/databudgie/public.product
      query: "select * from public.product where store_id = 4"
    public.sales:
      location: s3://my-s3-bucket/databudgie/public.sales
      query: "select * from public.sales where store_id = 4"
```

### DDL

By default, `databudgie` only backs up data. If the DDL structure of the database is
also expected to be backed up, there is an optional `ddl` section of the restore
config that can be supplied.

```yaml
backup:
  ddl:
    enabled: true
    location: s3://bucket/ddl

  tables:
    public.*:
      location: s3://bucket/{table}
```

The above fragment represents the available options and their defaults.

- `restore.ddl.enabled`: When true, backs up the structures (schemas, tables, etc).

- `restore.ddl.location`: The root location at which to store the ddl.

  Note that table-specific ddl composes with ddl root `location` with the table-specific
  `location` value, to determine the absolute path. Given the above example, you would get
  `s3://bucket/ddl/public.tablename` (in addition to the `s3://bucket/public.tablename` for
  the actual data).

## Restore

```bash
$ databudgie [--strict] restore
```

The restore command will download files and restore them into the database. databudgie will iterate over the `restore.tables` and insert the CSV contents into the tables in order of appearance.

The column headers in the CSV will be used to match the contents of the file to the columns in the table. This allows for leaving columns with default values unset if you are restoring data to a different table than which it was copied from.

```yaml
restore:
  url: postgresql://postgres:postgres@localhost:5432/postgres
  manifest: public.databudgie_manifest
  tables:
    public.product:
      location: s3://my-s3-bucket/databudgie/public.product
      strategy: use_latest_filename
      truncate: true
    public.sales:
      location: s3://my-s3-bucket/databudgie/public.sales
      strategy: use_latest_filename
      truncate: true
```

### DDL

By default, `databudgie` assumes the target tables already exist. If the DDL
structure of the database is also expected to be restored, there is an optional
`ddl` section of the restore config that can be supplied.

```yaml
restore:
  ddl:
    enabled: false
    clean: false
    location: s3://bucket/ddl

  tables:
    public.*:
      location: s3://bucket/{table}
```

The above fragment represents the available options and their defaults.

- `restore.ddl.enabled`: When true, restores the backed up structures (schemas, tables, etc),
  dropping tables if they already exist.

  This **can** result in problems with sufficiently complex foreign key relationships and
  existing data.

- `restore.ddl.clean`: Drops the target database, recreates it, and restores into the new database
  instead.

  To avoid the aforementioned issues with data/table complexity, starting with a known-empty
  database can be a simpler alternative, especially when the backups are of a set of
  self-contained set of structures.

- `restore.ddl.location`: The root location at which to look for the ddl.

  Note that table-specific ddl composes with ddl root `location` with the table-specific
  `location` value, to determine the absolute path. Given the above example, you would get
  `s3://bucket/ddl/public.tablename` (in addition to the `s3://bucket/public.tablename` for
  the actual data).

## Compression

Compression can be enabled in both the backup and restore config sections.

```yml
backup:
  compression: gzip

# or

restore:
  compression: gzip
```

Additionally, it can be enabled (or disabled) on a per-`table`-section basis. The
table-specific config will override the more global config value, if given.

Currently suported compression options are:

- gzip

This automatically appends the compression file extension to the backup files
(i.e. gz for gzip), and will only work correctly if both the backup side and
restore side agree on the value of the `compression` key.

## Manifests

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

## Configuration

The config is interpreted via [Configly](https://github.com/schireson/configly), so you can use env var interpolation like so:

```yaml
environment: <% ENV[ENVIRONMENT, null] %>
```

This is a complete sample configuration:

```yaml
environment: production

logging:
  enabled: true
  level: INFO

backup: # configuration for CSV sources
  url: postgresql://postgres:postgres@localhost:5432/postgres
  manifest: public.databudgie_manifest
  tables:
    public.product:
      query: "select * from public.product where store_id = 4"
      location: s3://my-s3-bucket/databudgie/dev/public.product

restore: # configuration for CSV restore targets
  url: postgresql://postgres:postgres@localhost:5432/postgres
  manifest: public.databudgie_manifest
  tables:
    public.product:
      strategy: use_latest_filename
      location: s3://my-s3-bucket/databudgie/dev/public.product
```

### Tables

The `tables` key above can be either a mapping or a list. The mapping version
(as exemplified above), is equivalent to a supplied `name` key.

```yaml
backup:
  tables:
    - name: public.product
      query: ...

# is equivalent to

backup:
  tables:
    public.product:
      query: ...
```

### Paths

Paths, defined as `location:` keys in the config, use the common URI protocols
for dynamically determining (on a per path basis) what protocol to use for the
backup/restore of that path.

Currently two path "kinds" are supported:

- S3: An S3 path is determined if the path is prefixed with `s3://`, such as
  `s3://bucket/path/to/folder`. Such paths will automatically read/write to S3.
  The common AWS cli environment variables (`AWS_PROFILE`, `AWS_REGION`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_ACCESS_KEY_ID`, etc) are automatically read,
  but configuration can also be included under the `s3:` namespace in your
  given config.

  ```
  s3: # used to access the bucket where CSVs will be uploaded
    aws_access_key_id: abcdefghijlkmnopq
    aws_secret_access_key: abcdefghijlkmnopqabcdefghijlkmnopq
    profile: databudgie-prod
    region: us-east-1
  ```

- local: A local path is any path which is not automatically recognized as a
  different style of path, such as `path/to/folder`. For backups, if the path
  leading up to the leaf folder does not yet exist, it will be automatically
  created.

### Config templating

The following format specifiers have been implemented for referencing non-static
data in config:

| Name  | Example                                          | Description                                                                                                                            |
| ----- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| table | query: select \* from {table}                    | Templates the referenced table name into "query"'s value                                                                               |
| ref   | location: {ref.backup.tables[public.*].location} | Templates the value retrieved by following the config traversal from backup -> tables -> public.\* -> location into "location"'s value |

### Globbing

Using common globbing rules:

| Pattern | Meaning                          |
| ------- | -------------------------------- |
| \*      | matches everything               |
| ?       | matches any single character     |
| [seq]   | matches any character in seq     |
| [!seq]  | matches any character not in seq |

```yml
backup:
  tables:
    public.*:
      query: "select * from {table}"
      location: s3://my-s3-bucket/databudgie/dev/{table}
restore:
  tables:
    public.*:
      query: "select * from {table}"
      location: s3://my-s3-bucket/databudgie/dev/{table}
```

This expands the definition of matched tables in both backup/restore.

### Sentry

Sentry configuration can optionally be included, so that any errors in uses of
the databudgie CLI are reported. Note this has no effect when using databudgie
as a library (where you should instead set up sentry in your application).

```yaml
sentry:
  sentry_dsn: sample@sentry.io/dsn
  version: abcedf
```

## Upgrading from Version 1.x

Databudgie v1 uses single-file backups per table instead of folders per table. Example:

```
v1: public.my_table --> s3://bucket/path/to/public.my_table.csv
v2: public.my_table --> s3://bucket/path/to/public.my_table/<timestamp>.csv
```

Existing v1 backups should be safely ignored by v2.

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md).
