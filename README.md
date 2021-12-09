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

1. Dumping postgres query results to a CSV file in an S3 bucket.
1. Restoring a CSV file from an S3 bucket into a postgres table.

### Backup

```
$ databudgie [--strict] backup
```

The backup command will query a postgres database specified by the `backup.url` connection string. databudgie will then iterate over `backup.tables`, run the queries against the database, and save the results to CSVs in the S3 bucket and path defined by the `.location` options. For `public.product` below, the file `s3://my-s3-bucket/databudgie/dev/public.product/2021-04-26T09:00:00.csv` will be created (with the timestamp matching the current date and time).

The name under `backup.tables.<NAME>` does not need to match the database in any manner. This value is only used for the `${ref:...}` annotations.

The `--strict` option will cause databudgie to exit if it encounters an error backing up a specific table, otherwise it will attempt to proceed to other tables.

Sample backup configuration:

```yml
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

## Restore

```
$ databudgie [--strict] restore
```

The restore command will download files from S3 and restore them into the database. databudgie will iterate over the `restore.tables` and insert the CSV contents into the tables in order of appearance.

The column headers in the CSV will be used to match the contents of the file to the columns in the table. This allows for leaving columns with default values unset if you are restoring data to a different table than which it was copied from.

```yml
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

## Manifests

Create manifest tables in your database using the Mixins provided by `databudgie.manifest`:

```py
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from databudgie.manifest import DatabudgieManifestMixin

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class DatabudgieManifest(Base, DatabudgieManifestMixin):
    __tablename__ = "databudgie_manifest"
```

Add manifest config options to your `backup` and `restore` sections:

```yml
backup:
  manifest: public.databudgie_manifest
```

Both the `backup` and `restore` commands accept a `--backup-id` or `--restore-id` option to continue a transaction which may have previously crashed in progress. Tables which already have manifest entries for the transaction id will be skipped.

## Configuration

The config is interpretted via [Configly](https://github.com/schireson/configly), so you can use env var interpolation like so:

```yml
environment: <% ENV[ENVIRONMENT, null] %>
```

This is a complete sample configuration:

```yml
environment: production

sentry:
  sentry_dsn: sample@sentry.io/dsn
  version: abcedf

logging:
  enabled: true
  level: INFO

s3: # used to access the bucket where CSVs will be uploaded
  aws_access_key_id: abcdefghijlkmnopq
  aws_secret_access_key: abcdefghijlkmnopqabcdefghijlkmnopq
  profile: databudgie-prod
  region: us-east-1

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
      location: ${ref:backup.tables."public.product".location}
      # Use referenced value from elsewhere in the config ^
```

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

### Config templating

The following format specifiers have been implemented for referencing non-static
data in config:

| Name  | Example                                          | Description                                                                                                                            |
| ----- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| table | query: select \* from {table}                    | Templates the referenced table name into "query"'s value                                                                               |
| ref   | location: {ref:backup.tables[public.*].location} | Templates the value retrieved by following the config traversal from backup -> tables -> public.\* -> location into "location"'s value |

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md).
