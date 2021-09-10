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

The backup command will query a postgres database specified by the `backup.url` connection string. databudgie will then iterate over `backup.tables`, run the queries against the database, and save the results to CSVs in the S3 bucket and path defined by the `.location` options. For `public.ad_facebook` below, the file `s3://my-s3-bucket/databudgie/dev/public.ad_facebook.csv` will be created.

The name under `backup.tables.<NAME>` does not need to match the database in any manner. This value is only used for the `${ref:...}` annotations.

The `--strict` option will cause databudgie to exit if it encounters an error backing up a specific table, otherwise it will attempt to proceed to other tables.

Sample backup configuration:

```yml
backup:
  url: postgresql://postgres:postgres@localhost:5432/postgres
  tables:
    public.ad_facebook:
      query: "select * from public.ad_facebook where advertiser_id = 4"
      location: s3://my-s3-bucket/databudgie/dev/public.ad_facebook.csv
    public.ad_twitter:
      query: "select * from public.ad_twitter where advertiser_id = 4"
      location: s3://my-s3-bucket/databudgie/dev/public.ad_twitter.csv
```

## Restore

TODO

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
  tables:
    public.ad_facebook:
      query: "select * from public.ad_facebook where advertiser_id = 4"
      location: s3://my-s3-bucket/databudgie/dev/public.ad_facebook.csv

restore: # configuration for CSV restore targets
  url: postgresql://postgres:postgres@localhost:5432/postgres
  tables:
    facebook.ad:
      strategy: use_latest
      location: ${ref:backup.tables."public.ad_facebook".location}
      # Use referenced value from elsewhere in the config ^
```


## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md).