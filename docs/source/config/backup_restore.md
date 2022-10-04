# Backup/Restore Config

The backup and restore configuration options are intentionally identical in nearly
all cases. Generally, you can assume any option given for one will be available
for the other, except when specifically noted otherwise.

```{note}
Any configuration defined at this level can also be supplied at the top level,
and the values will be shared between backup and restore config.
```

High level listing of all available config at this level follows:

```yaml
backup:
  url: ...
  tables: ...
  ddl: ...
  logging: ...
  sequences: true
  data: true
  s3: ...
  sentry: ...

restore:
  url: ...
  tables: ...
  ddl: ...
  logging: ...
  sequences: true
  data: true
  s3: ...
  sentry: ...
```

## `url`

Required! This is one of the only required pieces of configuration!

The `url` value can be given either by a `RFC-1738` compliant string, or by the component
parts of that string:

```yaml
# This
url: dialect+driver://username:password@host:port/database

# Is the same as this:
url:
  drivername: dialect+driver
  username: username
  password: password
  host: host
  port: port
  database: database
```

## `tables`

Defaults to `[]`.

Defines the set of tables (and their config) to be backed up or restored.

Table config can either be given as a list or a mapping, depending on how
complex the config is. Specifically giving tables as a mapping makes it impossible
to specify a backup of the same table twice (for example with different queries),
so using a list is generally preferred.

```yaml
# This
tables:
  public.*:
    query: select * from {table}

# Is the same as this.
tables:
 - name: public.*
   query: select * from {table}
```

Additionally, if you can omit all other table-specific config (either by giving
it at a more [general level of specificity](precedence), or relying on the default
values), you can simply list the tables/globs directly:

```yaml
tables:
  - public.*
  - foo.*
```

## `ddl`

The backup/restore of DDL akin to `pg_dump` can be enabled, see [DDL](ddl) for
more details.

## `sequences`

Defaults to `true`.

When `true`, backs up or restores all sequences' positions associated with matching target tables.

## `data`

Defaults to `true`.

When `true`, records the data returned by `query` (`table`-level config) or restores the data.

## `manifest`

A manifest of backup/restore operations over time can be written to, depending
on config.

See the [Manifest](manifest) documentation.

## `s3`

Any `location` field meant to specify where to backup tables to or where to restore from,
can be given either as a local file path, or an S3 path (including the bucket, like
`s3://bucket/rest/of/path/`).

When supplying an S3 path as a `location`, a supplemental configuration defining
how to authenticate against the bucket is required at either the backup/restore or
global level.

```yaml
s3:
  aws_access_key_id: ...
  aws_secret_access_key: ...
  region: ...
  profile: ...
```

## `sentry`

The top-level `sentry` configuration can optionally be included, which will enable
reporting of issues to sentry when invoked through the CLI.

Note this has no effect when using databudgie as a library (where you should instead
set up sentry in your application).

```yaml
sentry:
  sentry_dsn: sample@sentry.io/dsn
  environment: prod
  version: abcedf
```

## `logging`

Defaults to `enabled: true` and `level: INFO`.

Whether and how to configure python logging.

```yaml
logging:
  enabled: true
  logging: INFO
```
