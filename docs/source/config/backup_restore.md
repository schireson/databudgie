# Backup/Restore Config

The backup and restore configuration options are intentionally identical in
nearly all cases. Generally, you can assume any option given for one will be
available for the other, except when specifically noted otherwise.

```{note}
Any configuration defined at this level can also be supplied at the top level,
and the values will be shared between backup and restore config.
```

High level listing of all available config at this level follows:

```yaml
backup:
  url: ... # alternatively, 'connection:'
  connections: ...
  tables: ...
  ddl: ...
  logging: ...
  sequences: true
  data: true
  root_location: null
  s3: ...

restore:
  url: ... # alternatively, 'connection:'
  connections: ...
  tables: ...
  ddl: ...
  logging: ...
  sequences: true
  data: true
  root_location: null
  s3: ...
```

## `url` / `connection`

Examples below use `url` or `connection` depending on which makes more sense in
the scenario. In reality, these options are exactly equivalent.

The field name can be **any** of:

- A "url": `RFC-1738` style connection string (directly routed to SQLAlchemy),
  or its component parts:

  ```yaml
  url: dialect+driver://username:password@host:port/database
  ```

- An inline "connection", broken out by the above url's component parts:

  ```yaml
  connection:
    drivername: dialect+driver
    username: username
    password: password
    host: host
    port: port
    database: database
  ```

- A connection's name:

  ```yaml
  connections:
    foo: dialect+driver://username:password@host:port/database
    bar:
      host: host
      # ...

  connection: foo
  # or
  connection: bar
  ```

## `connections`

Defaults to `[]`.

Defines a set of named connection which can be selected to connect to at
command-time. One selects a connection through the CLI
`databudgie --connection <name>`.

While this config can coexist with the above `url`/`connection` config, they are
mutually exclusive at runtime. That is, if a `--connection` is supplied, it will
take precedence. If a `--connection` is not supplied, the `url`/`connection`
config will be used instead.

`connections` can either be given as a list or a mapping, similar to `tables`.

```yaml
# This
connections:
  dev: postgresql://localhost:5432
  prod:
    drivername: postgresql
    host: localhost
    port: 5432

# Is the same as this.
connections:
 - name: dev
   url: postgresql://localhost:5432
 - name: prod
   drivername: postgresql
   host: localhost
   port: 5432
```

## `tables`

Defaults to `[]`.

Defines the set of tables (and their config) to be backed up or restored.

Table config can either be given as a list or a mapping, depending on how
complex the config is. Specifically giving tables as a mapping makes it
impossible to specify a backup of the same table twice (for example with
different queries), so using a list is generally preferred.

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
it at a more [general level of specificity](precedence), or relying on the
default values), you can simply list the tables/globs directly:

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

When `true`, backs up or restores all sequences' positions associated with
matching target tables.

## `data`

Defaults to `true`.

When `true`, records the data returned by `query` (`table`-level config) or
restores the data.

## `root_location`

Defaults to `null`.

When specified, all table-level `location` values will be made relative to the
`root_location`.

## `manifest`

A manifest of backup/restore operations over time can be written to, depending
on config.

See the [Manifest](manifest) documentation.

## `s3`

Any `location` field meant to specify where to backup tables to or where to
restore from, can be given either as a local file path, or an S3 path (including
the bucket, like `s3://bucket/rest/of/path/`).

When supplying an S3 path as a `location`, a supplemental configuration defining
how to authenticate against the bucket is required at either the backup/restore
or global level.

```yaml
s3:
  aws_access_key_id: ...
  aws_secret_access_key: ...
  region: ...
  profile: ...
```

## `logging`

Defaults to `enabled: true` and `level: INFO`.

Whether and how to configure python logging.

```yaml
logging:
  enabled: true
  logging: INFO
```
