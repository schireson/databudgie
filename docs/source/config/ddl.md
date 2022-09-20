# DDL

By default, `databudgie` only backs up data. If the DDL structure of the database is
also expected to be backed up, `ddl` is an optional section which can be included
at any level of specificity (per table, backup/restore, global).

```yaml
# Global
ddl:
  ...

# Backup/restore
backup:
  ddl:
    ...

# Per Table
backup:
  tables:
    public.*:
      ddl:
        ...
```

Specifically the `ddl` sub-structure can contain any of the following

```yaml
ddl:
  enabled: true/false
  location: some/path/
  clean: true/false
```

## `enabled`

Defaults to `false`.

Enables or disables DDL backup/restore at whatever level of specificity it
is supplied.

Schemas, tables, sequences, dependent types, etc are all supported object kinds
to backup.

```{warning}
In a `restore` config position/command, enabling `ddl` will first drop the target object (table)
before attempting to restore its DDL.

This **can** result in problems with sufficiently complex foreign key relationships and
existing data.
```

```{note}
You can supply both more general and more specific `ddl` values, and the most specific
version available for that context will be used. That is, you could enable backup level
`ddl`, and then disable it for a specific table, or vice versa.
```

## `location`

Defaults to `ddl`.

The location for DDL backups to be recorded to/restored from.

```{note}
Like all locations, the DDL location can be specified as an S3 URI and that will
transparently read/write from/to S3 rather than locally.
```

```{note}
Table-specific ddl composes with ddl root `location` with the table-specific
`location` value, to determine the absolute path.

Given some table-specific location: `backups/{table}`, and the default ddl value: `ddl`,
you will get an composed full path: `ddl/backups/public.tablename` in addition to the
`backups/public.tablename` for the actual data.
```

## `clean`

Defaults to `false`. Only a relevant option in `restore` config/command.

Drops the target database, recreates it, and restores into the new database rather
than trying to mutate the target database in place.

To avoid the aforementioned issues with data/table complexity, starting with a known-empty
database can be a simpler alternative, especially when the backups are of a set of
self-contained set of structures.

```{note}
While the rest of the `ddl` options can be specified at any level of specificity,
given that this option affects the whole database, it is only read at restore or
global-level positions of specificity.
```
