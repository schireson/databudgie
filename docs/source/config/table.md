# Table Config

"table config" are options whichs are valid at the per-table level, such as:

```yaml
backup:
  tables:
    - name: foo # <--- here
```

Remember that any option defined below can be specified at more general levels
of config as a fallback for options which are the same across many/all tables.

All options are intentionally identical, and in most cases are valid for both
`backup` and `restore` commands, unless specifically noted otherwise.

## `name`

When `tables` are given as a mapping, defaults to the key-side of the mapping.
When `tables` are given as a list, this field is **required**!

The `name` field defines the matching criteria for tables which should be backed
up/ restored. For the simplest case, this can just be the name (or
`{schema}.{name}`) of the table. However this name can also be "globbed" to
match multiple tables!

```{note}
Remember each item in this list has a corresponding `query` field which can be
an arbitrary query. This means that you can utilize the same `name` more than once.
It's just a way of matching the **source table** that each iteration should target.
```

```yaml
# This
tables:
  public.foo:
  '*.*':

# Is the same as this
tables:
  - name: public.foo
  - name: '*.*'

# Is the same as this
tables:
  - public.foo
  - '*.*'
```

````{note}
`name` **can** also be omitted entirely, with some caveats. The "name" field
populates the `{table}` templated into queries and location paths (**both**
of which default to including the `{table}` template value).

Thus, if you omit the "name" field, you must have also provided a concrete "query"
and "location" field.

```yaml
tables:
  - query: select * from for_example_a_view
    location: backups/public.for_example_a_view
```
````

### Globbing

Using common globbing rules:

| Pattern | Meaning                          |
| ------- | -------------------------------- |
| \*      | matches everything               |
| ?       | matches any single character     |
| [seq]   | matches any character in seq     |
| [!seq]  | matches any character not in seq |

For some common examples:

- `public.*`: All tables in a schema
- `*.foo`: Tables with a given name in all schemas
- `*_log`: All tables ending with some suffix
- `*_*_log`: Multiple globs

See also the [](#exclude) key below.

```{note}
Globbing was chosen over regex for a much more simplified way of quickly matching
table names in a way that is easily grokkable. It's conceivable that regex matching
could be supported in the future, but in most common cases globs with exclusions
should be able to match most kinds of cases.
```

## `location`

Defaults to `backups/{table}`

`location` paths use URI protocols for determining (on a per path basis) what
protocol to use for the backup/restore of that path.

```{tip}
Output files default to being separated into table-specific folder through
`{table}`. They can be colocated regardless of folder by removing that template
source e.x. `backups/`.
```

### Local files

Note an otherwise unadorned path will be assumed to be a local file path, for
example `path/to/folder`.

For backups, if the path leading up to the leaf folder does not yet exist, it
will be automatically created.

### S3

A path is identified as an "S3 path" when it is prefixed with the S3 protocol:
`s3://`.

For example `s3://bucket/path/to/folder` references a path `path/to/folder`
inside of a bucket `bucket`.

S3 paths make use of the [s3](backup_restore.md#s3) config for authorization
against the included bucket. Alternatively, the common environment variables
recognized by the `aws` CLI (i.e. `AWS_PROFILE`, `AWS_REGION`,
`AWS_SECRET_ACCESS_KEY`, `AWS_ACCESS_KEY_ID`, etc) will be automatically read.

## `filename`

Defaults to `{timestamp}.{ext}`.

Coupled with the "location" configuration, a fully templated path will result as
(by default) `backups/{table}/{timestamp}.{ext}`. This yields a new file each
time a command is run.

```{tip}
`{timestamp}` is a "variable" template source, meaning a new value will be yielded
each time. In order to reference a static filename, configure a filename without
a variable source, e.x. `{table}.{ext}`.
```

## `strategy`

Defaults to `use_latest_filename`.

This option is **only** read during `restore` commands and has two valid values:
`use_latest_filename` and `use_latest_metadata`.

The restore-time "strategy" defines how databudgie should determine which file,
on a per-table basis to read from. Note that each time you run
`databudgie backup`, it's never altering preexisting files, instead it's writing
new files to disk with a timestamp in the name to disambiguate.

- `use_latest_filename` will make use of the default file naming scheme which
  includes write-time timestamps in the name of the file, and chooses the most
  recent timestamp.
- `use_latest_metadata` will use the Operating System file attributes for file
  creation time (or equivalent in S3), and chooses the most recent one.

## `truncate`

Defaults to `false`.

This option is **only** read during `restore` commands. When `true`, truncates
the contents of the table before attempting to restore into it.

```{note}
This can run afoul of foreign key constraints, depending on your table structure.

The tables are intentionally ordered in such a way as to avoid or reduce the possibility
of foreign key related issues; however self referential or circular foreign key
relationships may encounter issues with this option (on those tables).
```

## `query`

Defaults to `select * from {table}`

Specify the query to be used on a given match. The default, which simply selects
the whole table is the most obviously useful query one might use, to backup the
whole table.

There aren't any constraints on the query to be executed, however, so this field
can apply filters, perform joins, alter/obfuscate the data, or otherwise do
whatever it wants.

## `compression`

Defaults to `null`.

Depending on the size of tables, the backups can get quite large. By default
compression is disabled, but it can be enabled for any/all table "data" backups.

Valid values include: `gzip`.

```{note}
This automatically appends the compression file extension to the backup files
(i.e. `.gz` for gzip), and will only work correctly if both the backup side and
restore side agree on the value of the `compression` key.
```

## `exclude`

Defaults to `[]`.

This is most commonly useful when using globs, particularly when running up
against the limitations of glob matching versus regex

```yaml
tables:
  # All log tables
  - name: "*_log"
    exclude:
      - "tree_log"
```

Note that `exclude` list entries can also be globs themselves. So you can use
them to arrive at more complex matching criteria than could be achieved with the
single `name` matching glob.

## `follow_foreign_keys`

Defaults to `false`.

When `true`, any foreign keys on the table will be recursively followed when
performing the backup/restore. This allows one to specify **only** the table one
seeks to backup/restore and any tables related through foreign keys will also be
backed up.

```{note}
The backup file that is stored/read from will be relative to the explicit table
that originated the inclusion of that table in the config.

That is, if your config file includes some table "public.foo" with `location` "backups/{table}",
then any tables backed up as a result of `follow_foreign_keys` on behalf of this table
will end up at `backups/public.foo/{table}`.

In the event that two tables produce "followed" versions of the same table, only one
backup will be produced, under whichever table happens to resolve first (a necessary measure
on the restore-side due to foreign key constraints). For a given heirarchy of foreign keys, this
should remain constant, but doesnt preclude some future table/foreign key from taking
control of that table by virtue of being higher up in the heirarchy.
```

## `skip_if_exists`

```{note}
Unlike most options, this option only has an effect in the backup-side of the config.
```

Defaults to `false`.

When `true`, skips the backing up the table, if there already exists backup data
for the annotated table.
