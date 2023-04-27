# Precedence

It can be tedious to repeat all of the configuration at a very specific level
(like per-table) and repeat it for every single table, when it's always the
same. As such, nearly all of the configuration of databudgie is optional, and
has sensible fallbacks where they make sense.

In order to reduce the need to repeat identical config across locations in the
config, you can supply any config value at a more general level (i.e. a less
deeply nested level in the yaml) than it's actually defined for, and it will
"fall back" to the more general version when it's omitted at the more specific
level.

For example, `query: select * from {table}` or `compression: gzip` are a
table-specific pieces of configuration, and you could reasonably could apply
them to **all** table config in the whole file. Thus the three following options
could be equally valid.

```yaml
# Table specific
backup:
  tables:
    - name: '*.*'
      query: select * from {table}
      compression: gzip
    # ...

# Backup specific
backup:
  query: select * from {table}
  compression: gzip
  tables:
    - '*.*'
    # ...

# Command specific
query: select * from {table}
compression: gzip
backup:
  tables:
    - '*.*'
    # ...
```

This holds true for any piece of configuration, not just the examples listed.
Any value specified at a more specific level will take precedence over less
specific values, falling back all the way to the config-specific default, if
necessary.

(mulitple_files)=

## Multiple Files

Relatedly, it's possible at the command-line to supply multiple config values.
For example, `databudgie -c base.yml -c tables.yml backup`.

This applies precedence very similarly to before, but at an inter-config level.
The files' contents will be applied on top of one another in the order given.
This means that in the above example, values for `tables.yml` will be
prioritized over those of `base.yml`.

In the above example, those files might look like:

```yaml
---
name: base.yml
---
backup:
  url: postgresql://postgres:postgres@localhost:5432/postgres1

restore:
  url: postgresql://postgres:postgres@localhost:5432/postgres2
```

```yaml
---
name: tables.yml
---
tables:
  - public.foo
  - public.bar
```

In this example, you'd essentially end up with a straight merge of the two
files, which can be useful in separating out the static parts of config from the
parts more likely to be customized on demand (for example, out of source
control).

## CLI level options

At time of writing there aren't necessarily any CLI options to which this
comment will apply, but definitely planned is the ability to specify config
options at the CLI level, particularly those which would be common to want to
specify on-demand without alterting the config, such as tables.

Any CLI options which relate to configuration options will act as an additional
level of specificity **on top** of those defined in the config. That is, if
provided, a CLI-level option will always take precedence over configuration
options.
