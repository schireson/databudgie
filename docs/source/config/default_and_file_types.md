# Default Files and File Types

By default, `databudgie` looks through a seriess of specific file names, in
order:

- config.databudgie.yml (An artifact, to preserve backwards-compatibility)
- databudgie.yml
- databudgie.yaml
- databudgie.json
- databudgie.toml

Naming your config file as one of the above names, allows you to execute
`databudgie backup` without any specified config file.

**Specifying** a file at the command line through `databudgie -c databudgie.yml`
disables the default lookup heirarchy. You can additionally
[provide multiple](mulitple_files) `-c` arguments in order to compose config
across files.

## File Types

As evidenced by the default filename lookup options, we support mulitple
different kinds of config file formats. Currently: `yaml`, `json`, and `toml`.

All documentation will use `yaml` for examples, because we think it produces the
clearest and most concise config to read. However, any config can be represented
in any of the formats, prettry directly. represented

For example:

### Yaml

```yaml
backup:
  url: postgresql://postgres:postgres@localhost:5432/postgres
  tables:
    - name: public.product
      query: "select * from {table} where store_id = 4"
      location: s3://my-s3-bucket/databudgie/public.product
restore:
  url: postgresql://postgres:postgres@localhost:5432/postgres
  tables:
    - name: public.product
      query: "select * from {table} where store_id = 4"
      location: s3://my-s3-bucket/databudgie/public.product
```

### Json

```json
{
    "backup": {
        "url": "postgresql://postgres:postgres@localhost:5432/postgres"
        "tables": [
            {
                "name": "public.product",
                "query": "select * from {table} where store_id = 4",
                "location": "s3://my-s3-bucket/databudgie/public.product"
            }
        ]
    },
    "restore": {
        "url": "postgresql://postgres:postgres@localhost:5432/postgres"
        "tables": [
            {
                "name": "public.product",
                "query": "select * from {table} where store_id = 4",
                "location": "s3://my-s3-bucket/databudgie/public.product"
            }
        ]
    },
```

### Toml

```toml
[backup]
url = "postgresql://postgres:postgres@localhost:5432/postgres"

[[backup.tables]]
name = "public.product"
query = "select * from {table} where store_id = 4"
location = "s3://my-s3-bucket/databudgie/public.product"

[backup]
url = "postgresql://postgres:postgres@localhost:5432/postgres"

[[restore.tables]]
name = "public.product"
query = "select * from {table} where store_id = 4"
location = "s3://my-s3-bucket/databudgie/public.product"
```
