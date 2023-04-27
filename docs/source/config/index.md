# Config

To first get a sense of what databudgie config looks like, here is a basic
example configuration with both backup and restore defined.

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

The backup and restore configuration options are intentionally identical in
nearly all cases. Generally, you can assume any option given for one will be
available for the other, except when specifically noted otherwise.

```{toctree}
default_and_file_types
precedence
interpolation
backup_restore
table
ddl
manifest
```
