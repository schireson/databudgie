# Backup

```bash
$ databudgie [--strict] backup
```

At a high level, the backup command is:

- Connecting to the database given by the `backup.url` connection string
- Iterating over the set of matching tables given by `backup.tables`
- Executing the query given by `backup.tables.query` for each matching table
- Saving the result, including additional other data such as the DDL (if enabled), or
  sequence state information (if enabled) to the location given by `backup.tables.location`.

See [Backup Config](config/backup_restore.md) for more details on the particulars behind the config.

The `--strict` option will cause databudgie to exit if it encounters an error backing up a
specific table, otherwise it will attempt to proceed to other tables.

Sample backup configuration:

```yaml
backup:
  url: postgresql://postgres:postgres@localhost:5432/postgres
  manifest: public.databudgie_manifest
  tables:
    - name: public.product
      query: "select * from public.product where store_id = 4"
      location: s3://my-s3-bucket/databudgie/public.product
    - name: public.sales
      query: "select * from public.sales where store_id = 4"
      location: s3://my-s3-bucket/databudgie/public.sales
```
