# Restore

```bash
$ databudgie [--strict] restore
```

The restore command will download files and restore them into the database. databudgie will iterate over the `restore.tables` and insert the CSV contents into the tables in order of appearance.

The column headers in the CSV will be used to match the contents of the file to the columns in the table. This allows for leaving columns with default values unset if you are restoring data to a different table than which it was copied from.

```yaml
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

## DDL

By default, `databudgie` assumes the target tables already exist and only executes the DDL creation
commands if restore-side ddl options are enabled. Generally it would only make sense to enable
`ddl` options if the `backup`-side ddl options are also enable, although the inverse is not necessarily true.

See the [DDL](config/ddl.md) config section for more details.

```{note}
DDL restoration for `restore` in particular is more challenging and prone to issues than backup.
Due to foreign keys, indices, and references to other objects (like types) you're more likely to
encounter issues restoring from DDL.

While any restore-time ddl issues are considered bugs, if you're performing a full-database
restore it can sometimes be easier to use the vanilla tooling (pg_dump, migrations) to restore DDL
rather than using the DDL functionality of databudgie.
```
