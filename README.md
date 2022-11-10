# Databudgie

![Github Actions Build](https://github.com/schireson/databudgie/actions/workflows/build.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/schireson/databudgie/badge.svg?branch=main&t=6I0aU6)](https://coveralls.io/github/schireson/databudgie?branch=main)
[![Documentation
Status](https://readthedocs.org/projects/databudgie/badge/?version=latest)](https://databudgie.readthedocs.io)

![](docs/source/_static/databudgie.png)

Databudgie is a CLI & library for database performing targeted backup and restore
of database tables or arbitrary queries against database tables.

# Usage

A minimal config file might look like:

```yaml
# config.databudgie.yml
backup:
  url: postgresql://postgres:postgres@localhost:5432/postgres
  tables:
    - name: public.product
      query: "select * from {table} where store_id > 4"
      location: s3://my-s3-bucket/databudgie/public.product
restore:
  url: postgresql://postgres:postgres@localhost:5432/postgres
  tables:
    - name: public.product
      location: s3://my-s3-bucket/databudgie/public.product
```

With that config in place, backing up the defined tables (using the specified config)
is as simple as `databudgie backup`; and restore `databudgie restore`.

## Installation

```bash
pip install databudgie
```
