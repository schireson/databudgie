# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.4] - 2021-10-05
- Add additional documentation to restore and manifest features.

## [1.2.3] - 2021-10-04
- Move psycopg dependencies from dev deps to regulr deps.

## [1.2.2] - 2021-10-01
- Adjust boto3 dep to be compatible with internal projects.

## [1.2.1] - 2021-10-01
- Fix missing dependencies.
- Fix restore to tables with automatically defaulting columns.
    - Note: This change means column names in the CSV must match the column names in the tables.
- Fix unneeded error when truncating.

## [1.2.0] - 2021-10-01
- Add `databudgie config` command to print a populated/dereferenced config to stdout.

## [1.1.1] - 2021-09-29
- Downgrade click to ^7.0.0
- Change developer `make init` Python versions
- Change default docker image command from `bash` to `databudgie`

## [1.1.0] - 2021-09-28
- Add support for Python >3.7
- Add `-c/--config` flag to specify alternate config files.

## [1.0.0] - 2021-09-27
- Initial databudgie release.
    - `databudgie backup` feature for exporting SQL queries to S3 CSVs.
    - `databudgie restore` feature to loading S3 CSVs to database.
    - See README.md for additonal usage information.