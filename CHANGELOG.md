# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2021-09-28
- Add support for Python >3.7
- Add `-c/--config` flag to specify alternate config files.

## [1.0.0] - 2021-09-27

- Initial databudgie release.
    - `databudgie backup` feature for exporting SQL queries to S3 CSVs.
    - `databudgie restore` feature to loading S3 CSVs to database.
    - See README.md for additonal usage information.