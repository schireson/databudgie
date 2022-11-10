# Changelog

## [Unreleased](https://github.com/schireson/databudgie/compare/v2.7.0...HEAD) (2022-11-10)

### Fixes

* Issues from release.
  ([15066df](https://github.com/schireson/databudgie/commit/15066df96dcb40bd9dd2fc54454ff4ac7e0a0664))

## v2.7.0 (2022-11-09)

### Features

* Automatically include dependent tables.
  ([1c96e20](https://github.com/schireson/databudgie/commit/1c96e207b95c8a4c4a6b2a80767d2310f7d01f7e))
* Warn on unused table definitions.
  ([1d2a22b](https://github.com/schireson/databudgie/commit/1d2a22b5830865d9d31accbc566f0030f52e8ec1))
* Pretty output.
  ([920f260](https://github.com/schireson/databudgie/commit/920f2605f31bf94711ebbe482a8723edcd0ea0e9))
* Add version cli flag.
  ([55e7e78](https://github.com/schireson/databudgie/commit/55e7e78e7ab2588c02f7daa265135c7848e91cea))
* Add CLI arguments which compose with config.
  ([0ceb620](https://github.com/schireson/databudgie/commit/0ceb62007fbed1c82c8f6a46a50ac6743b4a944e))
* Allow ddl/sequences/data as table-specific pieces of configuration.
  ([96cc6e9](https://github.com/schireson/databudgie/commit/96cc6e9d0c4753574b9a9e94fe12587b1bcf8fbc))
* Add ability to stack config.
  ([01e90fe](https://github.com/schireson/databudgie/commit/01e90fe8e712af9690d0124b71f8f43d5eafb248))
* Config option to restore postgres sequences.
  ([6cf7d3f](https://github.com/schireson/databudgie/commit/6cf7d3f4e79d443953d77fabce18df3c02c5c517))
* Support url components, rather than just the whole url string.
  ([39c7ad5](https://github.com/schireson/databudgie/commit/39c7ad56e5a02859924aa526fe6ac1567236c534))
* Add support for backing up and restoring schema DDL.
  ([fb3902b](https://github.com/schireson/databudgie/commit/fb3902b07d22247aef6e1a196846bc91e7940d42))
* Add optional compression.
  ([60c9eff](https://github.com/schireson/databudgie/commit/60c9eff60f0f7a007ec83d3d8445f029d322baa4))
* Allow a tables list to enable multiple targets of the same table.
  ([b9ee003](https://github.com/schireson/databudgie/commit/b9ee003b9cecffd38084ab9a42a9a15feebf4a57))
* Add ability to drop/restore empty database as "clean" state.
  ([e993db0](https://github.com/schireson/databudgie/commit/e993db04787b4a3f0c0b89543b65ed9128e0863a))
* Add backup and restore functionality for ddl.
  ([dc852b0](https://github.com/schireson/databudgie/commit/dc852b0c9c86fa4a104399ff824004eccc46dd37))
* Support local backups, make boto3 an optional dependency.
  ([4acc494](https://github.com/schireson/databudgie/commit/4acc49464143447f0eb7ae8e6e92f626bde05c19))
* Implement globbing for restore operations.
  ([149ba4f](https://github.com/schireson/databudgie/commit/149ba4fdd3212ab164f18e876725e4c7d7fc1764))
* Add ability to specify tables as globs for backup.
  ([5ffef96](https://github.com/schireson/databudgie/commit/5ffef96eef25159990a208700fed2bf36440fca3))

### Fixes

* Bug where s3 bucket name is repeated in ddl paths.
  ([f4ddd51](https://github.com/schireson/databudgie/commit/f4ddd519b586545ec1cf6f77c98b1246136ba9c7))
* Bug where mutation of the underlying config dict structure affects later config  
lookups.
  ([5c81460](https://github.com/schireson/databudgie/commit/5c81460862130beb273c651750f0d428e1622d76))
* Autodeploy to artifactory.
  ([26374b6](https://github.com/schireson/databudgie/commit/26374b6e0b9fcd7acb42bd1e12d9963640eadf49))
* postgres version compatibility.
  ([44adc2b](https://github.com/schireson/databudgie/commit/44adc2b2ccffab6bacd7e407a1919da533e12322))
* compatibility with sqlalchemy 1.3.
  ([c473e82](https://github.com/schireson/databudgie/commit/c473e825ea8fe17e0c068892107757912fcfd152))
* Only require URL on commands which use it.
  ([8a98d32](https://github.com/schireson/databudgie/commit/8a98d322352f1589c29d7088cfe48baf4fe040b8))
* Handle the restore case where the backed up folder does not exist.
  ([9353c62](https://github.com/schireson/databudgie/commit/9353c628ac5fc1ecb85de04060ce96450e6121f3))
* Support sqlalchemy 1.3.
  ([9aea6b4](https://github.com/schireson/databudgie/commit/9aea6b43c2d16a6a378a15360e62072c18d67727))
* #43 replace experimental postgres table search
  ([f0d4237](https://github.com/schireson/databudgie/commit/f0d42375e42a4a8fd4b2b01b7be2b7b5f251153c))
* #43 backup hangs during table inspection
  ([633ce7d](https://github.com/schireson/databudgie/commit/633ce7db3bfa3f13e7788b2638dd71f6f852b541))
* Misc fixes.
  ([b1b64f6](https://github.com/schireson/databudgie/commit/b1b64f67bd8de513ca14c39e013523a6d086b2ab))
* Add missing py.typed file.
  ([6e6d664](https://github.com/schireson/databudgie/commit/6e6d6647ed20b7752a314bb403cca3633f82c431))
* Loosen version constraints.
  ([c619781](https://github.com/schireson/databudgie/commit/c61978101678ba3149408bdf41247acd87a9bde8))
* Black reformatting changes.
  ([c83b91e](https://github.com/schireson/databudgie/commit/c83b91eca5fa8a8fed4d92ca8f3ac1182a1e0c8e))
* lessen set of required dependencies.
  ([22d6b34](https://github.com/schireson/databudgie/commit/22d6b349b4a2b9c4fd0bb9e6e173125ff285c9cf))
* add missing dependencies
  ([2298efe](https://github.com/schireson/databudgie/commit/2298efe1f65aaae4df929ed96801477ea49be904))
