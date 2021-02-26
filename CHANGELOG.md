## dbt-spark 0.19.1 (Release TBD)

## dbt-spark 0.19.1b2 (February 26, 2021)

### Under the hood
- update serialization calls to use new API in dbt-core `0.19.1b2` ([#150](https://github.com/fishtown-analytics/dbt-spark/pull/150))

## dbt-spark 0.19.0.1 (February 26, 2021)

### Fixes
- Fix package distribution to include incremental model materializations ([#151](https://github.com/fishtown-analytics/dbt-spark/pull/151), [#152](https://github.com/fishtown-analytics/dbt-spark/issues/152))

## dbt-spark 0.19.0 (February 21, 2021)

### Breaking changes
- Incremental models have `incremental_strategy: append` by default. This strategy adds new records without updating or overwriting existing records. For that, use `merge` or `insert_overwrite` instead, depending on the file format, connection method, and attributes of your underlying data. dbt will try to raise a helpful error if you configure a strategy that is not supported for a given file format or connection. ([#140](https://github.com/fishtown-analytics/dbt-spark/pull/140), [#141](https://github.com/fishtown-analytics/dbt-spark/pull/141))

### Fixes
- Capture hard-deleted records in snapshot merge, when `invalidate_hard_deletes` config is set ([#109](https://github.com/fishtown-analytics/dbt-spark/pull/143), [#126](https://github.com/fishtown-analytics/dbt-spark/pull/144))

## dbt-spark 0.19.0rc1 (January 8, 2021)

### Breaking changes
- Users of the `http` and `thrift` connection methods need to install extra requirements: `pip install dbt-spark[PyHive]` ([#109](https://github.com/fishtown-analytics/dbt-spark/pull/109), [#126](https://github.com/fishtown-analytics/dbt-spark/pull/126))

### Under the hood
- Enable `CREATE OR REPLACE` support when using Delta. Instead of dropping and recreating the table, it will keep the existing table, and add a new version as supported by Delta. This will ensure that the table stays available when running the pipeline, and you can track the history.
- Add changelog, issue templates ([#119](https://github.com/fishtown-analytics/dbt-spark/pull/119), [#120](https://github.com/fishtown-analytics/dbt-spark/pull/120))

### Fixes
- Handle case of 0 retries better for HTTP Spark Connections ([#132](https://github.com/fishtown-analytics/dbt-spark/pull/132))

### Contributors
- [@danielvdende](https://github.com/danielvdende) ([#132](https://github.com/fishtown-analytics/dbt-spark/pull/132))
- [@Fokko](https://github.com/Fokko) ([#125](https://github.com/fishtown-analytics/dbt-spark/pull/125))

## dbt-spark 0.18.1.1 (November 13, 2020)

### Fixes
- Fix `extras_require` typo to enable `pip install dbt-spark[ODBC]` (([#121](https://github.com/fishtown-analytics/dbt-spark/pull/121)), ([#122](https://github.com/fishtown-analytics/dbt-spark/pull/122)))

## dbt-spark 0.18.1 (November 6, 2020)

### Features
- Allows users to specify `auth` and `kerberos_service_name` ([#107](https://github.com/fishtown-analytics/dbt-spark/pull/107))
- Add support for ODBC driver connections to Databricks clusters and endpoints ([#116](https://github.com/fishtown-analytics/dbt-spark/pull/116))

### Under the hood
- Updated README links ([#115](https://github.com/fishtown-analytics/dbt-spark/pull/115))
- Support complete atomic overwrite of non-partitioned incremental models ([#117](https://github.com/fishtown-analytics/dbt-spark/pull/117))
- Update to support dbt-core 0.18.1 ([#110](https://github.com/fishtown-analytics/dbt-spark/pull/110), [#118](https://github.com/fishtown-analytics/dbt-spark/pull/118))

### Contributors
- [@danielhstahl](https://github.com/danielhstahl) ([#107](https://github.com/fishtown-analytics/dbt-spark/pull/107))
- [@collinprather](https://github.com/collinprather) ([#115](https://github.com/fishtown-analytics/dbt-spark/pull/115))
- [@charlottevdscheun](https://github.com/charlottevdscheun) ([#117](https://github.com/fishtown-analytics/dbt-spark/pull/117))
- [@Fokko](https://github.com/Fokko) ([#117](https://github.com/fishtown-analytics/dbt-spark/pull/117))

## dbt-spark 0.18.0 (September 18, 2020)

### Under the hood
- Make a number of changes to support dbt-adapter-tests ([#103](https://github.com/fishtown-analytics/dbt-spark/pull/103))
- Update to support dbt-core 0.18.0. Run CI tests against local Spark, Databricks ([#105](https://github.com/fishtown-analytics/dbt-spark/pull/105))
