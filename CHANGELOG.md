## dbt-spark 0.19.0 (Release TBD)

### Breaking changes
- Users of the `http` and `thrift` connection methods need to install extra requirements: `pip install dbt-spark[PyHive]` ([#109](https://github.com/fishtown-analytics/dbt-spark/pull/109), [#126](https://github.com/fishtown-analytics/dbt-spark/pull/126))

### Under the hood
- Add changelog, issue templates ([#119](https://github.com/fishtown-analytics/dbt-spark/pull/119), [#120](https://github.com/fishtown-analytics/dbt-spark/pull/120))

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
