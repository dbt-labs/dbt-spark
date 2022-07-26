## dbt-spark 1.2.0 (July 26, 2022)

### Fixes
- Pin `pyodbc` to version 4.0.32 to prevent overwriting `libodbc.so` and `libltdl.so` on Linux ([#397](https://github.com/dbt-labs/dbt-spark/issues/397/), [#398](https://github.com/dbt-labs/dbt-spark/pull/398/))

### Contributors
- [@barberscott](https://github.com/barberscott)  ([#398](https://github.com/dbt-labs/dbt-spark/pull/398/))

## dbt-spark 1.2.0rc1 (July 12, 2022)

### Fixes
- Incremental materialization updated to not drop table first if full refresh for delta lake format, as it already runs _create or replace table_ ([#286](https://github.com/dbt-labs/dbt-spark/issues/286), [#287](https://github.com/dbt-labs/dbt-spark/pull/287/))
- Apache Spark version upgraded to 3.1.1 ([#348](https://github.com/dbt-labs/dbt-spark/issues/348), [#349](https://github.com/dbt-labs/dbt-spark/pull/349))

### Features
- Add grants to materializations ([#366](https://github.com/dbt-labs/dbt-spark/issues/366), [#381](https://github.com/dbt-labs/dbt-spark/pull/381))

### Under the hood
- Update `SparkColumn.numeric_type` to return `decimal` instead of `numeric`, since SparkSQL exclusively supports the former ([#380](https://github.com/dbt-labs/dbt-spark/pull/380))

### Contributors
- [@grindheim](https://github.com/grindheim) ([#287](https://github.com/dbt-labs/dbt-spark/pull/287/))
- [@nssalian](https://github.com/nssalian) ([#349](https://github.com/dbt-labs/dbt-spark/pull/349))

## dbt-spark 1.2.0b1 (June 24, 2022)

### Fixes
- `adapter.get_columns_in_relation` (method) and `get_columns_in_relation` (macro) now return identical responses. The previous behavior of `get_columns_in_relation` (macro) is now represented by a new macro, `get_columns_in_relation_raw` ([#354](https://github.com/dbt-labs/dbt-spark/issues/354), [#355](https://github.com/dbt-labs/dbt-spark/pull/355))

### Under the hood
- Initialize lift + shift for cross-db macros ([#359](https://github.com/dbt-labs/dbt-spark/pull/359))
- Add invocation env to user agent string ([#367](https://github.com/dbt-labs/dbt-spark/pull/367))
- Use dispatch pattern for get_columns_in_relation_raw macro ([#365](https://github.com/dbt-labs/dbt-spark/pull/365))

### Contributors
- [@ueshin](https://github.com/ueshin) ([#365](https://github.com/dbt-labs/dbt-spark/pull/365))
- [@dbeatty10](https://github.com/dbeatty10) ([#359](https://github.com/dbt-labs/dbt-spark/pull/359))

## dbt-spark 1.1.0 (April 28, 2022)

### Features
- Add session connection method ([#272](https://github.com/dbt-labs/dbt-spark/issues/272), [#279](https://github.com/dbt-labs/dbt-spark/pull/279))
- rename file to match reference to dbt-core ([#344](https://github.com/dbt-labs/dbt-spark/pull/344))

### Under the hood
- Add precommit tooling to this repo ([#356](https://github.com/dbt-labs/dbt-spark/pull/356))
- Use dbt.tests.adapter.basic in test suite ([#298](https://github.com/dbt-labs/dbt-spark/issues/298), [#299](https://github.com/dbt-labs/dbt-spark/pull/299))
- Make internal macros use macro dispatch to be overridable in child adapters ([#319](https://github.com/dbt-labs/dbt-spark/issues/319), [#320](https://github.com/dbt-labs/dbt-spark/pull/320))
- Override adapter method 'run_sql_for_tests' ([#323](https://github.com/dbt-labs/dbt-spark/issues/323), [#324](https://github.com/dbt-labs/dbt-spark/pull/324))
- when a table or view doesn't exist, 'adapter.get_columns_in_relation' will return empty list instead of fail ([#328]https://github.com/dbt-labs/dbt-spark/pull/328)

### Contributors
- [@JCZuurmond](https://github.com/dbt-labs/dbt-spark/pull/279) ( [#279](https://github.com/dbt-labs/dbt-spark/pull/279))
- [@ueshin](https://github.com/ueshin) ([#320](https://github.com/dbt-labs/dbt-spark/pull/320))

## dbt-spark 1.1.0b1 (March 23, 2022)

### Features
- Adds new integration test to check against new ability to allow unique_key to be a list. ([#282](https://github.com/dbt-labs/dbt-spark/issues/282)), [#291](https://github.com/dbt-labs/dbt-spark/pull/291))

### Fixes
- Closes the connection properly ([#280](https://github.com/dbt-labs/dbt-spark/issues/280), [#285](https://github.com/dbt-labs/dbt-spark/pull/285))

### Under the hood
- get_response -> AdapterResponse ([#265](https://github.com/dbt-labs/dbt-spark/pull/265))
- Adding stale Actions workflow ([#275](https://github.com/dbt-labs/dbt-spark/pull/275))
- Update plugin author name (`fishtown-analytics` &rarr; `dbt-labs`) in ODBC user agent ([#288](https://github.com/dbt-labs/dbt-spark/pull/288))
- Configure insert_overwrite models to use parquet ([#301](https://github.com/dbt-labs/dbt-spark/pull/301))

### Contributors
- [@amychen1776](https://github.com/amychen1776) ([#288](https://github.com/dbt-labs/dbt-spark/pull/288))
- [@ueshin](https://github.com/ueshin) ([#285](https://github.com/dbt-labs/dbt-spark/pull/285))

## dbt-spark 1.0.1rc0 (Release TBD)

### Fixes
- Closes the connection properly ([#280](https://github.com/dbt-labs/dbt-spark/issues/280), [#285](https://github.com/dbt-labs/dbt-spark/pull/285))

### Contributors
- [@ueshin](https://github.com/ueshin) ([#285](https://github.com/dbt-labs/dbt-spark/pull/285))

## dbt-spark 1.0.0 (December 3, 2021)

### Fixes
- Incremental materialization corrected to respect `full_refresh` config, by using `should_full_refresh()` macro ([#260](https://github.com/dbt-labs/dbt-spark/issues/260), [#262](https://github.com/dbt-labs/dbt-spark/pull/262/))

### Contributors
- [@grindheim](https://github.com/grindheim) ([#262](https://github.com/dbt-labs/dbt-spark/pull/262/))

## dbt-spark 1.0.0rc2 (November 24, 2021)

### Features
- Add support for Apache Hudi (hudi file format) which supports incremental merge strategies ([#187](https://github.com/dbt-labs/dbt-spark/issues/187), [#210](https://github.com/dbt-labs/dbt-spark/pull/210))

### Under the hood
- Refactor seed macros: remove duplicated code from dbt-core, and provide clearer logging of SQL parameters that differ by connection method ([#249](https://github.com/dbt-labs/dbt-spark/issues/249), [#250](https://github.com/dbt-labs/dbt-snowflake/pull/250))
- Replace `sample_profiles.yml` with `profile_template.yml`, for use with new `dbt init` ([#247](https://github.com/dbt-labs/dbt-spark/pull/247))

### Contributors
- [@vingov](https://github.com/vingov) ([#210](https://github.com/dbt-labs/dbt-spark/pull/210))

## dbt-spark 1.0.0rc1 (November 10, 2021)

### Under the hood
- Remove official support for python 3.6, which is reaching end of life on December 23, 2021 ([dbt-core#4134](https://github.com/dbt-labs/dbt-core/issues/4134), [#253](https://github.com/dbt-labs/dbt-snowflake/pull/253))
- Add support for structured logging ([#251](https://github.com/dbt-labs/dbt-spark/pull/251))

## dbt-spark 0.21.1 (Release TBD)

## dbt-spark 0.21.1rc1 (November 3, 2021)

### Fixes
- Fix `--store-failures` for tests, by suppressing irrelevant error in `comment_clause()` macro ([#232](https://github.com/dbt-labs/dbt-spark/issues/232), [#233](https://github.com/dbt-labs/dbt-spark/pull/233))
- Add support for `on_schema_change` config in incremental models: `ignore`, `fail`, `append_new_columns`. For `sync_all_columns`, removing columns is not supported by Apache Spark or Delta Lake ([#198](https://github.com/dbt-labs/dbt-spark/issues/198), [#226](https://github.com/dbt-labs/dbt-spark/issues/226), [#229](https://github.com/dbt-labs/dbt-spark/pull/229))
- Add `persist_docs` call to incremental model ([#224](https://github.com/dbt-labs/dbt-spark/issues/224), [#234](https://github.com/dbt-labs/dbt-spark/pull/234))

### Contributors
- [@binhnefits](https://github.com/binhnefits) ([#234](https://github.com/dbt-labs/dbt-spark/pull/234))

## dbt-spark 0.21.0 (October 4, 2021)

### Fixes
- Enhanced get_columns_in_relation method to handle a bug in open source deltalake which doesnt return schema details in `show table extended in databasename like '*'` query output. This impacts dbt snapshots if file format is open source deltalake ([#207](https://github.com/dbt-labs/dbt-spark/pull/207))
- Parse properly columns when there are struct fields to avoid considering inner fields: Issue ([#202](https://github.com/dbt-labs/dbt-spark/issues/202))

### Under the hood
- Add `unique_field` to better understand adapter adoption in anonymous usage tracking ([#211](https://github.com/dbt-labs/dbt-spark/pull/211))

### Contributors
- [@harryharanb](https://github.com/harryharanb) ([#207](https://github.com/dbt-labs/dbt-spark/pull/207))
- [@SCouto](https://github.com/Scouto) ([#204](https://github.com/dbt-labs/dbt-spark/pull/204))

## dbt-spark 0.21.0b2 (August 20, 2021)

### Fixes
- Add pyodbc import error message to dbt.exceptions.RuntimeException to get more detailed information when running `dbt debug` ([#192](https://github.com/dbt-labs/dbt-spark/pull/192))
- Add support for ODBC Server Side Parameters, allowing options that need to be set with the `SET` statement to be used ([#201](https://github.com/dbt-labs/dbt-spark/pull/201))
- Add `retry_all` configuration setting to retry all connection issues, not just when the `_is_retryable_error` function determines ([#194](https://github.com/dbt-labs/dbt-spark/pull/194))

### Contributors
- [@JCZuurmond](https://github.com/JCZuurmond) ([#192](https://github.com/fishtown-analytics/dbt-spark/pull/192))
- [@jethron](https://github.com/jethron) ([#201](https://github.com/fishtown-analytics/dbt-spark/pull/201))
- [@gregingenii](https://github.com/gregingenii) ([#194](https://github.com/dbt-labs/dbt-spark/pull/194))

## dbt-spark 0.21.0b1 (August 3, 2021)

## dbt-spark 0.20.1 (August 2, 2021)

## dbt-spark 0.20.1rc1 (August 2, 2021)

### Fixes
- Fix `get_columns_in_relation` when called on models created in the same run ([#196](https://github.com/dbt-labs/dbt-spark/pull/196), [#197](https://github.com/dbt-labs/dbt-spark/pull/197))

### Contributors
- [@ali-tny](https://github.com/ali-tny) ([#197](https://github.com/fishtown-analytics/dbt-spark/pull/197))


## dbt-spark 0.20.0 (July 12, 2021)

## dbt-spark 0.20.0rc2 (July 7, 2021)

### Features

- Add support for `merge_update_columns` config in `merge`-strategy incremental models ([#183](https://github.com/fishtown-analytics/dbt-spark/pull/183), [#184](https://github.com/fishtown-analytics/dbt-spark/pull/184))

### Fixes

- Fix column-level `persist_docs` on Delta tables, add tests ([#180](https://github.com/fishtown-analytics/dbt-spark/pull/180))

## dbt-spark 0.20.0rc1 (June 8, 2021)

### Features

- Allow user to specify `use_ssl` ([#169](https://github.com/fishtown-analytics/dbt-spark/pull/169))
- Allow setting table `OPTIONS` using `config` ([#171](https://github.com/fishtown-analytics/dbt-spark/pull/171))
- Add support for column-level `persist_docs` on Delta tables ([#84](https://github.com/fishtown-analytics/dbt-spark/pull/84), [#170](https://github.com/fishtown-analytics/dbt-spark/pull/170))

### Fixes
- Cast `table_owner` to string to avoid errors generating docs ([#158](https://github.com/fishtown-analytics/dbt-spark/pull/158), [#159](https://github.com/fishtown-analytics/dbt-spark/pull/159))
- Explicitly cast column types when inserting seeds ([#139](https://github.com/fishtown-analytics/dbt-spark/pull/139), [#166](https://github.com/fishtown-analytics/dbt-spark/pull/166))

### Under the hood
- Parse information returned by `list_relations_without_caching` macro to speed up catalog generation ([#93](https://github.com/fishtown-analytics/dbt-spark/issues/93), [#160](https://github.com/fishtown-analytics/dbt-spark/pull/160))
- More flexible host passing, https:// can be omitted ([#153](https://github.com/fishtown-analytics/dbt-spark/issues/153))

### Contributors
- [@friendofasquid](https://github.com/friendofasquid) ([#159](https://github.com/fishtown-analytics/dbt-spark/pull/159))
- [@franloza](https://github.com/franloza) ([#160](https://github.com/fishtown-analytics/dbt-spark/pull/160))
- [@Fokko](https://github.com/Fokko) ([#165](https://github.com/fishtown-analytics/dbt-spark/pull/165))
- [@rahulgoyal2987](https://github.com/rahulgoyal2987) ([#169](https://github.com/fishtown-analytics/dbt-spark/pull/169))
- [@JCZuurmond](https://github.com/JCZuurmond) ([#171](https://github.com/fishtown-analytics/dbt-spark/pull/171))
- [@cristianoperez](https://github.com/cristianoperez) ([#170](https://github.com/fishtown-analytics/dbt-spark/pull/170))


## dbt-spark 0.19.1 (April 2, 2021)

## dbt-spark 0.19.1b2 (February 26, 2021)

### Under the hood
- Update serialization calls to use new API in dbt-core `0.19.1b2` ([#150](https://github.com/fishtown-analytics/dbt-spark/pull/150))

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
