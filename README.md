<p align="center">
  <img src="/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>
<p align="center">
  <a href="https://circleci.com/gh/fishtown-analytics/dbt-spark/tree/master">
    <img src="https://circleci.com/gh/fishtown-analytics/dbt-spark/tree/master.svg?style=svg" alt="CircleCI" />
  </a>
  <a href="https://community.getdbt.com">
    <img src="https://community.getdbt.com/badge.svg" alt="Slack" />
  </a>
</p>

# dbt-spark

This plugin ports [dbt](https://getdbt.com) functionality to Spark. It supports running dbt against Spark clusters that are hosted via Databricks (AWS + Azure), Amazon EMR, or Docker.

We have not tested extensively against older versions of Apache Spark. The plugin uses syntax that requires version 2.2.0 or newer. Some features require Spark 3.0 and/or Delta Lake.

### Documentation
For more information on using Spark with dbt, consult the dbt documentation:
- [Spark profile](https://docs.getdbt.com/reference/warehouse-profiles/spark-profile/)
- [Spark specific configs](https://docs.getdbt.com/reference/resource-configs/spark-configs/)

### Installation
This plugin can be installed via pip. Depending on your connection method, you need to specify an extra requirement.

If connecting to Databricks via ODBC driver, it requires [`pyodbc`](https://github.com/mkleehammer/pyodbc). Depending on your system<sup>1</sup>, you can install it seperately or via pip:

```bash
# Install dbt-spark from PyPi for odbc connections:
$ pip install "dbt-spark[ODBC]"
```

If connecting to a Spark cluster via the generic `thrift` or `http` methods, it requires [`PyHive`](https://github.com/dropbox/PyHive):

```bash
# Install dbt-spark from PyPi for thrift or http connections:
$ pip install "dbt-spark[PyHive]"
```

<sup>1</sup>See the [`pyodbc` wiki](https://github.com/mkleehammer/pyodbc/wiki/Install) for OS-specific installation details.


### Configuring your profile

**Connection Method**

Connections can be made to Spark in three different modes:
- `odbc` is the preferred mode when connecting to Databricks. It supports connecting to a SQL Endpoint or an all-purpose interactive cluster.
- `http` is a more generic mode for connecting to a managed service that provides an HTTP endpoint. Currently, this includes connections to a Databricks interactive cluster.
- `thrift` connects directly to the lead node of a cluster, either locally hosted / on premise or in the cloud (e.g. Amazon EMR).

A dbt profile for Spark connections support the following configurations:

**Key**:
- ✅ Required
- ❌ Not used
- ❔ Optional (followed by `default value` in parentheses)

| Option | Description | ODBC | Thrift | HTTP | Example |
|-|-|-|-|-|-|
| method | Specify the connection method (`odbc` or `thrift` or `http`) | ✅ | ✅ | ✅ | `odbc` |
| schema | Specify the schema (database) to build models into | ✅ | ✅ | ✅ | `analytics` |
| host | The hostname to connect to | ✅ | ✅ | ✅ | `yourorg.sparkhost.com` |
| port | The port to connect to the host on | ❔ (`443`) | ❔ (`443`) | ❔ (`10001`) | `443` |
| token | The token to use for authenticating to the cluster | ✅ | ❌ | ✅ | `abc123` |
| auth  | The value of `hive.server2.authentication`    | ❌ | ❔ | ❌ | `KERBEROS` |
| kerberos_service_name  | Use with `auth='KERBEROS'`     | ❌ | ❔ | ❌ | `hive` |
| organization | Azure Databricks workspace ID (see note) | ❔ | ❌ | ❔ | `1234567891234567` |
| cluster | The name of the cluster to connect to | ✅ (unless `endpoint`) | ❌ | ✅ | `01234-23423-coffeetime` |
| endpoint | The ID of the SQL endpoint to connect to | ✅ (unless `cluster`) | ❌ | ❌ | `1234567891234a` |
| driver | Path of ODBC driver installed or name of the ODBC driver configured | ✅ | ❌ | ❌ | `/opt/simba/spark/lib/64/libsparkodbc_sb64.so` |
| user | The username to use to connect to the cluster | ❔ | ❔ | ❔ | `hadoop` |
| connect_timeout | The number of seconds to wait before retrying to connect to a Pending Spark cluster | ❌ | ❔ (`10`) | ❔ (`10`) | `60` |
| connect_retries | The number of times to try connecting to a Pending Spark cluster before giving up | ❌ | ❔ (`0`) | ❔ (`0`)  | `5` |
| use_ssl | The value of `hive.server2.use.SSL` (`True` or `False`). Default ssl store (ssl.get_default_verify_paths()) is the valid location for SSL certificate | ❌ | ❔ (`False`) | ❌ | `True` |

**Databricks** connections differ based on the cloud provider:

- **Organization:** To connect to an Azure Databricks cluster, you will need to obtain your organization ID, which is a unique ID Azure Databricks generates for each customer workspace.  To find the organization ID, see https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect#step-2-configure-connection-properties. This is a string field; if there is a leading zero, be sure to include it.

- **Host:** The host field for Databricks can be found at the start of your workspace or cluster url: `region.azuredatabricks.net` for Azure, or `account.cloud.databricks.com` for AWS. Do not include `https://`.

**Amazon EMR**: To connect to Spark running on an Amazon EMR cluster, you will need to run `sudo /usr/lib/spark/sbin/start-thriftserver.sh` on the master node of the cluster to start the Thrift server (see https://aws.amazon.com/premiumsupport/knowledge-center/jdbc-connection-emr/ for further context). You will also need to connect to port `10001`, which will connect to the Spark backend Thrift server; port `10000` will instead connect to a Hive backend, which will not work correctly with dbt.


**Example profiles.yml entries:**

**ODBC**
```
your_profile_name:
  target: dev
  outputs:
    dev:
      type: spark
      method: odbc
      driver: path/to/driver
      host: yourorg.databricks.com
      organization: 1234567891234567    # Azure Databricks only
      port: 443                         # default
      token: abc123
      schema: analytics

      # one of:
      cluster: 01234-23423-coffeetime
      endpoint: coffee01234time
```

**Thrift**
```
your_profile_name:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      host: 127.0.0.1
      port: 10001                         # default
      schema: analytics
      
      # optional
      user: hadoop
      auth: KERBEROS
      kerberos_service_name: hive
      connect_retries: 5
      connect_timeout: 60
```


**HTTP**
```
your_profile_name:
  target: dev
  outputs:
    dev:
      type: spark
      method: http
      host: yourorg.sparkhost.com
      organization: 1234567891234567    # Azure Databricks only
      port: 443                         # default
      token: abc123
      schema: analytics
      cluster: 01234-23423-coffeetime

      # optional
      connect_retries: 5
      connect_timeout: 60
```


### Usage Notes

**Model Configuration**

The following configurations can be supplied to models run with the dbt-spark plugin:


| Option               | Description                                                                                                                                                 | Required?                               | Example              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- | -------------------- |
| file_format          | The file format to use when creating tables (`parquet`, `delta`, `csv`, `json`, `text`, `jdbc`, `orc`, `hive` or `libsvm`).                                 | Optional                                | `parquet`            |
| location_root        | The created table uses the specified directory to store its data. The table alias is appended to it.                                                        | Optional                                | `/mnt/root`          |
| partition_by         | Partition the created table by the specified columns. A directory is created for each partition.                                                            | Optional                                | `partition_1`        |
| clustered_by         | Each partition in the created table will be split into a fixed number of buckets by the specified columns.                                                  | Optional                                | `cluster_1`          |
| buckets              | The number of buckets to create while clustering                                                                                                            | Required if `clustered_by` is specified | `8`                  |
| incremental_strategy | The strategy to use for incremental models (`append`, `insert_overwrite`, or `merge`). | Optional (default: `append`)  | `merge`              |
| persist_docs         | Whether dbt should include the model description as a table or column `comment`                                                                                       | Optional                                | `{'relation': true, 'columns': true}` |


**Incremental Models**

dbt has a number of ways to build models incrementally, called "incremental strategies." Some strategies depend on certain file formats, connection types, and other model configurations:
- `append` (default): Insert new records without updating or overwriting any existing data.
- `insert_overwrite`: If `partition_by` is specified, overwrite partitions in the table with new data. (Be sure to re-select _all_ of the relevant data for a partition.) If no `partition_by` is specified, overwrite the entire table with new data.  [Cannot be used with `file_format: delta` or when connectinng via Databricks SQL Endpoints. For dynamic partition replacement with `method: odbc` + Databricks `cluster`, you must you **must** include `set spark.sql.sources.partitionOverwriteMode DYNAMIC` in the [cluster SparkConfig](https://docs.databricks.com/clusters/configure.html#spark-config). For atomic replacement of Delta tables, use the `table` materialization instead.]
- `merge`: Match records based on a `unique_key`; update old records, insert new ones. (If no `unique_key` is specified, all new data is inserted, similar to `append`.) [Requires `file_format: delta`. Available only on Databricks Runtime.]

Examples:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}


--  All rows returned by this query will be appended to the existing table

select * from {{ ref('events') }}
{% if is_incremental() %}
  where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
```

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by=['date_day'],
    file_format='parquet'
) }}

-- Every partition returned by this query will overwrite existing partitions

select
    date_day,
    count(*) as users

from {{ ref('events') }}
{% if is_incremental() %}
  where date_day > (select max(date_day) from {{ this }})
{% endif %}
group by 1
```

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    file_format='delta'
) }}

-- Existing events, matched on `event_id`, will be updated
-- New events will be appended

select * from {{ ref('events') }}
{% if is_incremental() %}
  where date_day > (select max(date_day) from {{ this }})
{% endif %}
```

### Running locally

A `docker-compose` environment starts a Spark Thrift server and a Postgres database as a Hive Metastore backend.

```
docker-compose up
```

Create a profile like this one:

```
spark-testing:
  target: local
  outputs:
    local:
      type: spark
      method: thrift
      host: 127.0.0.1
      port: 10000
      user: dbt
      schema: analytics
      connect_retries: 5
      connect_timeout: 60
```

Connecting to the local spark instance:

* The Spark UI should be available at [http://localhost:4040/sqlserver/](http://localhost:4040/sqlserver/)
* The endpoint for SQL-based testing is at `http://localhost:10000` and can be referenced with the Hive or Spark JDBC drivers using connection string `jdbc:hive2://localhost:10000` and default credentials `dbt`:`dbt`

Note that the Hive metastore data is persisted under `./.hive-metastore/`, and the Spark-produced data under `./.spark-warehouse/`. To completely reset you environment run the following:

```
docker-compose down
rm -rf ./.hive-metastore/
rm -rf ./.spark-warehouse/
```

### Reporting bugs and contributing code

-   Want to report a bug or request a feature? Let us know on [Slack](http://slack.getdbt.com/), or open [an issue](https://github.com/fishtown-analytics/dbt-spark/issues/new).

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [PyPA Code of Conduct](https://www.pypa.io/en/latest/code-of-conduct/).
