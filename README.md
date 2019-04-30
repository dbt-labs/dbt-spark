## dbt-spark

### Installation
This plugin can be installed via pip:
```
$ pip install dbt-spark
```

### Configuring your profile

A dbt profile can be configured to run against Spark using the following configuration:

| Option  | Description                                        | Required?               | Example                  |
|---------|----------------------------------------------------|-------------------------|--------------------------|
| schema  | Specify the schema (database) to build models into | Required                | `analytics`              |
| host    | The hostname to connect to                         | Required                | `yourorg.sparkhost.com`  |
| port    | The port to connect to the host on                 | Optional (default: 443) | `443`                    |
| token   | The token to use for authenticating to the cluster | Required                | `abc123`                 |
| connect_timeout | The number of seconds to wait before retrying to connect to a Pending Spark cluster | Optional (default: 10) | `60` |
| connect_retries | The number of times to try connecting to a Pending Spark cluster before giving up   | Optional (default: 0)  | `5` |


**Example profiles.yml entry:**
```
your_profile_name:
  target: dev
  outputs:
    dev:
      type: spark
      schema: analytics
      host: yourorg.sparkhost.com
      port: 443
      token: abc123
      cluster: 01234-23423-coffeetime
      connect_retries: 5
      connect_timeout: 60
```

### Usage Notes

**Model Configuration**

The following configurations can be supplied to models run with the dbt-spark plugin:


| Option  | Description                                        | Required?               | Example                  |
|---------|----------------------------------------------------|-------------------------|--------------------------|
| file_format  | The file format to use when creating tables | Optional                | `parquet`              |



**Incremental Models**

Spark does not natively support `delete`, `update`, or `merge` statements. As such, [incremental models](https://docs.getdbt.com/docs/configuring-incremental-models)
are implemented differently than usual in this plugin. To use incremental models, specify a `partition_by` clause in your model config.
dbt will use an `insert overwrite` query to overwrite the partitions included in your query. Be sure to re-select _all_ of the relevant
data for a partition when using incremental models.

```
{{ config(
    materialized='incremental',
    partition_by=['date_day'],
    file_format='parquet'
) }}

/*
  Every partition returned by this query will be overwritten
  when this model runs
*/

select
    date_day,
    count(*) as users

from {{ ref('events') }}
where date_day::date >= '2019-01-01'
group by 1
```

### Reporting bugs and contributing code

-   Want to report a bug or request a feature? Let us know on [Slack](http://slack.getdbt.com/), or open [an issue](https://github.com/fishtown-analytics/dbt-spark/issues/new).

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [PyPA Code of Conduct](https://www.pypa.io/en/latest/code-of-conduct/).
