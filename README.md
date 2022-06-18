# dbt-spark-livy

The `dbt-spark-livy` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache spark-livy](https://spark.apache.org/) and [Cloudera Data Platform](https://cloudera.com) with Livy server support. This code bases use the dbt-spark project (https://github.com/dbt-labs/dbt-spark), and provides a Livy connectivity support over it. 

## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Requirements

Python >= 3.8

dbt-core >= 1.1.0

pyspark

sqlparams

### Installing dbt-spark-livy

`pip install dbt-spark-livy`

### Profile Setup

```
demo_project:
  target: dev
  outputs:
    dev:
     type: spark
     method: livy
     schema: my_db
     host: https://spark-livy-gateway.my.org.com/dbt-spark/cdp-proxy-api/livy_for_spark3/
     user: my_user
     password: my_pass
```

## Supported features
Please see the original adapter documentation: https://github.com/dbt-labs/dbt-spark and https://docs.getdbt.com/reference/warehouse-profiles/spark-profile
