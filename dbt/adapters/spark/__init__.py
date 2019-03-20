from dbt.adapters.spark.connections import SparkConnectionManager
from dbt.adapters.spark.connections import SparkCredentials
from dbt.adapters.spark.relation import SparkRelation
from dbt.adapters.spark.impl import SparkAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import spark

Plugin = AdapterPlugin(
    adapter=SparkAdapter,
    credentials=SparkCredentials,
    include_path=spark.PACKAGE_PATH)
