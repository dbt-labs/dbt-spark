from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

def create_spark_context(spark_conf):
    spark = SparkSession.builder.enableHiveSupport().config(conf=spark_conf).getOrCreate()
    
    def squared(s):
        return s * s

    spark.udf.register("squaredWithPython", squared, LongType())

    return spark
