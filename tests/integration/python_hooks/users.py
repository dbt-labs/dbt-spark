
def create_dataframe(spark, table_name, start_time, end_time, **kwargs):
    df = spark.range(1, 5)
    df = df.withColumnRenamed('id', 'user_id')
    return df
