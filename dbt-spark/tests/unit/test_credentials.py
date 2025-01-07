from dbt.adapters.spark.connections import SparkConnectionMethod, SparkCredentials


def test_credentials_server_side_parameters_keys_and_values_are_strings() -> None:
    credentials = SparkCredentials(
        host="localhost",
        method=SparkConnectionMethod.THRIFT,
        database="tests",
        schema="tests",
        server_side_parameters={"spark.configuration": 10},
    )
    assert credentials.server_side_parameters["spark.configuration"] == "10"
