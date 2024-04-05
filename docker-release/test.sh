# - VERY rudimentary test script to run latest + specific branch image builds and test them all by running `--version`
# TODO: create a real test suite

clear \
&& echo "\n\n"\
"########################################\n"\
"##### Testing dbt-spark latest #####\n"\
"########################################\n"\
&& docker build --tag dbt-spark \
  --target dbt-spark \
  docker \
&& docker run dbt-spark --version \
\
&& echo "\n\n"\
"#########################################\n"\
"##### Testing dbt-spark-1.0.0b1 #####\n"\
"#########################################\n"\
&& docker build --tag dbt-spark-1.0.0b1 \
  --target dbt-spark \
  --build-arg dbt_spark_ref=dbt-spark@v1.0.0b1 \
  docker \
&& docker run dbt-spark-1.0.0b1 --version
