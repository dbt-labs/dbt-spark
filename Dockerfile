# global args
ARG SPARK_VERSION=2.4.5
ARG HADOOP_VERSION=2.7.7
ARG HADOOP_MINOR_VERSION=2.7
ARG HADOOP_HOME=/usr/local/hdp
ARG SPARK_NAME=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MINOR_VERSION}
ARG SPARK_HOME=/usr/local/spark
ARG SPARK_WAREHOUSE_DIR=$SPARK_HOME/.warehouse

FROM debian:buster-slim as builder

RUN apt-get -y update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Spark
ARG SPARK_VERSION
ARG SPARK_NAME
ARG SPARK_HOME
ARG SPARK_WAREHOUSE_DIR

RUN cd /tmp && \
    curl -o $SPARK_NAME.tgz http://mirrors.sonic.net/apache/spark/spark-$SPARK_VERSION/$SPARK_NAME.tgz && \
    tar -xzf $SPARK_NAME.tgz -C /tmp --owner root --group root --no-same-owner && \
    rm $SPARK_NAME.tgz && \
    mv /tmp/$SPARK_NAME $SPARK_HOME && \
    mkdir -p $SPARK_WAREHOUSE_DIR

# Hadoop
ARG HADOOP_VERSION
ARG HADOOP_MINOR_VERSION
ARG HADOOP_HOME

RUN cd /usr/local && \
    curl -o hadoop-$HADOOP_VERSION.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    chown -R root:root $HADOOP_HOME && \
    chmod -R 777 $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Postgresql driver
RUN curl -o $SPARK_HOME/jars/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.2.10.jar

# ------------------------------------------------
# ------------------ Main stage ------------------
# ------------------------------------------------
FROM python:3.8.1-slim-buster

ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG HADOOP_MINOR_VERSION
ARG HADOOP_HOME
ARG SPARK_NAME
ARG SPARK_HOME
ARG SPARK_WAREHOUSE_DIR

# Spark
ENV SPARK_VERSION=$SPARK_VERSION \
    SPARK_HOME=$SPARK_HOME \
    SPARK_WAREHOUSE_DIR=$SPARK_WAREHOUSE_DIR \
    SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"

# Hadoop
ENV HADOOP_VERSION=$HADOOP_VERSION \
    HADOOP_MINOR_VERSION=$HADOOP_MINOR_VERSION \
    HADOOP_HOME=$HADOOP_HOME \
    HADOOP_CONF_DIR=/usr/local/spark/conf

# Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip

# DBT
ENV DBT_HOME=/usr/local/dbt-spark

# General
ENV LANG=C.UTF-8 \
    PATH=$PATH:$SPARK_HOME/bin

COPY --from=builder $SPARK_HOME $SPARK_HOME
COPY --from=builder $HADOOP_HOME $HADOOP_HOME
# AWS jars
COPY --from=builder $HADOOP_HOME/share/hadoop/tools/lib/*aws*jar $SPARK_HOME/jars/

RUN apt-get -y update && \
  # Java 8
  apt-get install -y software-properties-common gnupg2 wget && \
  wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
  add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
  apt-get -y update && \
  mkdir -p /usr/share/man/man1 && \
  apt-get install -y \
  # Core libraries and deps
  libsasl2-dev \
  procps \
  python3-dev \
  build-essential \
  # Java
  adoptopenjdk-8-hotspot && \
  echo JAVA_HOME=$JAVA_HOME && \
  echo "which java=`which java`" && \
  java -version && \
  # Cleanup
  rm -rf /var/lib/apt/lists/*

# Python libs
RUN pip3 install --upgrade \
    pyhive[hive] \
    pyspark

# dbt-spark
COPY . $DBT_HOME
RUN python3 $DBT_HOME/setup.py install && \
    dbt --version

# Bootstrap scripts
COPY docker/bootstrap.sh /home/bin/
RUN chmod -R 777 /home/bin/*

# Hive config
COPY docker/hive-site.xml $SPARK_HOME/conf

WORKDIR $SPARK_HOME

ENTRYPOINT [ "/home/bin/bootstrap.sh" ]
