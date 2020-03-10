FROM openjdk:8-slim-buster as base

RUN apt-get -y update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Hadoop
ARG HADOOP_VERSION=2.7.7
ARG HADOOP_MINOR_VERSION=2.7

ENV HADOOP_VERSION=$HADOOP_VERSION
ENV HADOOP_MINOR_VERSION=$HADOOP_MINOR_VERSION
ENV HADOOP_HOME=/usr/local/hdp

RUN cd /usr/local && \
    curl -o hadoop-$HADOOP_VERSION.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    chown -R root:root $HADOOP_HOME && \
    chmod -R 777 $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Spark
ARG SPARK_VERSION=2.4.5

ENV SPARK_NAME=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MINOR_VERSION}
ENV SPARK_HOME=/usr/local/spark

RUN cd /tmp && \
  curl -o $SPARK_NAME.tgz http://mirrors.sonic.net/apache/spark/spark-$SPARK_VERSION/$SPARK_NAME.tgz && \
  tar -xzf $SPARK_NAME.tgz -C /tmp --owner root --group root --no-same-owner && \
  rm $SPARK_NAME.tgz && \
  mv /tmp/$SPARK_NAME $SPARK_HOME

# Postgresql driver
RUN curl -o $SPARK_HOME/jars/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.2.10.jar

# ------------------------------------------------
# ------------------ Main stage ------------------
# ------------------------------------------------
FROM base

# Spark
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"

# Hadoop
ENV HADOOP_CONF_DIR=$SPARK_HOME/conf

# General
ENV LANG=C.UTF-8 \
    PATH=$PATH:$SPARK_HOME/bin

# DBT
# this database will be created on start-up if it doesn't exist
ENV DEFAULT_DATABASE=dbt

RUN apt-get update && \
    apt-get install -y procps && \
    adduser --disabled-password --gecos '' hadoop

COPY --from=base $SPARK_HOME $SPARK_HOME
COPY --from=base $HADOOP_HOME $HADOOP_HOME
# AWS jars
COPY --from=base $HADOOP_HOME/share/hadoop/tools/lib/*aws*jar $SPARK_HOME/jars/

# Bootstrap scripts
COPY bootstrap.sh /home/bin/

# Hive config
COPY hive-site.xml $SPARK_HOME/conf

WORKDIR $SPARK_HOME

ENTRYPOINT [ "/home/bin/bootstrap.sh" ]
