# ARG base_image=ubuntu:18.04
ARG base_image=python:3.8
FROM $base_image

# Version strings and default paths

ARG SPARK_VERSION=2.4.5
ARG HADOOP_VERSION=2.7.7
ARG HADOOP_VERSION_SHORT=2.7

# Spark
ENV SPARK_VERSION=$SPARK_VERSION \
    SPARK_HOME=/usr/local/spark \
    SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"

# Hadoop
ENV HADOOP_VERSION=$HADOOP_VERSION \
    HADOOP_VERSION_SHORT=${HADOOP_VERSION_SHORT} \
    HADOOP_HOME=/usr/local/hdp \
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

RUN apt-get -y update && \
    apt-get install -y \
    # Core libraries and deps
    apt-transport-https \
    apt-utils \
    ca-certificates \
    ca-certificates-java \
    iptables \
    libcurl4-openssl-dev \
    libsasl2-dev \
    libssl-dev \
    libxml2-dev \
    lxc \
    openssh-client \
    net-tools \
    wget \
    # MySQL (hive metastore)
    default-libmysqlclient-dev \
    default-mysql-server \
    # Java
    default-jre && \
    echo JAVA_HOME=$JAVA_HOME && \
    echo "which java=`which java`" && \
    java -version && \
    # Cleanup
    rm -rf /var/lib/apt/lists/*

# Spark
RUN cd /tmp && \
    SPARK_NAME=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT} && \
    curl -o $SPARK_NAME.tgz http://mirrors.sonic.net/apache/spark/spark-${SPARK_VERSION}/$SPARK_NAME.tgz && \
    tar -xzf $SPARK_NAME.tgz -C /usr/local --owner root --group root --no-same-owner && \
    rm $SPARK_NAME.tgz && \
    cd /usr/local && \
    ln -s $SPARK_NAME spark

# Hadoop
RUN cd /usr/local && \
    HADOOP_MINOR_VERSION=2.7.7 && \
    curl -o hadoop-$HADOOP_MINOR_VERSION.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_MINOR_VERSION/hadoop-$HADOOP_MINOR_VERSION.tar.gz && \
    tar -xzf hadoop-$HADOOP_MINOR_VERSION.tar.gz && \
    mv hadoop-$HADOOP_MINOR_VERSION $HADOOP_HOME && \
    chown -R root:root $HADOOP_HOME && \
    chmod -R 777 $HADOOP_HOME && \
    rm hadoop-$HADOOP_MINOR_VERSION.tar.gz

# AWS jars
RUN echo -e "HADOOP_HOME=$HADOOP_HOME\nSPARK_HOME=$SPARK_HOME" && \
    find $HADOOP_HOME/share/hadoop/tools/lib/ -name "*aws*.jar" && \
    cp `find $HADOOP_HOME/share/hadoop/tools/lib/ -name "*aws*.jar"` $SPARK_HOME/jars/ && \
    find $SPARK_HOME/jars -name "*aws*.jar" -print

# Delta Lake
RUN cd $SPARK_HOME/bin && echo "print('Hello, Delta Lake!')" > pydummy.py && \
    ./spark-submit \
    --packages io.delta:delta-core_2.11:0.4.0 \
    --conf spark.yarn.submit.waitAppCompletion=false pydummy.py

# Python libs
RUN pip3 install --upgrade \
    pyhive[hive] \
    pyspark

# Bootstrap scripts
COPY spark/docker/bootstrap.sh /home/bin/
RUN chmod -R 777 /home/bin/*

# dbt-spark
COPY . $DBT_HOME
RUN python3 $DBT_HOME/setup.py install && \
    dbt --version

ENTRYPOINT [ "/home/bin/bootstrap.sh" ]
