ARG base_image=python:3.8.1-slim-buster
FROM $base_image

# Version strings and default paths

ARG SPARK_VERSION=2.4.5
ARG HADOOP_VERSION=2.7.7
ARG HADOOP_VERSION_SHORT=2.7

# Spark
ENV SPARK_VERSION=$SPARK_VERSION \
    SPARK_HOME=/usr/local/spark \
    SPARK_WAREHOUSE_DIR=/usr/local/spark/.warehouse \
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
    # Java 8
    apt-get install -y software-properties-common gnupg2 wget && \
    wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
    add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
    apt-get -y update && \
    mkdir -p /usr/share/man/man1 && \
    apt-get install -y \
    # Core libraries and deps
    apt-transport-https \
    apt-utils \
    ca-certificates \
    iptables \
    libcurl4-openssl-dev \
    libsasl2-dev \
    libssl-dev \
    libxml2-dev \
    lxc \
    openssh-client \
    net-tools \
    curl \
    ps \
    python3-dev \
    build-essential \
    # MySQL (hive metastore)
    default-libmysqlclient-dev \
    default-mysql-server \
    # Java
    adoptopenjdk-8-hotspot && \
    echo JAVA_HOME=$JAVA_HOME && \
    echo "which java=`which java`" && \
    java -version && \
    # Cleanup
    rm -rf /var/lib/apt/lists/*

# Spark
RUN cd /tmp && \
    SPARK_NAME=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT} && \
    curl -o $SPARK_NAME.tgz http://mirrors.sonic.net/apache/spark/spark-${SPARK_VERSION}/$SPARK_NAME.tgz && \
    tar -xzf $SPARK_NAME.tgz -C /tmp --owner root --group root --no-same-owner && \
    rm $SPARK_NAME.tgz && \
    mv /tmp/$SPARK_NAME $SPARK_HOME && \
    mkdir -p $SPARK_WAREHOUSE_DIR


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

# Python libs
RUN pip3 install --upgrade \
    pyhive[hive] \
    pyspark

# dbt-spark
COPY . $DBT_HOME
RUN python3 $DBT_HOME/setup.py install && \
    dbt --version

# Bootstrap scripts
COPY spark/docker/bootstrap.sh /home/bin/
RUN chmod -R 777 /home/bin/*

WORKDIR $SPARK_HOME

ENTRYPOINT [ "/home/bin/bootstrap.sh" ]
