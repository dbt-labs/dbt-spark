# ARG base_image=ubuntu:18.04
ARG base_image=python:3.8
FROM $base_image

USER root

# Version strings and default paths
ENV SPARK_VERSION=2.4.5 \
    HADOOP_VERSION=2.7 \
    HADOOP_HOME=/usr/local/hdp \
    SPARK_HOME=/usr/local/spark \
    DBT_HOME=/usr/local/dbt-spark \
    HOME=/home
WORKDIR $HOME

### Use spark/conf instead of hadoop/shared/conf:
ENV HADOOP_CONF_DIR /usr/local/spark/conf

# Prevents some log trimming:
ENV PYTHONUNBUFFERED 1

# Install core libraries and dependencies
RUN apt-get -y update && \
    apt-get install -y \
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
    wget && \
    rm -rf /var/lib/apt/lists/*

# Install Java
RUN apt-get -y update && \
    apt-get install -y \
    default-jre && \
    rm -rf /var/lib/apt/lists/* && \
    echo JAVA_HOME=$JAVA_HOME && \
    echo "which java=`which java`" && \
    java -version

# RUN update-alternatives --config java
# RUN apt-get -y update && \
#     apt-get install -y \
#     -t jessie-backports \
#     openjdk-8-jre-headless && \
#     rm -rf /var/lib/apt/lists/*

# Download spark
RUN cd /tmp && \
    SPARK_NAME=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} && \
    curl -o $SPARK_NAME.tgz http://mirrors.sonic.net/apache/spark/spark-${SPARK_VERSION}/$SPARK_NAME.tgz && \
    tar -xzf $SPARK_NAME.tgz -C /usr/local --owner root --group root --no-same-owner && \
    rm $SPARK_NAME.tgz && \
    cd /usr/local && \
    ln -s $SPARK_NAME spark

# Spark config
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip \
    SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH=$PATH:$SPARK_HOME/bin

# Install mysql server and drivers (for hive metastore)
RUN apt-get update && \
    apt-get install -y \
    default-libmysqlclient-dev \
    default-mysql-server

# Install Docker
RUN curl -sSL https://get.docker.com/ | sh

# Install Hadoop
RUN cd /usr/local && \
    HADOOP_MINOR_VERSION=2.7.7 && \
    curl -o hadoop-$HADOOP_MINOR_VERSION.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_MINOR_VERSION/hadoop-$HADOOP_MINOR_VERSION.tar.gz && \
    tar -xzf hadoop-$HADOOP_MINOR_VERSION.tar.gz && \
    mv hadoop-$HADOOP_MINOR_VERSION $HADOOP_HOME && \
    chown -R root:root $HADOOP_HOME && \
    chmod -R 777 $HADOOP_HOME && \
    rm hadoop-$HADOOP_MINOR_VERSION.tar.gz

# Copy hadoop libraries for AWS & S3 to spark classpath
RUN echo -e "HADOOP_HOME=$HADOOP_HOME\nSPARK_HOME=$SPARK_HOME" && \
    find $HADOOP_HOME/share/hadoop/tools/lib/ -name "*aws*.jar" && \
    cp `find $HADOOP_HOME/share/hadoop/tools/lib/ -name "*aws*.jar"` $SPARK_HOME/jars/ && \
    find $SPARK_HOME/jars -name "*aws*.jar" -print

# Not working:
# # Copy mysql jdbc driver into spark classpath
# RUN find / -name "*mysql-connector-java*.jar" && \
#     cp `find /usr/ -name "*mysql-connector-java*.jar"` $SPARK_HOME/jars/ && \
#     find $SPARK_HOME/jars -name "*mysql-connector-java*.jar" -print

# Install Delta Lake for Spark
RUN cd $SPARK_HOME/bin && echo "print('Hello, Delta Lake!')" > pydummy.py && \
    ./spark-submit \
    --packages io.delta:delta-core_2.11:0.4.0 \
    --conf spark.yarn.submit.waitAppCompletion=false pydummy.py

# Install python libraries
RUN pip3 install --upgrade \
    autocorrect \
    docker \
    boto3 \
    fire \
    junit-xml \
    numpy \
    pandas \
    pyarrow \
    pyhive[hive] \
    pyspark \
    s3fs \
    tqdm \
    xmlrunner

# ENV SCRATCH_DIR /tmp/scratch
# RUN mkdir -p $SCRATCH_DIR

COPY spark/docker/bootstrap.sh /home/bin/
RUN chmod -R 777 /home/bin/*

# Install DBT-Spark
ENV VENV_ROOT=$HOME/venv
COPY . $DBT_HOME
WORKDIR $DBT_HOME
RUN python3 -m venv ${VENV_ROOT}/dbt-spark && \
    VENV_BIN=${VENV_ROOT}/dbt-spark/bin && \
    ${VENV_BIN}/python3 setup.py install && \
    ln -s ${VENV_BIN}/dbt /usr/bin/dbt-spark && \
    cd / && \
    dbt-spark --version

ENTRYPOINT [ "/home/bin/bootstrap.sh" ]
