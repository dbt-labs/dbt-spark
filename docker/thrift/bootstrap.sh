#!/bin/bash

set -e  # Fail script on error

mkdir -p /tmp/spark-events

echo "Waiting for hive-metastore to be ready.."
while !</dev/tcp/hive-metastore/5432
do
    echo "hive-metastore was not ready yet.."
    sleep 2
done

echo "starting Spark Thrift Server.."
sbin/start-thriftserver.sh

echo "Creating default database (if not exists) ${DEFAULT_DATABASE}"
bin/spark-sql -e "CREATE DATABASE IF NOT EXISTS ${DEFAULT_DATABASE}"

tail -f logs/*
