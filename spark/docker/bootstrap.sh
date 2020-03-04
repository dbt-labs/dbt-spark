#!/bin/bash

set -e  # Fail script on error
mkdir -p /tmp/spark-events
sbin/start-history-server.sh
sbin/start-thriftserver.sh
tail -f logs/*
