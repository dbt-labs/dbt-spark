#!/bin/bash
set -eo
apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    git \
    curl \
    unixodbc \
    unixodbc-dev \
    libsasl2-modules-gssapi-mit \
    unzip
