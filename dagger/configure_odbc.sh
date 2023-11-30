#!/bin/bash
set -e
apt update && apt install -y --no-install-recommends \
    g++ \
    git \
    curl \
    unixodbc-dev \
    libsasl2-modules-gssapi-mit \
    unzip

rm -rf /tmp && mkdir /tmp

curl -OL "https://databricks.com/wp-content/uploads/drivers-2020/SimbaSparkODBC-2.6.16.1019-Debian-64bit.zip"

unzip SimbaSparkODBC-2.6.16.1019-Debian-64bit.zip -d /tmp/
dpkg -i /tmp/*/simbaspark_2.6.16.1019-2_amd64.deb
echo "--------------------------------------------"
echo sh -c  echo "[Simba]\nDriver = /opt/simba/spark/lib/64/libsparkodbc_sb64.so" >> /etc/odbcinst.ini
dpkg -l | grep Simba # confirm that the driver is installed
rm -rf /tmp
