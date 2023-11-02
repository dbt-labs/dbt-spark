set -e
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    g++ \
    unixodbc-dev \
    libsasl2-modules-gssapi-mit \
    unzip

sudo apt-get install  -y curl
rm -rf /tmp && mkdir /tmp

curl -OL "https://databricks.com/wp-content/uploads/drivers-2020/SimbaSparkODBC-2.6.16.1019-Debian-64bit.zip"

unzip SimbaSparkODBC-2.6.16.1019-Debian-64bit.zip -d /tmp/
sudo dpkg -i /tmp/SimbaSparkODBC-*/*.deb
echo "--------------------------------------------"
sudo  sh -c  echo "[Simba]\nDriver = /opt/simba/spark/lib/64/libsparkodbc_sb64.so" >> /etc/odbcinst.ini

rm -rf /tmp
sudo dpkg -l | grep Simba # confirm that the driver is installed

sudo ldd /opt/simba/spark/lib/64/libsparkodbc_sb64.so
echo "--------------------------------------------"
odbcinst -j
