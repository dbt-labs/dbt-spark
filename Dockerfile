FROM fishtownjacob/test-container:4

USER root

RUN apt-get update \
 && apt-get dist-upgrade -y \
 && apt-get install -y  --no-install-recommends \
      libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit \
 && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

USER dbt_test_user
