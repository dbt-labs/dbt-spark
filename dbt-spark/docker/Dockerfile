# this image gets published to GHCR for production use
ARG py_version=3.11.2

FROM python:$py_version-slim-bullseye AS base

RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    build-essential=12.9 \
    ca-certificates=20210119 \
    gcc=4:10.2.1-1 \
    git=1:2.30.2-1+deb11u2 \
    libpq-dev=13.18-0+deb11u1 \
    libsasl2-dev=2.1.27+dfsg-2.1+deb11u1 \
    make=4.3-4.1 \
    openssh-client=1:8.4p1-5+deb11u3 \
    python-dev-is-python2=2.7.18-9 \
    software-properties-common=0.96.20.2-2.1 \
    unixodbc-dev=2.3.6-0.1+b1 \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

RUN python -m pip install --upgrade "pip==24.0" "setuptools==69.2.0" "wheel==0.43.0" --no-cache-dir


FROM base AS dbt-spark

ARG commit_ref=main
ARG extras=all

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app/dbt/
ENTRYPOINT ["dbt"]

RUN python -m pip install --no-cache-dir "dbt-spark[${extras}] @ git+https://github.com/dbt-labs/dbt-spark@${commit_ref}"
