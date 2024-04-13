# this image does not get published, it is intended for local development only, see `Makefile` for usage
FROM ubuntu:22.04 as base

# prevent python installation from asking for time zone region
ARG DEBIAN_FRONTEND=noninteractive

# add python repository
RUN apt-get update \
  && apt-get install -y software-properties-common=0.99.22.9 \
  && add-apt-repository -y ppa:deadsnakes/ppa \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# install python
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential=12.9ubuntu3 \
    gcc=4:11.2.0-1ubuntu1 \
    git-all=1:2.34.1-1ubuntu1.10 \
    libsasl2-dev=2.1.27+dfsg2-3ubuntu1.2 \
    python3.10=3.10.12-1~22.04.3 \
    python3.10-dev=3.10.12-1~22.04.3 \
    python3.10-venv=3.10.12-1~22.04.3 \
    python3-distutils=3.10.8-1~22.04 \
    python3-pip=22.0.2+dfsg-1ubuntu0.4 \
    python3-wheel=0.37.1-2ubuntu0.22.04.1 \
    unixodbc-dev=2.3.9-5ubuntu0.1 \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# update the default system interpreter to the newly installed version
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1

# install python dependencies
RUN python3 -m pip install --upgrade --no-cache-dir  \
    "beartype==0.17.2" \
    "dagger-io==0.10.3" \
    "python-dotenv==1.0.1"


FROM base as dbt-spark-dev

HEALTHCHECK CMD python3 --version || exit 1

# send stdout/stderr to terminal
ENV PYTHONUNBUFFERED=1

# setup mount for local code
WORKDIR /opt/code
VOLUME /opt/code

# create a virtual environment
RUN python3 -m venv /opt/venv
