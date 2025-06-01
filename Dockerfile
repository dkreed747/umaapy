FROM python:3.13

ARG CONNEXTDDS_VERSION=7.5.0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        curl \
        gnupg \
        apt-transport-https && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/share/keyrings && \
    curl -sSL \
      -o /usr/share/keyrings/rti-official-archive.gpg \
      https://packages.rti.com/deb/official/repo.key && \
    printf "deb [arch=$(dpkg --print-architecture), signed-by=/usr/share/keyrings/rti-official-archive.gpg] \
      https://packages.rti.com/deb/official $(. /etc/os-release && echo ${VERSION_CODENAME}) main\n" \
      > /etc/apt/sources.list.d/rti-official.list

RUN echo "rti-connext-dds-${CONNEXTDDS_VERSION}-common rti-connext-dds-${CONNEXTDDS_VERSION}/license/accepted select true" | debconf-set-selections

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      rti-connext-dds-${CONNEXTDDS_VERSION}-services-all && \
    rm -rf /var/lib/apt/lists/* && \
    pip install rti.connext==${CONNEXTDDS_VERSION}

COPY ./rti_license.dat /opt/rti.com/rti_connext_dds-${CONNEXTDDS_VERSION}/rti_license.dat

RUN echo "eval $(rtienv -l /opt/rti.com/rti_connext_dds-${CONNEXTDDS_VERSION}/rti_license.dat)" >> /etc/bash.bashrc