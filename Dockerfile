ARG BASE_IMAGE=python
ARG BASE_IMAGE_TAG=3.13
ARG CONNEXTDDS_VERSION=7.5.0

FROM ${BASE_IMAGE}:${BASE_IMAGE_TAG} AS develop

ARG CONNEXTDDS_VERSION
ENV DEBIAN_FRONTEND=noninteractive

# RTI
ENV CONNEXTDDS_VERSION=${CONNEXTDDS_VERSION}
ENV CONNEXTDDS_ARCH=x64Linux4gcc7.3.0
ENV CONNEXTDDS_DIR=/opt/rti.com/rti_connext_dds-${CONNEXTDDS_VERSION}
ENV NDDSHOME=${CONNEXTDDS_DIR}
ENV CONNEXTDDS_ENV=${NDDSHOME}/resource/scripts/rtisetenv_x64Linux4gcc7.3.0.sh
ENV RTI_LICENSE_FILE=${NDDSHOME}/rti_license.dat
ENV LD_LIBRARY_PATH=/opt/rti.com/rti_connext_dds-7.5.0/lib/x64Linux4gcc7.3.0:/opt/rti.com/rti_connext_dds-7.5.0/third_party/openssl-3.0.12/x64Linux4gcc7.3.0/release/lib:/opt/rti.com/rti_connext_dds-7.5.0/third_party/openssl-/x64Linux4gcc7.3.0/release/lib:/opt/rti.com/rti_connext_dds-7.5.0/third_party/civetweb-/x64Linux4gcc7.3.0/release/lib:${LD_LIBRARY_PATH}
ENV PATH=/opt/rti.com/rti_connext_dds-7.5.0/bin::${PATH}
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      git \
      curl \
      gnupg \
      locales \
      xauth \
      libgtk-3-0 \
      dbus-x11 \
      at-spi2-core \
      apt-transport-https && \
    sed -i 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/share/keyrings && \
    curl -sSL \
      -o /usr/share/keyrings/rti-official-archive.gpg \
      https://packages.rti.com/deb/official/repo.key && \
    printf "deb [arch=$(dpkg --print-architecture), signed-by=/usr/share/keyrings/rti-official-archive.gpg] \
      https://packages.rti.com/deb/official $(. /etc/os-release && echo ${VERSION_CODENAME}) main\n" \
      > /etc/apt/sources.list.d/rti-official.list

RUN echo "rti-connext-dds-${CONNEXTDDS_VERSION}-common rti-connext-dds-${CONNEXTDDS_VERSION}/license/accepted select true" | debconf-set-selections

COPY requirements.txt .

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      rti-connext-dds-${CONNEXTDDS_VERSION} \
      rti-connext-dds-${CONNEXTDDS_VERSION}-tools-all \
      rti-connext-dds-${CONNEXTDDS_VERSION}-services-all && \
    rm -rf /var/lib/apt/lists/* && \
    pip install -r requirements.txt
