ARG BASE_IMAGE=python
ARG BASE_IMAGE_TAG=3.12
FROM ${BASE_IMAGE}:${BASE_IMAGE_TAG} AS develop

ENV DEBIAN_FRONTEND=noninteractive

ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ARG CYCLONEDDS_VERSION=0.10.5
ENV CYCLONEDDS_HOME=/usr/local
ENV CMAKE_PREFIX_PATH=/usr/local

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      git \
      curl \
      gnupg \
      locales \
      ca-certificates \
      xauth \
      libgtk-3-0 \
      dbus-x11 \
      at-spi2-core \
      apt-transport-https \
      build-essential \
      cmake \
      ninja-build \
      pkg-config && \
    sed -i 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen && \
    rm -rf /var/lib/apt/lists/*

RUN git clone --branch ${CYCLONEDDS_VERSION} --depth 1 https://github.com/eclipse-cyclonedds/cyclonedds /tmp/cyclonedds && \
    cmake -S /tmp/cyclonedds -B /tmp/cyclonedds/build -DCMAKE_INSTALL_PREFIX=${CYCLONEDDS_HOME} && \
    cmake --build /tmp/cyclonedds/build --target install --parallel && \
    rm -rf /tmp/cyclonedds

COPY requirements.txt .

RUN apt-get update && \
    rm -rf /var/lib/apt/lists/* && \
    pip install -r requirements.txt
