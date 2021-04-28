# syntax = docker/dockerfile:experimental

#FROM continuumio/miniconda3:4.9.2
#FROM python:3.7-slim-buster
#FROM ubuntu:18.04
FROM ubuntu:20.04 AS builder
#FROM alpine:3.7

# Pass the build variables to the environment.
ARG CONTAINER_VERSION
ENV CONTAINER_VERSION=$CONTAINER_VERSION
ARG BUILD_TAG
ENV BUILD_TAG=$BUILD_TAG
RUN echo "CONTAINER_VERSION=$CONTAINER_VERSION, BUILD_TAG=$BUILD_TAG"

# TODO(gp): Move all this in `devops/docker_build/install_packages.sh`
# so we can create a single smaller layer.

# TODO(gp): Trim this down. npm needed?
RUN apt update && \
    apt install --no-install-recommends -y \
      cifs-utils \
      git \
      keyutils \
      make \
      vim

# This is needed to compile ujson (see https://github.com/alphamatic/lemonade/issues/155)
RUN apt install --no-install-recommends -y build-essential autoconf libtool python3-dev

# Install pip.
RUN apt install --no-install-recommends -y python3-venv python3-pip

# Install poetry.
RUN pip3 install poetry

# Clean up.
RUN apt-get purge -y --auto-remove

# Create conda environment.
ENV ENV_NAME="venv"

ENV APP_DIR=/app
WORKDIR $APP_DIR

# Install requirements.
# Copy the minimum amount of files needed to call install_requirements.sh so we
# can cache it effectively.
COPY devops/docker_build/poetry.lock .
COPY devops/docker_build/poetry.toml .
COPY devops/docker_build/pyproject.toml .
COPY devops/docker_build/install_requirements.sh .

RUN /bin/bash -c "./install_requirements.sh"
# This is not portable across BUILDKIT=1 and BUILDKIT=0 and it's not cached.
#RUN --mount=source=.,target=/amp ./devops/docker_build/install_requirements.sh

COPY devops/docker_build/init_bash.sh .
RUN /bin/bash -c "./init_bash.sh"

# Run repo-specific initialization scripts.
#RUN devops/docker_build/init.sh
# This is not portable across BUILDKIT=1 and BUILDKIT=0 and it's not cached.
#RUN --mount=source=.,target=/... ./devops/docker_build/init.sh

COPY devops/docker_build/install_jupyter_extensions.sh .
RUN /bin/sh -c "./install_jupyter_extensions.sh"

# TODO(gp): Move this in `devops/docker_build/install_packages.sh`, if
# possible.
COPY devops/docker_build/cleanup.sh .
RUN /bin/sh -c "./cleanup.sh"

# Mount external filesystems.
#RUN mkdir -p /s3/alphamatic-data
#RUN mkdir -p /fsx/research

# We assume that the needed files to build the image are under
# devops/{docker_build,docker_scripts}
# We want to minimize the dependencies to avoid to invalidate Docker cache for
# a change in files that doesn't matter for building the image.
ENV DIR="devops/docker_build"
RUN mkdir -p $APP_DIR/$DIR
COPY $DIR $APP_DIR/$DIR

ENV DIR="devops/docker_scripts"
RUN mkdir -p $APP_DIR/$DIR
COPY $DIR $APP_DIR/$DIR

WORKDIR $APP_DIR

ENTRYPOINT ["devops/docker_build/entrypoint.sh"]
