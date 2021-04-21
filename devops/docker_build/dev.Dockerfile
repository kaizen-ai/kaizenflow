# syntax = docker/dockerfile:experimental

#FROM continuumio/miniconda3:4.9.2
#FROM python:3.7-slim-buster
#FROM ubuntu:18.04
FROM ubuntu:20.04 AS builder
#FROM alpine:3.7

# TODO(gp): Trim this down. npm needed?
RUN apt update && \
    apt install --no-install-recommends -y \
      cifs-utils \
      git \
      keyutils \
      make \
      vim

# apt install -y npm && \
# apt install -y s3fs && \
# apt install -y graphviz && \

# This is needed to compile ujson (see https://github.com/alphamatic/lemonade/issues/155)
RUN apt install --no-install-recommends -y build-essential autoconf libtool python3-dev

# Install pip.
RUN apt install --no-install-recommends -y python3-venv python3-pip

# Install poetry.
RUN pip3 install poetry

# Clean up.
RUN apt-get purge -y --auto-remove

# Configure conda.
#RUN conda init bash
#RUN conda config --set default_threads 4
#RUN conda config --add channels conda-forge
#RUN conda config --show-sources

# Create conda environment.
ENV ENV_NAME="venv"
#RUN conda create -n $ENV_NAME python=3.7 -y

ENV APP_DIR=/app
WORKDIR $APP_DIR

# Install requirements.
# Copy the minimum amount of files needed to call install_requirements.sh so we
# can cache it effectively.
COPY devops/docker_build/pyproject.toml .
COPY devops/docker_build/poetry.lock .
COPY devops/docker_build/install_requirements.sh .
COPY poetry.toml .

RUN /bin/bash -c "./install_requirements.sh"
# This is not portable across BUILDKIT=1 and BUILDKIT=0 and it's not cached.
#RUN --mount=source=.,target=/amp ./devops/docker_build/install_requirements.sh

# Run repo-specific initialization scripts.
#RUN devops/docker_build/init.sh
# This is not portable across BUILDKIT=1 and BUILDKIT=0 and it's not cached.
#RUN --mount=source=.,target=/... ./devops/docker_build/init.sh

COPY devops/docker_build/install_jupyter_extensions.sh .
RUN /bin/sh -c "./install_jupyter_extensions.sh"

COPY devops/docker_build/cleanup.sh .
RUN /bin/sh -c "./cleanup.sh"

#
# Multi-stage build.
#

FROM ubuntu:20.04

# TODO(gp): Trim this down. npm needed?
RUN apt update && \
    apt install --no-install-recommends -y \
      cifs-utils \
      git \
      keyutils \
      make \
      vim

# Clean up.
RUN apt-get purge -y --auto-remove

# Mount external filesystems.
RUN mkdir -p /s3/default00-bucket
RUN mkdir -p /fsx/research

# We don't mount this yet.
#COPY devops/docker_build/fstab /etc/fstab

# Without this, Docker errors out with "cannot normalize nothing".
ENV APP_DIR=/app

COPY --from=builder $APP_DIR $APP_DIR
COPY --from=builder /root /root

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
