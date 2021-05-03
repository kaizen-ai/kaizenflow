# syntax = docker/dockerfile:experimental

#FROM continuumio/miniconda3:4.9.2
#FROM python:3.7-slim-buster
#FROM ubuntu:18.04
FROM ubuntu:20.04 AS builder
#FROM alpine:3.7

# Clean up the installation.
ENV CLEAN_UP_INSTALLATION=True

# Pass the build variables to the environment.
ARG CONTAINER_VERSION
ENV CONTAINER_VERSION=$CONTAINER_VERSION
RUN echo "CONTAINER_VERSION=$CONTAINER_VERSION"

# Install the needed packages.
COPY devops/docker_build/install_packages.sh .
RUN /bin/bash -c "./install_packages.sh"

# Name of the virtual environment to create.
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

COPY devops/docker_build/install_jupyter_extensions.sh .
RUN /bin/sh -c "./install_jupyter_extensions.sh"

# Mount external filesystems.
#RUN mkdir -p /s3/alphamatic-data
#RUN mkdir -p /fsx/research

# TODO(gp): Is this stuff needed?
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

# This is the last step since the build tag contains a timestamp that might
# trigger a re-build even though nothing has changed.
ARG BUILD_TAG
ENV BUILD_TAG=$BUILD_TAG
RUN echo "BUILD_TAG=$BUILD_TAG"

# TODO(gp): Is this needed?
WORKDIR $APP_DIR

ENTRYPOINT ["devops/docker_build/entrypoint.sh"]
