# syntax = docker/dockerfile:experimental

# Do not forget to use:
# `export DOCKER_BUILDKIT=1`
# To build an image run:
# > docker build --progress=plain -t particle:test -f docker_build/Dockerfile .
FROM continuumio/miniconda3:4.9.2
# Install / remove directories and prepare filesystem.
# Don't forget credential for auth.
RUN apt update && \
    apt install cifs-utils -y && \
    apt install keyutils -y && \
    apt install make -y && \
    apt install graphviz -y && \
    apt install vim -y && \
    apt install git -y && \
    apt install s3fs -y && \
    apt install npm -y && \
    apt-get purge -y --auto-remove

RUN npm install --global prettier
RUN mkdir -p /s3/default00-bucket
RUN mkdir -p /fsx/research
COPY docker_build/fstab /etc/fstab

RUN conda init bash
RUN conda config --set default_threads 4
RUN conda config --add channels conda-forge
RUN conda config --show-sources
ENV ENV_DIR="/env_dir"
RUN mkdir $ENV_DIR
ENV APP_DIR=/amp
WORKDIR $APP_DIR
ENV ENV_NAME="venv"
RUN conda create -n $ENV_NAME python=3.7 -y
# Install requirements.
RUN --mount=source=.,target=/amp docker_build/install_requirements.sh
# Run repo-specific initialization scripts.
RUN --mount=source=.,target=/amp docker_build/init.sh
RUN echo "conda activate venv" >> ~/.bashrc
ENTRYPOINT ["./docker_build/entrypoint.sh"]
