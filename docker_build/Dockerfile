# syntax = docker/dockerfile:experimental

# Do not forget to use:
# `export DOCKER_BUILDKIT=1`
# To build an image run:
# > docker build --progress=plain -t particle:test -f docker_build/Dockerfile .
FROM continuumio/anaconda3:2020.02
# Install / remove directories and prepare filesystem.
# Don't forget credential for auth.
RUN apt update && \
    apt install cifs-utils -y && \
    apt install keyutils -y && \
    apt install npm -y && \
    apt install make -y && \
    apt install graphviz -y && \
    apt install vim -y && \
    apt install s3fs -y

RUN npm install --global prettier
# Nessesery for openjdk whici is dependency for plantuml
# https://github.com/geerlingguy/ansible-role-java/issues/64#issuecomment-393299088
#RUN mkdir -p /usr/share/man/man1
#RUN apt install plantuml -y
#RUN curl https://arelle.org/arelle/download/1019/ -o arelle.tar.gz && \
#    mkdir /usr/bin/arelle && \
#    tar zxvf arelle.tar.gz -C /usr/bin/arelle && \
#    rm -f arelle.tar.gz

ARG req=requirements_dev.yaml

RUN mkdir -p /s3/default00-bucket
RUN mkdir -p /fsx/research

RUN conda init bash
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
