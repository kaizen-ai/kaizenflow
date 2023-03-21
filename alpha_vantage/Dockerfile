# syntax = docker/dockerfile:experimental

FROM ubuntu:20.04 AS builder
FROM python:3.10-slim-buster
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get install gcc python3-dev -y

RUN apt install -y --no-install-recommends --fix-missing \
    sudo \
    curl \
    gnupg \
    git \
    vim \
    jupyter-notebook

ENV INSTALL_DIR=/alpha_vantage
WORKDIR $INSTALL_DIR

COPY requirements.txt .
COPY models models/
COPY api api/
COPY list_of_tickers.py .
COPY .env .
COPY main.ipynb .

RUN pip3 install -r requirements.txt

EXPOSE 8888

ENTRYPOINT (jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root --NotebookApp.token='') && \
        /bin/bash