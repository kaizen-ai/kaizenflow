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
    vim

ENV INSTALL_DIR=/alpha_vantage
WORKDIR $INSTALL_DIR

COPY requirements.txt .
COPY models models/
COPY api api/
COPY list_of_tickers.py .
COPY .env .
COPY main.ipynb .

RUN pip3 install -r requirements.txt

RUN mkdir install/
COPY install_jupy.sh install/
RUN chmod +x ./install/

RUN mkdir /root/.local
RUN mkdir /root/.local/share
RUN mkdir /root/.local/share/jupyter
RUN mkdir /root/.local/share/jupyter/nbextensions
RUN ./install/install_jupy.sh

EXPOSE 8888