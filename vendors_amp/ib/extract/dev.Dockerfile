FROM python:3.7-slim-buster

RUN apt-get update \
  && apt-get install -y vim

COPY vendors_amp/ib/extract/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

WORKDIR /amp
