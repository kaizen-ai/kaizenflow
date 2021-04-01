FROM python:3.7-slim-buster

RUN apt-get update && \
    apt-get install gcc -y && \
    apt-get install python-dev -y && \
    apt-get install libpq-dev -y && \
    apt-get install postgresql-client -y &&\
    apt-get purge gcc -y

COPY devops/requirements.txt /
RUN pip install -r /requirements.txt

WORKDIR /app
