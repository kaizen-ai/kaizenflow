FROM python:3.7-slim-buster

RUN apt-get update && \
    apt-get install postgresql-client -y

COPY requirements.txt .

RUN pip install -r requirements.txt

WORKDIR /app