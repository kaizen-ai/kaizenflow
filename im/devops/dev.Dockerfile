FROM python:3.7-slim-buster

# Install the postgres client.
RUN apt-get update && \
    apt-get install postgresql-client -y

# Name of the virtual environment to create.
ENV ENV_NAME="venv"

# Specify the working dir.
ENV APP_DIR=/app
WORKDIR $APP_DIR

# Install poetry.
RUN pip3 install poetry

# Copy the poetry files.
COPY ./poetry.lock .
COPY ./poetry.toml .
COPY ./pyproject.toml .
COPY ./install_requirements.sh .

# Install the Python packages.
RUN /bin/bash -c "./install_requirements.sh"