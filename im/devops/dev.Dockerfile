FROM python:3.7-slim-buster

RUN apt-get update && \
    apt-get install postgresql-client -y

# Name of the virtual environment to create.
ENV ENV_NAME="venv"

ENV APP_DIR=/app
WORKDIR $APP_DIR

# Install requirements.
# Copy the minimum amount of files needed to call install_requirements.sh so we
# can cache it effectively.
RUN pip3 install poetry
COPY ./poetry.lock .
COPY ./poetry.toml .
COPY ./pyproject.toml .
COPY ./install_requirements.sh .
RUN /bin/bash -c "./install_requirements.sh"