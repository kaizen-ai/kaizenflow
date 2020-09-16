FROM python:3.7-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    git && \
    pip install pre-commit \
    && mkdir /pre-commit && \
    cd /pre-commit && \
    git init . && \
    pre-commit install

# TODO(amr): add `.pre-commit-config.yaml` to this image
COPY ./dev_scripts /dev_scripts
COPY ./helpers /helpers

ENTRYPOINT [ "pre-commit" ]
