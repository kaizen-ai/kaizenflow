# syntax = docker/dockerfile:experimental

FROM continuumio/miniconda3:4.9.2

# TODO(gp): Trim this down. npm needed?
RUN apt update && \
    apt install cifs-utils -y && \
    apt install git -y && \
    apt install graphviz -y && \
    apt install keyutils -y && \
    apt install make -y && \
    apt install npm -y && \
    apt install s3fs -y && \
    apt install vim -y && \
    apt-get purge -y --auto-remove

# TODO(*): Remove prettier since it goes in dev_tools.
RUN npm install --global prettier

# Mount external filesystems.
RUN mkdir -p /s3/default00-bucket
RUN mkdir -p /fsx/research
COPY devops/docker_build/fstab /etc/fstab

# Configure conda.
RUN conda init bash
RUN conda config --set default_threads 4
RUN conda config --add channels conda-forge
RUN conda config --show-sources

ENV APP_DIR=/app

# Create conda environment.
ENV ENV_NAME="venv"
RUN conda create -n $ENV_NAME python=3.7 -y

# We assume that the needed files to build the image are under
# devops/{docker_build,docker_scripts}
# We want to minimize the dependencies to avoid to invalidate Docker cache for
# a change in files that doesn't matter for building the image.
ENV DIR="devops/docker_build"
RUN mkdir -p $APP_DIR/$DIR
COPY $DIR $APP_DIR/$DIR

ENV DIR="devops/docker_scripts"
RUN mkdir -p $APP_DIR/$DIR
COPY $DIR $APP_DIR/$DIR

WORKDIR $APP_DIR

# Install requirements.
RUN devops/docker_build/install_requirements.sh
# TODO(gp): This is not portable across BUILDKIT=1 and BUILDKIT=0 and it's not
# cached.
#RUN --mount=source=.,target=/amp ./devops/docker_build/install_requirements.sh

# Run repo-specific initialization scripts.
RUN devops/docker_build/init.sh
# TODO(gp): This is not portable across BUILDKIT=1 and BUILDKIT=0 and it's not
# cached.
#RUN --mount=source=.,target=/app ./devops/docker_build/init.sh

RUN echo "conda activate venv" >> ~/.bashrc

ENTRYPOINT ["./devops/docker_build/entrypoint.sh"]
