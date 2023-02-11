FROM ubuntu:22.04
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -y update
RUN apt-get -y upgrade

# Install system utilities.
RUN apt install -y --no-install-recommends \
    sudo \
    curl \
    systemctl \
    gnupg \
    git \
    vim

# Install Python.
RUN apt install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev

# Install Python packages.
RUN pip3 install \
    dask[complete] \
    fastparquet \
    ipython \
    jupyter \
    jupyter-contrib-core \
    jupyter-contrib-nbextensions
    matplotlib \
    pandas \
    seaborn \
    yapf

RUN mkdir /install

# Config.
ADD etc_sudoers /install/
COPY etc_sudoers /etc/sudoers
COPY bashrc /var/lib/postgresql/.bashrc

# Install Jupyter extensions.
ADD install_jupyter_extensions.sh /install/
RUN /install/install_jupyter_extensions.sh

# Report package versions.
ADD version.sh /install/
RUN /install/version.sh 2>&1 | tee version.log

# Jupyter.
EXPOSE 8888
