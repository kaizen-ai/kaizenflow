FROM ubuntu:20.04 AS builder

# Where to copy the installation files.
ENV INSTALL_DIR=/install
WORKDIR $INSTALL_DIR

# - Install OS packages.
COPY devops/docker_build/install_os_packages.sh .
RUN /bin/bash -c "./install_os_packages.sh"

# - Install Python packages.
# Copy the minimum amount of files needed to call install_requirements.sh so we
# can cache it effectively.
COPY devops/docker_build/poetry.lock .
COPY devops/docker_build/poetry.toml .
COPY devops/docker_build/pyproject.toml .
COPY devops/docker_build/install_python_packages.sh .
RUN /bin/bash -c "./install_python_packages.sh"

# - Install Jupyter extensions.
COPY devops/docker_build/install_jupyter_extensions.sh .
RUN /bin/sh -c "./install_jupyter_extensions.sh"

# TODO(gp): Is this needed?
WORKDIR $APP_DIR

ENTRYPOINT ["devops/docker_run/entrypoint.sh"]