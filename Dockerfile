# Base image
FROM jupyter/base-notebook:latest

# Maintainer and metadata
LABEL maintainer="Farhad Abasahl <farhad@umd.edu>"
LABEL version="1.0"
LABEL description="This is my project Docker image for running a Python script and Jupyter notebook along with Node.js."

# Set user to root to install additional software
USER root

#Create data directory
RUN mkdir -p /app/data

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install Node.js
RUN apt-get update && apt-get install -y curl \
    && curl -sL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Node dependencies
COPY package.json package-lock.json /tmp/
RUN cd /tmp && npm install && mv node_modules $CONDA_DIR

# Revert to non-root user
USER jovyan

# Copy your application source code
COPY --chown=jovyan:jovyan . /home/jovyan/work

# Expose the necessary port
EXPOSE 8888

# Set the default command to launch Jupyter Notebook
CMD ["start-notebook.sh", "--ip=0.0.0.0", "--port=8888", "--no-browser"]