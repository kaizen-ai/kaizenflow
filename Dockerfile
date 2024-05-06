# Base image for ARM architecture
FROM jupyter/base-notebook:latest

# Maintainer and metadata
LABEL maintainer="Farhad Abasahl <farhad@umd.edu>"
LABEL version="1.0"
LABEL description="Docker image for running Python script and Jupyter notebook along with Node.js."

# Install software as root
USER root

# Install Python, Node.js, curl, and git
RUN apt-get update && apt-get install -y python3 python3-pip curl git && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Node dependencies as root
COPY package.json package-lock.json /tmp/
RUN cd /tmp && npm install && mv node_modules /opt/conda

# Switch back to the regular user
USER jovyan

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Copy the application source code
COPY --chown=jovyan:jovyan . /home/jovyan/work

# Expose port for Jupyter
EXPOSE 8888

# Launch Jupyter Notebook
CMD ["start-notebook.sh", "--ip=0.0.0.0", "--port=8888", "--no-browser"]