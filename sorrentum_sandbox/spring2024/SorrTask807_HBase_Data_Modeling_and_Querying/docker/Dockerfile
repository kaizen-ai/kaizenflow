# Use the official Jupyter base image
FROM quay.io/jupyter/base-notebook

# Switch to root to install additional system dependencies
USER root

# Install necessary happybase using Conda
RUN conda install --quiet --yes 'happybase' && \
    conda clean -afy

# Copy the notebook files into the container
COPY . /home/jovyan/work

# Expose the port the notebook runs on
EXPOSE 8888
