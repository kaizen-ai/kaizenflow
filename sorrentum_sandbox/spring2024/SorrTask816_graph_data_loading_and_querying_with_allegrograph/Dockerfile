# Base image for AllegroGraph
FROM franzinc/agraph

# Set environment variables for AllegroGraph
ENV AGRAPH_SUPER_USER=admin
ENV AGRAPH_SUPER_PASSWORD=xyz123

# Expose necessary ports
EXPOSE 10000-10035

# Switch to root user
USER root

# Create the missing directory for apt-get
RUN mkdir -p /var/lib/apt/lists/partial

# Update package lists
RUN apt-get update

# Install Python, pip, and Jupyter Notebook
RUN apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip && \
    pip3 install jupyter && \
    jupyter notebook --generate-config && \
    echo "c.NotebookApp.password = ''" >> ~/.jupyter/jupyter_notebook_config.py

# Expose Jupyter Notebook port
EXPOSE 8888

# Start Jupyter Notebook on container start
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
