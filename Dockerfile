# Base image using Python
FROM python:3.9

# Maintainer and metadata
LABEL maintainer="Farhad Abasahl <farhad@umd.edu>"
LABEL version="1.0"
LABEL description="This is my project Docker image for running a Python script and Jupyter notebook along with Node.js."

# Set working directory to /app
WORKDIR /app

# Copy only the dependency management files first
COPY requirements.txt package.json package-lock.json ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt --verbose

# Install Node.js
RUN apt-get update && apt-get install -y curl
RUN curl -sL https://deb.nodesource.com/setup_18.x | bash -
RUN apt-get install -y nodejs

# Install Node dependencies
RUN npm install

# Copy the rest of the application source code
COPY . /app

# Expose the necessary ports
EXPOSE 8888 80

# Create a non-privileged user
RUN useradd -ms /bin/sh -u 1001 app
USER app

# Define environment variable
ENV NAME World

# Set the default command to launch Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]











