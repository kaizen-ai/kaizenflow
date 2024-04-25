# Use a base Hadoop Streaming image
FROM hadoop-streaming-image:latest

# Optionally, configure any additional Hadoop settings or environment variables
# ENV HADOOP_HOME=/path/to/hadoop

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy any necessary files or scripts into the container
COPY mapper.py reducer.py ./
