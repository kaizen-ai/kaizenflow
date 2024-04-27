# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y wget && \
    apt-get install -y tar && \
    wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.2.tgz && \
    tar xzf spark-3.5.1-bin-hadoop3.2.tgz && \
    mv spark-3.5.1-bin-hadoop3.2 /spark && \
    rm spark-3.5.1-bin-hadoop3.2.tgz && \
    apt-get remove -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Spark
ENV SPARK_HOME=/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Install Python dependencies
RUN pip install pyspark

# Copy the Python script into the container
COPY spring2024_streaming_word_count_with_apache_spark_streaming.py /app

# Command to run your Python script
CMD ["python", "spring2024_streaming_word_count_with_apache_spark_streaming.py"]








