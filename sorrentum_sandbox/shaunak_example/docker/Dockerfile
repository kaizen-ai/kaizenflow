# Use an official Python runtime as a parent image
FROM python:3

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install Redis and required Python packages
RUN apt-get update && apt-get install -y redis-server && pip install redis

# Expose Redis default port
EXPOSE 6379

# Copy the mock database JSON file into the container
COPY user_profiles1.json /app

# Run Redis server
CMD ["redis-server"]

# Run the Python script
CMD ["python", "Redis cache to fetch user profiles.ipynb"]

