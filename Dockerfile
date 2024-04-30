# Distributed Messaging with ZeroMQ
# Dockerfile for building a containerized application


# Use an official Python runtime as a base image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the application files to the container
COPY . .

# Install any dependencies required for the application
RUN pip install -r requirements.txt

# Specify the command to run when the container starts
CMD ["python", "app.py"]

# Expose the port the app runs on
EXPOSE 8888

