# Use the official Python image as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the client script to the container
COPY client/client.py .

# Command to run the client script
CMD ["python", "client.py"]