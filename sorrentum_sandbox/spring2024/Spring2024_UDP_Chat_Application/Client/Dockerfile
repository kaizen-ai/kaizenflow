# Use the official Python image as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the client script to the container
COPY client.py .

# Expose the port the client will use (not necessary in this case)
# EXPOSE 0

# Command to run the client script
CMD ["python", "client.py"]