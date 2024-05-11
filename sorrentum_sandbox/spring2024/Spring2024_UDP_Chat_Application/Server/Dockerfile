# Use the official Python image as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the server script to the container
COPY server/server.py .

# Expose the port the server will run on
EXPOSE 9999

# Command to run the server script
CMD ["python", "server.py"]