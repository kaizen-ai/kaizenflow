# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install the needed packages specified in requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Make port 4000 available to the world outside this container
EXPOSE 4000

# Define environment variable
ENV NAME World

# Run main.py when the container launches
CMD ["python", "main.py"]

#Complete
