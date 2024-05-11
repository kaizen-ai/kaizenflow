# Distributed Messaging with ZeroMQ
# Dockerfile for building a containerized application

# Use an official Python runtime as a base image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the application files to the container
COPY . .

# Copy additional files
COPY pub.py .
COPY sub_patient1.py .
COPY sub_patient2.py .
COPY sub_patient3.py .
COPY sub_all_patient.py .
COPY psychiatrist_subscriber.py .

# Install any dependencies required for the application
RUN pip install -r requirements.txt

# Specify the command to run when the container starts
CMD ["python", "pub.py"]   # Adjusted to run pub2.py, change accordingly for other files

# Expose the port the app runs on
EXPOSE 8888




