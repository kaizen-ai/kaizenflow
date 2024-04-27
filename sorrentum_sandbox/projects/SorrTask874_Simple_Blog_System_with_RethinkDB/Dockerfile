# Use an official Python runtime as a parent image
FROM python:3.8.10
# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 5000 to the outside world
EXPOSE 5000

# Run app.py when the container launches
CMD ["python", "app.py"]
