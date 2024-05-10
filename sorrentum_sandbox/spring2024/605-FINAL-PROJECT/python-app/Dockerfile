# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the application's requirements and install them
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of your application
COPY . /app

# Install Jupyter if not already included in your requirements.txt
RUN pip install notebook psycopg2-binary

# Expose ports for web application and Jupyter
EXPOSE 80 8888

# Copy entrypoint script and make it executable
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set the entrypoint script
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default command
CMD ["python", "app.py"]

