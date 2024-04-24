FROM python:3.9

RUN apt-get update -yqq && \
	apt-get install -y \
	postgresql-client

# Set the working directory in the container.
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port Jupyter Notebook runs on.
EXPOSE 8888