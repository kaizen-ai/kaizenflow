# Base image.
FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy local src dir to working dir.
COPY . .

#CMD ["python", "./server.py"]
