# Base image.
FROM python:3.8-slim

WORKDIR /app

COPY devops/requirements.txt .
RUN pip install -r requirements.txt

COPY ./src .

#CMD ["python", "./server.py"]
