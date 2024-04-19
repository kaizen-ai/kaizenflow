FROM python:3.9-slim 

WORKDIR /app 


COPY . /app


RUN apt-get update 

RUN apt-get install -y --no-install-recommends build-essential libpq-dev 


COPY requirements.txt . 

RUN pip install --no-cache-dir -r requirements.txt 
RUN mkdir -p /app/data

EXPOSE 80

COPY . . 

CMD ["python", "scripts/fetch_crypto_data.py"]

