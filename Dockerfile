FROM python:3.9-slim
WORKDIR /app
COPY . /app

RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 5556
EXPOSE 5557
EXPOSE 5558

ENV NAME World

CMD ['python3', "zeroMQ_receiver.py"]
