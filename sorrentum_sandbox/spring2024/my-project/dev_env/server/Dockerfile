FROM ubuntu:18.04
FROM python:3.9

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev


COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install MarkupSafe
RUN pip3 install pika
RUN pip3 install -r requirements.txt

COPY . /app

ENTRYPOINT [ 'python3' ]

CMD [ 'app.py' ]
