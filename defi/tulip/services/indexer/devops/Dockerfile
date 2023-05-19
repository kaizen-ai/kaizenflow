FROM python:3.7-slim
RUN mkdir /app
ADD ./events_indexer.py /app
ADD ./TulipABI.json /app
WORKDIR /app
RUN pip install web3
EXPOSE 80
CMD ["python", "events_indexer.py"]