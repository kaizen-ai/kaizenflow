FROM python:latest
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY app.py app.py
COPY helpers helpers
COPY common common
COPY download_yahoo.py download_yahoo.py
COPY download_to_csv_yahoo.py download_to_csv_yahoo.py
COPY db_yahoo.py db_yahoo.py
COPY download_db_yahoo.py download_db_yahoo.py
ENTRYPOINT ["python","download_db_yahoo.py"]
