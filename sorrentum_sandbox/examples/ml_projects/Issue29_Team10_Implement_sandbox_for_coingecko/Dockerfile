FROM python:latest
WORKDIR /code

# Set the environment variable for the database connection URL
ENV DATABASE_URL=postgresql://postgres:postgres@db/db

# Expose ports
EXPOSE 5432

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip3 install \
    ipython \
    pycoingecko \
    ipython-sql \
    jupyter \
    jupyter-contrib-core \
    jupyter-contrib-nbextensions \
    praw \
    psycopg2 \
    postgres \
    pymongo \
    s3fs \
    seaborn \
    tqdm \
    yapf 

COPY download_coingecko.py download_coingecko.py
COPY download_to_csv_cg.py download_to_csv_cg.py
COPY db_coingecko.py db_coingecko.py
COPY download_to_db.py download_to_db.py
ENTRYPOINT ["python","download_coingecko.py"]