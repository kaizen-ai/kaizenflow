FROM python:latest
WORKDIR /code

# Set the environment variable for the database connection URL
ENV DATABASE_URL=postgresql://postgres:postgres@db/db

# Expose ports
EXPOSE 5432
EXPOSE 8000

ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
COPY download_coingecko.py download_coingecko.py
COPY download_to_csv_cg.py download_to_csv_cg.py
COPY db_coingecko.py db_coingecko.py
COPY dowonload_to_db.py dowonload_to_db.py
CMD ["python", "-u", "download_coingecko.py"]