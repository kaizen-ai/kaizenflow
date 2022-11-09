# Adapted from https://github.com/resdevd/docker-compose-airflow/blob/main/Dockerfile.
FROM apache/airflow:2.2.2

USER root

RUN apt-get update -yqq \
    && apt-get install -y vim \
    python3-pip python3-dev \
    postgresql postgresql-contrib \
    libpq-dev \
    jupyter-notebook

# Add any python libraries  (Optional)
#https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html?highlight=snowflake

USER airflow

# Install Python packages.
RUN pip3 install \
    jupyter \
    yapf \
    ipython \
    ipython-sql \
    psycopg2 \
    jupyter-contrib-core \
    jupyter-contrib-nbextensions

# Part of the original DockerFile
RUN pip install \
    'python-dotenv' \
    'apache-airflow-providers-http==2.0.1' \
    'apache-airflow-providers-postgres==2.2.0'

USER airflow