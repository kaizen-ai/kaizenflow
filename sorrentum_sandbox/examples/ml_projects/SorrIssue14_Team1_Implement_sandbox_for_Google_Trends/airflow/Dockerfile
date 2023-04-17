FROM apache/airflow:2.2.2

USER root

RUN apt-get update -yqq && \
    apt-get install -y \
      jupyter-notebook \
      libpq-dev \
      postgresql \
      postgresql-contrib \
      python3-dev \
      python3-pip \
      vim

# Add (optional) Python libraries.
#https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html?highlight=snowflake

USER airflow

# Update pip.
RUN /usr/local/bin/python -m pip install --upgrade pip

RUN pip3 install google_search_results pandas psycopg2_binary python-dotenv


# Install Python packages.
#RUN pip3 install \
#    ipython \
#    ipython-sql \
#    jupyter \
#    jupyter-contrib-core \
#    jupyter-contrib-nbextensions \
#    praw \
#    psycopg2 \
#    pymongo \
#    s3fs \
#    seaborn \
#    tqdm \
#    yapf
#
## Part of the original DockerFile.
RUN pip3 install \
    apache-airflow-providers-http==2.0.1 \
    apache-airflow-providers-postgres==2.2.0
