# The FROM command tells the file to use the python image

FROM python

# This part of the code indicates the path in which the files code605.ipynb market_price.csv will be stored in the image's file system

WORKDIR /usr/src/app

# This part of the code adds the files code605.ipynb market_price.csv to the working directory

ADD code605.ipynb .

# This part of the code installs necessary packages

RUN pip install requests beautifulsoup4 sqlalchemy pandas

RUN apt-get update -yqq && \
    apt-get install -y \
      jupyter-notebook \
      libpq-dev \
      postgresql \
      postgresql-contrib \
      python3-dev \
      python3-pip \
      vim

RUN pip3 install \
    ipython \
    ipython-sql \
    jupyter \
    jupyter-contrib-core \
    jupyter-contrib-nbextensions \
    praw \
    psycopg2 \
    pymongo \
    s3fs \
    seaborn \
    tqdm \
    yapf


# This part describes how the container should be run

CMD [ "python", "./code605.ipynb"]
