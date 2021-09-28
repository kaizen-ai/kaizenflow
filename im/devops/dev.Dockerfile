# TODO(Grisha): use environment variable for image #106.
FROM 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:dev

# TODO(Grisha): rebuild amp image so that it has `postgresql-client` #106.
RUN apt-get update && \
    apt-get install postgresql-client -y

WORKDIR /app