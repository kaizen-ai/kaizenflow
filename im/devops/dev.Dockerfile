FROM 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:prod

RUN apt-get update && \
    apt-get install postgresql-client -y

WORKDIR /app