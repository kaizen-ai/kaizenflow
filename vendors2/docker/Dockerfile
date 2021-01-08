FROM postgres:13.1-alpine

# TODO(vr): Remove this!
ENV POSTGRES_PASSWORD password

ADD db.sql /docker-entrypoint-initdb.d/
