#
# Create a PROD image with the current code inside of the DEV image.
#
ARG VERSION
# TODO(gp): We should have a way to specify amp or cmamp here. We could have
# the code use the value from repo_config.py to control the name of the image.
FROM 665840871993.dkr.ecr.us-east-1.amazonaws.com/cmamp:dev-${VERSION}

RUN ls .
COPY . /app
