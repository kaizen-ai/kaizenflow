#
# Create a PROD image with the current code inside of the DEV image.
#
ARG VERSION
# TODO(gp): We should have a way to specify amp or cmamp here. We could have
# the code use the value from repo_config.py to control the name of the image.
FROM 665840871993.dkr.ecr.us-east-1.amazonaws.com/cmamp:dev-${VERSION}

# Specify that this is a production cmamp container, used inside
# `/app/repo_config.py` to determine configuration.
ENV CK_IN_PROD_CMAMP_CONTAINER=1

RUN ls .
COPY . /app
