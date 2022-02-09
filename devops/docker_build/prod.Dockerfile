#
# Create a PROD image with the current code inside of the DEV image.
#
ARG VERSION
# TODO(gp): We should have a way to specify amp or cmamp here. We could have
# the code use the value from repo_config.py to control the name of the image.
FROM 665840871993.dkr.ecr.us-east-1.amazonaws.com/cmamp:dev-${VERSION}

# Before building prod image, create folder aws in `src/cmamp<X>/`
# The folder should have the typical aws credentials folder structure (files
# named config and credentials).
# DO NOT use your personal credentials here, since prod image has its own
# credentials.
COPY ./aws /home/.aws/

RUN ls .
COPY . /app
