#
# Create a PROD image with the current code inside of the DEV image.
#
ARG VERSION
FROM 665840871993.dkr.ecr.us-east-1.amazonaws.com/cmamp:dev-${VERSION}

RUN ls .
COPY . /app
