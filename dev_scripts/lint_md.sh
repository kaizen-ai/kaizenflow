#!/usr/bin/env bash
# """
# Lint a markdown file and update the table of content.
# To generate a table of content, add a comment `<!-- toc -->` at the top of
# the markdown file.
#
# Usage:
# > dev_scripts/lint_md.sh file1.md ... fileN.md
# """

set -eux

# Build the Docker container.
TMP_FILENAME="/tmp/tmp.lint_markdown.Dockerfile"
cat>$TMP_FILENAME <<EOF
FROM ubuntu:latest

RUN apt-get update && \
    apt-get -y upgrade

RUN apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Node.js.
RUN curl -sL https://deb.nodesource.com/setup_18.x | bash -
RUN apt-get update && apt-get install -y nodejs
RUN node -v
RUN npm -v

# Install executables.
RUN npm install -g prettier
RUN npm install -g markdown-toc
RUN npm install -g markdown-link-check

RUN npx prettier -v
EOF

export DOCKER_CONTAINER_NAME="sorrentum_mdlint"
docker build -f $TMP_FILENAME -t $DOCKER_CONTAINER_NAME .

# Create the script to run the linter.
LINTER="./tmp.lint_md.sh"
cat >$LINTER <<EOF
npx prettier --parser markdown --prose-wrap always --tab-width 2 --write $@
npx markdown-toc -i $@
EOF
chmod +x $LINTER

# Execute the custom script using Docker.
CMD="bash -c $LINTER $@"
USER="$(id -u $(logname)):$(id -g $(logname))"
WORKDIR="$(realpath .)"
MOUNT="type=bind,source=${WORKDIR},target=${WORKDIR}"
docker run --rm -it --workdir "${WORKDIR}" --mount "${MOUNT}" ${DOCKER_CONTAINER_NAME} ${CMD}

# TODO: Find a way to remove empty lines in bullet list.

# Clean up temporary files.
rm $LINTER
rm $TMP_FILENAME
