#!/usr/bin/env bash
# """
# Lint a markdown file and update the table of content.
# To generate a table of content, add a comment `<!-- toc -->` at the top of
# the markdown file.
#
# Usage:
# > dev_scripts/lint_md.sh file1.md ... fileN.md
# """

#set -eux
#
## Build the Docker container.
#TMP_FILENAME="/tmp/tmp.lint_markdown.Dockerfile"
#cat>$TMP_FILENAME <<EOF
#FROM ubuntu:latest
#
#RUN apt-get update && \
#    apt-get -y upgrade && \
#    apt-get install -y curl && \
#    apt-get install -y ca-certificates && \
#    apt-get install -y gnupg && \
#    apt-get clean && \
#    rm -rf /var/lib/apt/lists/*
#
## Install Node.js.
## `https://github.com/nodesource/distributions#ubuntu-versions`
#RUN mkdir -p /etc/apt/keyrings
#RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
#RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
#RUN apt-get update && apt-get install -y nodejs
#RUN node -v
#RUN npm -v
#
## Install Prettier.
#RUN npm install -g prettier && \
#    npm install -g markdown-toc
#
#RUN npx prettier -v
#EOF
#
#export DOCKER_CONTAINER_NAME="sorrentum_mdlint"
#docker build -f $TMP_FILENAME -t $DOCKER_CONTAINER_NAME .
#
## Create the script to run the linter.
#LINTER="./tmp.lint_md.sh"
#cat >$LINTER <<EOF
#npx prettier --parser markdown --prose-wrap always --tab-width 2 --write $@
#npx markdown-toc -i $@
#EOF
#chmod +x $LINTER
#
## Execute the custom script using Docker.
#CMD="bash -c $LINTER $@"
#USER="$(id -u $(logname)):$(id -g $(logname))"
#WORKDIR="$(realpath .)"
#MOUNT="type=bind,source=${WORKDIR},target=${WORKDIR}"
#docker run --rm -it --workdir "${WORKDIR}" --mount "${MOUNT}" ${DOCKER_CONTAINER_NAME} ${CMD}
#
## Add files after linting.
#git add $@
## TODO: Find a way to remove empty lines in bullet list.
#
## Clean up temporary files.
#rm $LINTER
#rm $TMP_FILENAME
