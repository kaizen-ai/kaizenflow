#!/usr/bin/env bash
# Markdown linter using prettier.
#
# Usage: 
# npx prettier --parser markdown \
#   --prose-wrap always \
#   --write \
#   --tab-width 2 \
#   file1.md file2.md ....
# 
# To generate TOC:
# Add <!-- toc -->; at the top of the markdown file.
# npx markdown-toc -i file1.md file2.md ....

set -eux

# Create a temporary file to build the Docker container.
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

# Install Prettier.
RUN npm install -g prettier && \
    npm install -g markdown-toc

RUN npx prettier -v

EOF

docker build -f $TMP_FILENAME -t markdownlint .

USER="$(id -u $(logname)):$(id -g $(logname))"
WORKDIR="$(realpath .)"
MOUNT="type=bind,source=${WORKDIR},target=${WORKDIR}"

# Append code to initialize the linter.
cat >./run_linter.sh <<EOF

npx prettier --parser markdown --prose-wrap always --tab-width 2 --write $@
npx markdown-toc -i $@

EOF

# Execute the custom script using Docker.
chmod +x ./run_linter.sh


CMD="bash -c ./run_linter.sh $@"

docker run --rm -it --workdir "${WORKDIR}" --mount "${MOUNT}" markdownlint:latest $CMD

# Clean up temporary files.
rm run_linter.sh
rm $TMP_FILENAME
