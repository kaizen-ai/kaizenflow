#!/usr/bin/env bash
#
# Dockerized solidity formatter and linter
#
# Prettier formatter
# sudo npm install --save-dev prettier prettier-plugin-solidity
# npx prettier --write \'contracts/**/*.sol\'
#
# EthLint
# sudo npm install -g solium
# solium --init
# solium -c --dir contracts . 

set -eux

cat >/tmp/tmp.dockerfile <<EOF
FROM ubuntu:latest

RUN apt-get update && \
    apt-get -y upgrade

RUN apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Node.js
RUN curl -sL https://deb.nodesource.com/setup_18.x | bash -
RUN apt-get update && apt-get install -y nodejs
RUN node -v
RUN npm -v

#Install Prettier and Ethlint(Solium)
RUN npm install -g prettier && \
    npm install -g prettier-plugin-solidity && \
    npm install -g solium

RUN npx prettier -v && \
    solium -V

EOF

docker build -f /tmp/tmp.dockerfile -t ubuntulint .

USER="$(id -u $(logname)):$(id -g $(logname))"
WORKDIR="$(realpath .)"
MOUNT="type=bind,source=${WORKDIR},target=${WORKDIR}"


cat >./run_linter.sh <<EOF

solium --init
for arg in "$@"
do
  npx prettier --write $arg
  solium -f $arg
done
rm .soliumrc.json .soliumignore

EOF
chmod +x ./run_linter.sh


CMD="bash -c ./run_linter.sh $@"

docker run --rm -it --workdir "${WORKDIR}" --mount "${MOUNT}" ubuntulint:latest $CMD

rm run_linter.sh
