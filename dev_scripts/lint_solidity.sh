#!/usr/bin/env bash
#
# Dockerized solidity formatter and linter
#
# Prettier formatter
# sudo npm install --save-dev prettier prettier-plugin-solidity
# 
# EthLint
# sudo npm install -g solium


set -eux

cat >/tmp/tmp.dockerfile <<EOF
FROM ubuntu:20.04

RUN apt-get update && \
    apt-get -y upgrade

# Build from npm package manager 
RUN export DEBIAN_FRONTEND=noninteractive; \
    apt-get install -y curl && \
    curl -sL https://deb.nodesource.com/setup_18.x && \
    apt-get install -y nodejs && \
    apt-get install -y npm

RUN node -v
RUN npm -v

RUN npm install --save-dev prettier prettier-plugin-solidity && \
    npm install -g solium
EOF

docker build -f /tmp/tmp.dockerfile -t soliditylinter .


USER="$(id -u $(logname)):$(id -g $(logname))"
WORKDIR="$(realpath .)"
MOUNT="type=bind,source=${WORKDIR},target=${WORKDIR}"


cat >./run_lint_solidity.sh <<EOF

solium --init

for arg in "$@"
do
  npx prettier --write $arg
  solium -f $arg
done

rm .soliumrc.json .soliumignore

EOF

chmod +x ./run_lint_solidity.sh

CMD="bash -c './run_lint_solidity.sh'"

#OPTS="--user "${USER}"
OPTS=""
docker run --rm -it $OPTS --workdir "${WORKDIR}" --mount "${MOUNT}" soliditylinter:latest $CMD "$@"

rm run_lint_solidity.sh
