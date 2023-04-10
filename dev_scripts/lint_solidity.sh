#!/usr/bin/env bash
#
# Dockerized solidity formatter and linter
#
# Prettier formatter
# sudo npm install --save-dev prettier prettier-plugin-solidity
#
# EthLint
# sudo npm install -g solium
# solium --init


set -eux

cat >/tmp/tmp.dockerfile <<EOF
FROM node:14

RUN npm install -g prettier && \
    npm install -g prettier-plugin-solidity && \
    npm install -g solium
EOF

docker build -f /tmp/tmp.dockerfile -t nodesoliditylinter .


USER="$(id -u $(logname)):$(id -g $(logname))"
WORKDIR="$(realpath .)"
MOUNT="type=bind,source=${WORKDIR},target=${WORKDIR}"


cat >./run_lint_solidity.sh <<EOF

solium --init

for arg in "$@"
do
  npx prettier --check "*.sol"
  npx prettier --write $arg
  solium -f $arg
done

rm .soliumrc.json .soliumignore

EOF

chmod +x ./run_lint_solidity.sh

CMD="bash -c './run_lint_solidity.sh'"

#OPTS="--user "${USER}"
OPTS=""
docker run --rm -it $OPTS --workdir "${WORKDIR}" --mount "${MOUNT}" nodesoliditylinter:latest $CMD "$@"

rm run_lint_solidity.sh
