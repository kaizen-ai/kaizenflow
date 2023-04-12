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
# Usage: ./lint_solidity.sh file1.sol file2.sol ....

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

: <<'END_COMMENT'
Since solium does not support multiple file inputs but a directory 
so copied all the files supplied to command line arguments  
to temporary created contracts directory which will work as input to ethlint
END_COMMENT

mkdir temp
# Loop through each file name passed as an argument
for file in "$@"; do
    # Check if the file exists
    if [ -e "$file" ]; then
        # Copy the file to the new directory
        cp "$file" "temp/"
    else
        # If the file doesn't exist, print an error message
        echo "Error: $file does not exist"
    fi
done


cat >./run_linter.sh <<EOF

solium --init
for arg in "$@"
do
  npx prettier --write $@
  solium -d temp/
done

rm .soliumrc.json .soliumignore

EOF

chmod +x ./run_linter.sh


CMD="bash -c ./run_linter.sh $@"

docker run --rm -it --workdir "${WORKDIR}" --mount "${MOUNT}" ubuntulint:latest $CMD

rm run_linter.sh
rm -r temp
