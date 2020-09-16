#!/bin/bash -e

cd $(git rev-parse --show-toplevel)

# TODO(gp): replace `p1-precommit` with remote docker image tag
cat > '.git/hooks/pre-commit' << 'EOD'
#!/bin/bash -e

# Run pre-commit inside docker
cd $(git rev-parse --show-toplevel)

docker run --rm -t \
    --env "PRE_COMMIT_HOME=$HOME/.cache/pre-commit" \
    --env "PATH=/dev_scripts/linter2:${PATH}" \
    --env "PYTHONPATH=/:${PYTHONPATH}" \
    --env "SKIP=$SKIP" \
    -v "$HOME/.cache:$HOME/.cache:rw" \
    -v "$(pwd):/$(pwd)" \
    --workdir "$PWD" \
    p1-precommit "$@"

EOD

# TODO(amr): Provide a command `p1-precommit` that calls `pre-commit` through the docker image
