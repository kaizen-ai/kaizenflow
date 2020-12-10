docker run --rm -t \
    --env "PRE_COMMIT_HOME=$HOME/.cache/pre-commit" \
    --env "SKIP=$SKIP" \
    -v "$HOME/.cache:$HOME/.cache:rw" \
    -v "$(pwd)":/src \
    --workdir /src \
    083233266530.dkr.ecr.us-east-2.amazonaws.com/dev_tools:prod "$@"
