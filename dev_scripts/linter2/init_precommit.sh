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

# TODO: add this to PATH?
# TODO: should that script be placed in another place other than local directory?
# I like the idea of moving it to `/usr/local/bin`, but that would require sudo privilages
ln -s .git/hooks/pre-commit p1-precommit
chmod +x p1-precommit
