PWD=$(pwd)

AMP=$PWD

echo "# Activate virtual env"
VENV_DIR="$HOME/venv/client_setup"
cmd="source $VENV_DIR/bin/activate"
echo "> $cmd"
eval $cmd

echo "which python="$(which python)
echo "python -v="$(python --version)

echo "# Set path"
export PATH=.:$PATH
export PATH=$AMP:$PATH
export PATH=$AMP/dev_scripts:$PATH
export PATH=$AMP/dev_scripts/aws:$PATH
export PATH=$AMP/dev_scripts/git:$PATH
export PATH=$AMP/dev_scripts/infra:$PATH
export PATH=$AMP/dev_scripts/install:$PATH
export PATH=$AMP/dev_scripts/notebooks:$PATH
export PATH=$AMP/dev_scripts/testing:$PATH

export PATH=$(echo $PATH | perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')
echo "PATH=$PATH"

echo $PATH | perl -e 'print join("\n", grep { not $seen{$_}++ } split(/:/, scalar <>))'

echo "# Set PYTHONPATH"
export PYTHONPATH=$PWD:$PYTHONPATH
export PYTHONPATH=$(echo $PYTHONPATH | perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')

echo "PYTHONPATH=$PYTHONPATH"

echo $PYTHONPATH | perl -e 'print join("\n", grep { not $seen{$_}++ } split(/:/, scalar <>))'

echo "# Configure env"
echo "which gh="$(which gh)
alias ghamp="gh --repo alphamatic/amp"
alias ghdt="gh --repo alphamatic/dev_tools"
alias ghlem="gh --repo alphamatic/lemonade"

alias i="invoke"
alias il="invoke --list"

echo "==> SUCCESS <=="
