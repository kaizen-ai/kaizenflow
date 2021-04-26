PWD=$(pwd)

AMP=$PWD

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
echo "# PATH=$PATH"

export PYTHONPATH=$PWD:$PYTHONPATH
export PYTHONPATH=$(echo $PYTHONPATH | perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')

echo "# PYTHONPATH=$PYTHONPATH"

echo "# which gh="$(which gh)
alias ghamp="gh --repo alphamatic/amp"
alias ghdt="gh --repo alphamatic/dev_tools"
alias ghlem="gh --repo alphamatic/lemonade"

alias i="invoke"
alias il="invoke --list"

# Activate the virtuel env.
source venv/bin/activate
