PWD=$(pwd)

AMP=$PWD

export PATH=$AMP/dev_scripts:$PATH
export PATH=$AMP/dev_scripts/aws:$PATH
export PATH=$AMP/dev_scripts/git:$PATH
export PATH=$AMP/dev_scripts/infra:$PATH
export PATH=$AMP/dev_scripts/install:$PATH
export PATH=$AMP/dev_scripts/notebooks:$PATH
export PATH=$AMP/dev_scripts/testing:$PATH

export PATH=$(echo $PATH | perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')
echo "PATH=$PATH"

export PYTHONPATH=$PWD:$PYTHONPATH
export PYTHONPATH=$(echo $PYTHONPATH | perl -e 'print join(":", grep { not $seen{$_}++ } split(/:/, scalar <>))')

echo "PYTHONPATH=$PYTHONPATH"
