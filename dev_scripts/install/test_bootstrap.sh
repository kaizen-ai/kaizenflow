#!/bin/bash -xe

# """
# Test all the executables that need to bootstrap.
# """

(cd $HOME/src/commodity_research1/amp; dev_scripts/_setenv_amp.py)
(cd $HOME/src/commodity_research1/amp; dev_scripts/install/create_conda.py -h)

(cd $HOME/src/commodity_research1; dev_scripts_p1/_setenv_p1.py)

(cd $HOME/src/commodity_research1/amp; source dev_scripts/setenv_amp.sh)
(cd $HOME/src/commodity_research1; source dev_scripts_p1/setenv_p1.sh)
