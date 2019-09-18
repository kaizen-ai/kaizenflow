# Workflow

## Be patient
- This flow is designed to make our projects portable across:
    - platforms (e.g., macOS, Linux)
    - different installation of OSes (e.g., GP's laptop vs Paul's laptop) with
      all the peculiar ways we install and manage servers and laptops
    - different versions of conda
    - different versions of python 3.x
    - different versions of python packages

- There is no easy way to make sure that this script works for everybody
    - We can only make sure that Jenkins can follow the process described below
    - Try to follow the steps one by one, using a clean shell, cutting and
      pasting commands
    - If you hit a problem, be patient, ping GP, and we will extend the script to
      handle the quirks of your set-up

## Clone the packages
- Assume that $SRC_DIR contains the source code, e.g., `$HOME/src`
```
> cd $SRC_DIR
> git clone --recursive git@github.com:gpsaggese/lemonade.git 
```

## Assumptions
1) The env var `SRC_DIR` points to the dir with all your code you cloned
    - You want to put in your `.bashrc` an `export SRC_DIR=/Users/saggese/src`

2) You have a recent version of `conda`
```
> conda -V
conda 4.6.7
```

3) Your default python is 3.x
```
> python -V
Python 3.7.1
```
- TODO(GP): I don't remember how I did that. I might have create a conda env with
  python3 to bootstrap, like:
```
> export PATH=/Users/saggese/anaconda2/condabin:$PATH
> conda create -n py3 python=3.7
> conda activate py3
```
- Probably one can also install python3 in the root environment, or upgrade
  python to 3 system wide

## Create `develop` conda environment
- `develop` is the official package needed to run `amp` and `lemonade`
    - In general we prefer to use a single conda environment, unless it's not
      possible at all (e.g., using packages like Sage that don't support
      python3)

- Run this from `$SRC_DIR/lemonade/amp`
- NOTE: this directory needs to be in your PYTHONPATH (or else the imports will fail).
```
> create_conda.py --req_file dev_scripts/install/requirements/develop.txt --env_name develop --delete_env_if_exists 2>&1 | tee create_conda.log
```
- Note that `create_conda.py` has lots of options, including creating a new test
  environment

## Configure a shell for `utilities`
```
> cd $SRC_DIR/utilities && source dev_scripts/setenv.sh
...
# Testing packages
3.7.1 | packaged by conda-forge | (default, Feb 25 2019, 21:02:05)
[Clang 4.0.1 (tags/RELEASE_401/final)]
('numpy', '1.15.4')
('pandas', '0.24.1')
```

- You should see that the conda env `develop` has been selected
```
> conda info --envs
# conda environments:
#
develop               *  /Users/saggese/.conda/envs/develop
...
```

## Configure a shell
- `lemonade` depends on `utilities`
```
> cd $SRC_DIR/lemonade && source dev_scripts/setenv.sh
```

## Start jupyter
```
> jupyter notebook --ip=* --browser="chrome" . --port 9999
> run_jupyter.sh
```

## Misc

* Other interesting packages
```bash
> create_conda.py --req_file dev_scripts/install/requirements/pymc.txt --env_name pymc3 --delete_env_if_exists -v DEBUG
> conda create sage -n sage
` ``

# Running unit tests

- To enable debug info
```bash
> pytest --dbg_verbosity DEBUG
```

- To update golden outcomes
```bash
> pytest --update_outcomes
```

- To stop at first failure
```bash
> pytest -x
```

- To run a single class
```bash
> pytest -k TestPcaFactorComputer1
```

- To run a single test method
```bash
> pytest core/test/test_core.py::TestPcaFactorComputer1::test_linearize_eigval_eigvec
```
