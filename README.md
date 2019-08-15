# Workflow

## Be patient
- This flow is designed to make our projects portable across
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
- The code is organized in two repos:
    - `utilities`: contains everything that is project agnostic (e.g., utilities,
      frameworks, ...)
    - `lemonade`: contains the models shared by GP & Paul
- Clone the packages in the same dir `$SRC_DIR`
```
> cd $SRC_DIR
> git clone git@github.com:gpsaggese/utilities.git 
> git clone git@github.com:gpsaggese/lemonade.git 
```

## Assumptions
1) All the repos are cloned in the same dir like:
```
> ls -1
lemonade
git_gp1
git_gp2
utilities
```

2) The env var `SRC_DIR` points to the dir with all your code you cloned
    - You want to put in your `.bashrc` an `export SRC_DIR=/Users/saggese/src`

3) You have a recent version of `conda`
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
- `develop` is the official package needed to run `utilities` and `lemonade`
    - In general we prefer to use a single conda environment, unless it's not
      possible at all (e.g., using packages like Sage that don't support
      python3)

- Run this from `$SRC_DIR/utilities`
- NOTE: this directory needs to be in your PYTHONPATH (or else the imports will fail).
```
> install/create_conda.py --req_file install/requirements/requirements_develop.txt --env_name develop --delete_env_if_exists 2>&1 | tee create_conda.log
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

## Configure a shell for `lemonade`
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
```
> install/create_conda.py --req_file requirements_pymc.txt --env_name pymc3 --delete_env_if_exists -v DEBUG
> conda create sage -n sage
` ``

# Code org
- `helpers`: low-level helpers that are general and not specific of any project
- `core`: helpers that are specific of data science, finance projects
