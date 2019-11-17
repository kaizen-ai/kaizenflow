<!--ts-->
   * [Design principles for multiple Git repos](#design-principles-for-multiple-git-repos)
      * [Steps requiring composition of Git repos](#steps-requiring-composition-of-git-repos)
   * [Configuring a Git client](#configuring-a-git-client)
      * [Bootstrapping](#bootstrapping)
      * [When bootstrapping happens](#when-bootstrapping-happens)
      * [Mechanics of bootstrapping](#mechanics-of-bootstrapping)
   * [create_conda design notes](#create_conda-design-notes)
   * [Using YAML files](#using-yaml-files)
      * [Merging multiple YAML](#merging-multiple-yaml)
      * [Does pip install works?](#does-pip-install-works)
      * [How to specify multiple conda channel?](#how-to-specify-multiple-conda-channel)
      * [Commented out packages](#commented-out-packages)
      * [Comments](#comments)
   * [setenv.sh design notes](#setenvsh-design-notes)
   * [To debug](#to-debug)



<!--te-->

# Design principles for multiple Git repos

- We support multiple Git repos using Git submodules

- Avoid bash scripts at all costs
    - bash scripts are just flaky and unmanageable
    - use python whenever possible
    - if we really need to use bash scripts, we prefer to use python scripts that
      generate bash scripts to source (see `setenv.sh` and `_setenv.py`)
      
- Avoid using scripts from external repos calling into internal repos
    - We prefer to:
        - add general code performing the actions in the innermost repo (e.g.,
          `create_conda.py`)
        - each repo has a way to specify the actions needed through functions,
          config files (e.g., `YAML` files)
        - outer repos collect the actions from the needed repos, merge the
          actions into a single work plan, and call the code to perform the work

- Avoid messing up with `PATH` to automatically find executables
    - Better to be explicit with full path

- Avoid calling scripts with the same name in different repos
    - It's just too confusing

- Avoid factoring too much (e.g., bash scripts) since this creates all sort of
  subtle dependencies, especially when bootstrapping the environment
    - It's ok to have some redundancy (e.g., `setenv.sh`)

- Tweak `PYTHONPATH` to find each Git repo

- The outermost repo decides global configuration (e.g., Git user, emails, conda
  package) and all repos inherit these parameters

## Steps requiring composition of Git repos

1) installing a conda env
    - `create_conda.py`
    - we need to compute the union of the packages required by all repos
    - we should not use pinning of packages (yet another reasons why pinning is
      evil): otherwise we should let conda resolve the constraints, including `>=`

2) configuring a client
    - `setenv.sh`
    - we need to compute the union of the variables set, updating (e.g., `PATH`,
      `PYTHONPATH`) or overwriting some

3) handling credentials
    - E.g., for tunnels, for Git

4) handling `pytest`

# Configuring a Git client

## Bootstrapping

- The problem of bootstrapping is related to using our python libraries to create
  a python development environment in which we can use our python libraries

- In other words, think of the problem of using code from `helpers` from `//amp`
  before changing `PYTHONPATH` so that python can find `helpers`

- Our solution is to modify the running `PYTHONPATH` by injecting paths to the
  `//amp` libraries we use, e.g., `helpers`

## When bootstrapping happens

- There are 2 points for bootstrapping the development environment:
    1) when creating a conda environment in `create_conda.py`
        - we want to use our libraries (e.g., `dbg.dassert*`, `si.system`,
          logging), but we don't have a conda environment and we haven't
          configured yet our development environment
        - also `create_conda.py` can depend only from python system libraries

    2) when configuring a git client with `setenv.sh`
        - we want to use our libraries, but we haven't configured yet our
          development environment
        - since `setenv.sh` runs after `create_conda.py` we can use python
          libraries installed in our conda environment, although we prefer to
          avoid this

## Mechanics of bootstrapping

- `//amp/dev_scripts/_bootstrap.py` contains a function that updates the running
  library path using a relative path that points to `helpers`

- To avoid to replicate the `bootstrap` function we use relative imports to
  `bootstrap.py`, e.g., in `//amp/dev_scripts/_setenv.py`
    ```python
    # This script is `//amp/dev_scripts/_setenv.sh`, so we need to go up one level to
    # reach `//amp/helpers`.
    import _bootstrap as boot
    boot.bootstrap("..")
    ```
- After this call, we can import `//amp/helpers` libraries
    ```python
    # pylint: disable=C0413
    import helpers.dbg as dbg  # isort:skip # noqa: E402
    ```

# `create_conda` design notes

- `create_conda.py` is used to create a complete dev environment in a
  reproducible end-to-end way
  
- The problem of bootstrapping
    - `create_conda`
        - can only rely on standard Python libraries (otherwise it would depend on
          installing other packages)
        - use `amp` libraries
            - this is achieved by changing the running python path, before
             importing the `amp` libs from `helpers`
        
- Environment specification files are under
  `//amp/dev_scripts/install/requirements/`
  
- Jenkins runs a build to test a few `create_conda` environments

- It allows to select from different environments
    - E.g., `develop` is the official one
    - One can have special purpose environments (e.g., one with all
      experimental NLP libraries before they go in the main code)

- It allows to merge different environments (e.g., one from `//amp` and one
  from `p1`)
  
- It allows to save in the repo a list of all packages installed for future
  reference
  
# Using YAML files

- YAML files (instead of `.txt`) allow specifying also pip packages
- Refs:
    - [https://stackoverflow.com/questions/35245401]
    - [https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually]
    
- One can override the name of the package with:
    ```bash
    > conda env create -f dev_scripts/install/requirements/amp_develop.yaml -n test
    ```
  
## Merging multiple YAML
- One can merge different YAML files automatically with multiple `-f` options

## Does pip install works?
- It does work, e.g.,
    ```yaml
    ...
    - pip
    - pip:
          # works for regular pip packages
          - docx
          - gooey
          # and for wheels
          - http://www.lfd.uci.edu/~gohlke/pythonlibs/bofhrmxk/opencv_python-3.1.0-cp35-none-win_amd64.whl
    ```
- In our case:
    ```yaml
    name: amp_develop
    dependencies:
      - python >= 3.6
      - networkx
      - pip
      - pip:
         #pip install ta
         - ta
    ```
      
## How to specify multiple conda channel?
- One can use the `channels` statement
    ```yaml
    name: amp_develop
    channels:
      - quantopian
    dependencies:
    ```

## Commented out packages
- We comment out some packages to remember we used them in the past and for some
  reason we are not using them anymore or because they are creating problems
    ```yaml
    #- ta                   # Technical analysis package.
    #- python-graphviz      # To plot pymc3 graphical models.
    ```

## Comments
- We had comments to track why we need packages and what they are
    ```yaml
    - arviz                 # Needed by pymc3 for some plotting functionality.
    - mkl-service           # pymc3 expects it; not sure why conda doesn't solve for it.
    - pandas-datareader=0.8.0     # PartTask344.
    ```
- We use some tags, e.g., `# Not on Mac.`, to do conditional builds, since
  `conda` doesn't support them out of the box

# `setenv.sh` design notes

- Each `setenv.sh` (e.g., `//amp/dev_scripts/setenv.sh`,
  `//p1/dev_scripts/setenv.sh`)
    - contains some boiler plate code (calls `helpers.sh`, checks python version)
    - `execute_setenv`
        - calls `_setenv.py` to generate a bash script to configure
        - executes the bash script and configures the environment
    - is typically the same besides the pointer to `helpers.sh`

- `_setenv.py`
    - bootstraps the dev env
    - import `_setenv_lib` to get code that is used by all `_setenv.py`
    - do the specific work to configure the repo

# To debug
- From `dev_scripts/install/test_bootstrap.sh`
    ```bash
    > (cd $HOME/src/commodity_research1/amp; dev_scripts/_setenv_amp.py)
    > (cd $HOME/src/commodity_research1/amp; source dev_scripts/setenv.sh)
    > (cd $HOME/src/commodity_research1/amp; dev_scripts/install/create_conda.py -h)

    > (cd $HOME/src/commodity_research1; dev_scripts/_setenv_p1.py)
    > (cd $HOME/src/commodity_research1; source dev_scripts/setenv.sh)
    ```
