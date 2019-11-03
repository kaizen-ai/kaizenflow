<!--ts-->
   * [Set up a new machine](#set-up-a-new-machine)
      * [Definitions](#definitions)
      * [Connect to the server](#connect-to-the-server)
      * [(optional) Install python3](#optional-install-python3)
      * [(optional) Install anaconda](#optional-install-anaconda)
      * [(optional) Configure anaconda](#optional-configure-anaconda)
   * [Create a Git client](#create-a-git-client)
      * [Check out the git code](#check-out-the-git-code)
      * [Configure git submodules](#configure-git-submodules)
      * [Configure user credentials](#configure-user-credentials)
      * [Create conda environment](#create-conda-environment)
      * [Check conda environment](#check-conda-environment)
      * [Configure conda environment](#configure-conda-environment)
      * [Delete / recreate environment](#delete--recreate-environment)
         * [Overwrite a conda environment with create_conda.py](#overwrite-a-conda-environment-with-create_condapy)
         * [Manually delete a conda environment](#manually-delete-a-conda-environment)
         * [To delete the entire conda installation (advanced users)](#to-delete-the-entire-conda-installation-advanced-users)
   * [Be patient](#be-patient)
   * [Workflow examples](#workflow-examples)
      * [Working with multiple clients](#working-with-multiple-clients)
      * [A simple set-up](#a-simple-set-up)
      * [GP's set-up](#gps-set-up)



<!--te-->

# Set up a new machine

- You need to set up any machine you use (e.g., laptop and AWS) in order to
  develop

## Definitions

- We refer to Git repos in the following way:
    - `ParticleDev/commodity_research` as `//Part`
    - `alphamatic/amp` as `//Amp`

## Connect to the server

- E.g., `ssh 3.16.128.114`
- Add your pub key to the server so that you can login without typing in a
  password

## (optional) Install python3

- Check on your computer how to install

## (optional) Install anaconda

- For the AWS machine there is already a central conda, and so there is no need
  for users to install
- For a laptop you need to install it yourself
- You need *anaconda3*

## (optional) Configure anaconda

- If you installed anaconda, you need to configure anaconda for your shell
    ```bash
    > conda init bash
    ```
- Anaconda3 adds a snippet of code in your `.bashrc`

# Create a Git client

## Check out the git code

- You can check out the code multiple times in different directories, if you want
  to have multiple clients

- To check out the code for the first time, do:
    ```bash
    > DST_DIR="commodity_research"
    > git clone --recursive git@github.com:ParticleDev/commodity_research.git $DST_DIR
    ```
    - If you encounter the error
        ```buildoutcfg
        git@github.com: Permission denied (publickey).
        fatal: Could not read from remote repository.

        Please make sure you have the correct access rights
        and the repository exists.
        ```
    then make sure that your SSH key in `/.ssh/id_rsa.pub` is on your GitHub
    account. Follow the instructions
    [here](https://help.github.com/en/articles/adding-a-new-ssh-key-to-your-github-account)

- To check out another copy of the codebase (e.g., see possible workflows below)
  do:
    ```bash
    > more dev_scripts/git_checkout.sh
    #!/bin/bash -xe
    DST_DIR="commodity_research"
    git clone --recursive git@github.com:ParticleDev/commodity_research.git $DST_DIR
    ```

## Configure git submodules

- This is needed to have each submodule use the `master` branch
    ```bash
    > cd commodity_research
    > (cd amp; git checkout master)
    > (cd infra; git checkout master)
    ```

- Make sure you have both submodule repos `infra` and `amp`

## Configure user credentials

- Update the user credential files in `amp/helpers/user_credentials.py`
    - Commit this so all your clients are configured
- Typically you can just copy-paste a portion of the configuration of another
  user

## Create conda environment

- This is needed to install all the packages that are required for development
    ```bash
    > cd commodity_research
    > ./dev_scripts/create_conda.p1_develop.sh
    ```

## Check conda environment

- Check that your conda environment is working
    ```bash
    > conda info --envs
    # conda environments:
    #
    base                     /anaconda3
    p1_develop            *  /home/<USER>/.conda/envs/p1_develop
    ```

## Configure conda environment

- Every time you cd in a shell:
- You need to run:
    ```bash
    source dev_scripts/setenv.sh
    ```
    
## Delete / recreate environment

### Overwrite a conda environment with `create_conda.py`

- You can use the option `--delete_env_if_exists` to overwrite a conda env,
  creating it from scratch
- This is the typical approach

- There are some pre-packaged command lines to create the standard environments,
  e.g., `./dev_scripts/create_conda.p1_develop.sh`
      ```bash
      > amp/dev_scripts/install/create_conda.py \
        --env_name $CONDA_ENV \
        --req_file amp/dev_scripts/install/requirements/develop.yaml \
        --req_file dev_scripts/install/requirements/p1_develop.yaml \
        --delete_env_if_exists
      ```

- The `create_conda.py` help as some useful examples of command lines, see the
  help:
    ```bash
    > create_conda.py -h
    ```

### Manually delete a conda environment

- You can delete a conda environment by simply deleting the corresponding
  directory
- The conda command tries to be smart removing the packages and leaving the dir,
  but IMO it doesn't always work
- You look at the environments with:
    ```bash
    > conda info --envs
    # conda environments:
    #
    ...
    develop               *  /Users/<USER>/.conda/envs/develop
    ...
    ```
- Then you can delete with:
    ```bash
    > rm -rf /Users/<USER>/.conda/envs/develop
    ```
- It's a good idea to move it so you can resume it if something goes wrong:
    ```bash
    > mv /Users/<USER>/.conda/envs/develop > /Users/<USER>/.conda/envs/develop.OLD
    ```
    - Note that `develop.OLD` might not work anymore since all the links are
      broken by the move

### To delete the entire conda installation (advanced users)

- This is a dangerous operation, since it deletes the executable `conda`
    - You want to do this only when your environment is screwed up: a more expert
      team member can help you diagnose it
- If you want to delete your conda installation, find the base env
    ```bash
    > conda info --envs
    base                     /anaconda3
    ...
    ```
- Run `rm -rf /anaconda3`
- A good idea is to move it so you can resume the state

## Update anaconda

- To update anaconda (i.e., the framework that manages conda packages and `conda`
  executable)
  ```bash
  > conda activate base
  # Remove index cache, lock files, tarballs, unused cache packages, and source
  # cache.
  > conda clean --all
  > conda update conda
  > conda update anaconda
  > conda -V
  conda 4.7.12
  ```

- You can try to activate one environment
    ```bash
    > conda activate amp_develop
    > which python
    /Users/saggese/.conda/envs/amp_develop/bin/python
    ```

# Be patient

- The `create_conda.py` flow is designed to make our projects portable across:
    - platforms (e.g., macOS, Linux)
    - different installation of OSes (e.g., GP's laptop vs Paul's laptop) with
      all the peculiar ways we install and manage servers and laptops
    - different versions of conda
    - different versions of python 3.x
    - different versions of python packages

- There is no easy way to make sure that `create_conda.py` works for everybody
    - We can only make sure that Jenkins builds the environment correctly in its
      set-up by following the process described above
    - Try to follow the steps one by one, using a clean shell, cutting and
      pasting commands
    - If you hit a problem, be patient, ping GP / Paul, and we will extend the
      script to handle the quirks of your set-up

# Workflow examples

## Working with multiple clients

- Different people have different set-ups that reflect their workflows

## A simple set-up

- Always work from the Git repo `//Part`, typically `commodity_research`
- You might need to `cd` back and forth between the two repos `//Part` and
  `//Amp`

## GP's set-up

- My set-up is a bit on the complicated side:
    - I like to have multiple clients (a residual behavior from subversion that
      doesn't allow to switch clients as quickly as Git)
    - One client is always at `master`
    - One client for checking out branches to do reviews
    - One client for development

- Two Git clients `commodity_research1` and `commodity_research2`
    - one for development
    - one for review CLs
- One terminal window per Git client
    - (So I can switch easily between Git clients)
- One Pycharm project for each Git client
    - To edit the code
- One tmux session in each terminal with:
    - (So I can switch easily between dirs of the project)
    - one shell cd-ed in `commodity_research*`
    - one shell running jupyter
    - one shell cd-ed `commodity_research*/amp`
    - See details `//Amp/dev_scripts/tmux.sh`
