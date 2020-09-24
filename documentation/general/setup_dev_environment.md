<!--ts-->
   * [Tools](#tools)
      * [Editors](#editors)
      * [Development / Data science](#development--data-science)
      * [Infra](#infra)
   * [Set up a new machine](#set-up-a-new-machine)
      * [Server vs laptop](#server-vs-laptop)
      * [Definitions](#definitions)
      * [Connect to the server](#connect-to-the-server)
      * [Use python3](#use-python3)
      * [(optional) Install anaconda](#optional-install-anaconda)
      * [Configure anaconda](#configure-anaconda)
   * [Create a Git client](#create-a-git-client)
      * [Clone the git code](#clone-the-git-code)
      * [Configure git submodules](#configure-git-submodules)
      * [Configure user credentials](#configure-user-credentials)
      * [Create conda environment](#create-conda-environment)
      * [Check conda environment](#check-conda-environment)
      * [Configure conda environment](#configure-conda-environment)
      * [Delete / recreate environment](#delete--recreate-environment)
         * [Overwrite a conda environment with create_conda.py](#overwrite-a-conda-environment-with-create_condapy)
         * [Manually delete a conda environment](#manually-delete-a-conda-environment)
         * [To delete the entire conda installation (advanced users)](#to-delete-the-entire-conda-installation-advanced-users)
      * [Update anaconda](#update-anaconda)
      * [Clone multiple git client](#clone-multiple-git-client)
   * [Be patient](#be-patient)
   * [Workflow examples](#workflow-examples)
      * [Working with multiple clients](#working-with-multiple-clients)
      * [A simple set-up](#a-simple-set-up)
      * [GP's set-up](#gps-set-up)
      * [Run jupyter notebook](#run-jupyter-notebook)



<!--te-->

# Tools

## Editors

- PyCharm
- `vim` (or something worse like `emacs`)

## Development / Data science

- Python 3
- Conda: manage virtual environments
- Linux and bash: we prefer command line: learn how to use it
- Git: source control
- GitHub: repo and bug tracker
- ZenHub: project management
- Chrome (recommended) or Firefox: browser
- Google docs and markdown: documentation
- Google suite: email, calendar
- Email client (recommended) or Gmail web client
- Telegram: instant messaging
- Standard data science stack (e.g., Jupyter, pandas, numpy)

## Infra

- WireGuard: VPN
- AWS: computing infrastructure
- Jenkins: continuous integration and development
- MongoDB, SQL: DB backends
- Docker: container

# Set up a new machine

## Server vs laptop

- We prefer to work on the dev server on AWS since it is more reliable and
  powerful

- You have an option to work on your laptop, but it's not officially supported,
  so you are kind of your own
  - People use their laptop as thin client:
    - Use PyCharm
    - Use the browser, email
    - To develop when Internet is slow

- You need to set up any machine you use (e.g., laptop and AWS) in order to
  develop

- We recommend to set up the server first and then over time set up also the
  laptop for some development

## Definitions

- We refer to Git repos in the following way:
  - `ParticleDev/commodity_research` as `//p1`
  - `alphamatic/amp` as `//amp`

## Connect to the server

- After enabling the VPN on your laptop, open a terminal
- Make sure you see the servers:

  ```bash
  > ping research.p1
  PING research.p1 (172.31.16.23): 56 data bytes
  64 bytes from 172.31.16.23: icmp_seq=0 ttl=63 time=19.780 ms
  ...
  ```

- Try to connect to the servers:
  ```bash
  > ssh research.p1
  ```
- Best course of action is to pass your public key to Infra so that you can
  login without typing in a password

## Use python3

- Confirm that python 3 is the default upon running `python -V`, e.g.,
  ```bash
  > python -V
  Python 3.7.3
  ```

## (optional) Install anaconda

- For the AWS machine there is already a central conda, so there is no need for
  users to install
- For a laptop you need to install it yourself
  - You need _anaconda3_

## Configure anaconda

- Configure anaconda for your shell using:
  ```bash
  > conda init bash
  ```
- Anaconda3 adds a snippet of code in your `.bashrc`

# Create a Git client

## Clone the git code

- You can clone the code multiple times in different directories, if you want to
  have multiple clients
  - E.g., `$HOME/src/commodity_research1`, `$HOME/src/commodity_research2`, ...
- For now let's create a single client

- To clone the code for the first time run:

  ```bash
  > DST_DIR="commodity_research"
  > git clone --recursive git@github.com:ParticleDev/commodity_research.git $DST_DIR
  ```

- If you encounter the error

  ```bash
  bash git@github.com: Permission denied (publickey).
  fatal: Could not read from remote repository.

  Please make sure you have the correct access rights
  and the repository exists.
  ```

  make sure that your SSH key in `$HOME/.ssh/id_rsa.pub` is on your GitHub
  account. Follow the instructions
  [here](https://help.github.com/en/articles/adding-a-new-ssh-key-to-your-github-account)

- If you have problems run
  ```bash
  > ssh -T git@github.com
  ```
  and follow this
  [tutorial](https://help.github.com/en/github/authenticating-to-github/testing-your-ssh-connection)

## Configure git submodules

- Make sure you have submodule (e.g., `amp`) by running:

  ```bash
  > cd $DST_DIR
  > ls amp
  ```

- Make sure each submodule uses the `master` branch:
  ```bash
  > cd $DST_DIR
  > (cd amp; git checkout master)
  ```

## Configure user credentials

- For now this topic is obsolete. All development with AWS is running on a server
side (or locally) in a docker container. Here you can find the documentation
[the link](https://github.com/ParticleDev/commodity_research/blob/master/documentation_p1/technical/docker.md)

- Update the user credential files in `amp/helpers/user_credentials.py`
  - Commit this so all your clients are configured
- Typically you can just copy-paste a portion of the configuration of another
  user

## Create conda environment

- This is needed to install all the packages that are required for development:
  ```bash
  > cd $DST_DIR
  > ./dev_scripts_p1/create_conda.p1_develop.sh
  ```
- This script takes 5 mins to run

- You need to create an environment for every server you use (e.g., for the AWS
  server `research.p1`, for your laptop)
- You can reuse the same environment for multiple Git clients

## Check conda environment

- Check that your conda environment exists:
  ```bash
  > conda info --envs
  # conda environments:
  #
  base                     /anaconda3
  p1_develop            *  /home/<USER>/.conda/envs/p1_develop
  ```

## Configure conda environment

- Every time you open a shell you need to activate the development environment
  run:

  ```bash
  > source dev_scripts_p1/setenv_p1.sh
  ```

- This script:
  - Activates the conda environment
  - Sets environment variables
  - Makes sure things are working properly

## Delete / recreate environment

### Overwrite a conda environment with `create_conda.py`

- You can use the option `--delete_env_if_exists` to overwrite a conda env,
  creating it from scratch
- This is the typical approach

- There are some pre-packaged command lines to create the standard environments,
  e.g., `./dev_scripts_p1/create_conda.p1_develop.sh`

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

- To update anaconda (i.e., the framework that manages conda packages and
  `conda` executable)

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

## Clone multiple git client

- To check out another copy of the codebase (e.g., see possible workflows below)
  do:
  ```bash
  > more dev_scripts_p1/git_checkout.sh
  #!/bin/bash -xe
  DST_DIR="commodity_research"
  git clone --recursive git@github.com:ParticleDev/commodity_research.git $DST_DIR
  ```

# Be patient

- The `create_conda.py` flow is designed to make our projects portable across:
  - Platforms (e.g., macOS, Linux)
  - Different installation of OSes (e.g., GP's laptop vs Paul's laptop) with all
    the peculiar ways we install and manage servers and laptops
  - Different versions of conda
  - Different versions of python 3.x
  - Different versions of python packages

- There is no easy way to make sure that `create_conda.py` works for everybody
  - We can only make sure that Jenkins builds the environment correctly in its
    set-up by following the process described above
  - Try to follow the steps one by one, using a clean shell, cutting and pasting
    commands
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
  - One for development
  - One for review CLs
- One terminal window per Git client
  - (So I can switch easily between Git clients)
- One Pycharm project for each Git client
  - To edit the code
- One tmux session in each terminal with:
  - (So I can switch easily between dirs of the project)
  - One shell cd-ed in `commodity_research*`
  - One shell running jupyter
  - One shell cd-ed `commodity_research*/amp`
  - See details `//amp/dev_scripts/tmux.sh`

## Run jupyter notebook

- Launch jupyter notebook on the server:
  - Everyone chooses which port to use.
  - Running notebook without a tmux session will stop as soon as you leave the
    server.
  - If you want to have a constantly running notebook, create a tmux session.
    - `tmux` - Create tmux session.
    - `tmux a` - Connect to the last session.
    - Leave/detach the tmux session by typing `Ctrl+b` and `then d`.
  - Example run notebook:
    - IP - You can allow all addresses, but we expect you to use the internal
      server addresses(Example: 172.31.16.23).
    - PORT - use your port instead of a variable {PORT}(Example: 8088).
  ```bash
  jupyter notebook --ip=172.31.16.23 --port 8088
  ```
