# Set up a new machine
- You need to set up any machine you use (e.g., laptop and AWS) in order to
  develop

## Definitions
- We refer to Git repos in the following way:
    - `ParticleDev/commodity_research` as `//Part`
    - `alphamatic/amp` as `//Amp`

## Connect to the server
- E.g., ssh 3.16.128.114
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
  - ```bash
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
    account. Follow the instructions here:
    https://help.github.com/en/articles/adding-a-new-ssh-key-to-your-github-account

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

## Check conda environment
- Check that your conda environment is working
    ```bash
    > conda info --envs
    # conda environments:
    #
    base                     /anaconda3
    p1_develop            *  /home/saggese/.conda/envs/p1_develop
    ```

## Configure conda environment
- Every time you cd in a shell:
- You need to run:
    ```bash
    source dev_scripts/setenv.sh
    ```
    
## Delete/Recreate environment
todo: (GP) please add procedure for Delete/Recreate

## Working with multiple clients
- Different people have different set-ups that reflect their workflows

## A simple set-up
- Always work from the Git repo `//Part`, typically `commodity_research`
- You might need to `cd` back and forth between the two repos `//Part` and
  `//Amp`

## GP's set-up
- two Git clients `commodity_research1` and `commodity_research2`
    - one for development
    - one for review CLs
- one terminal window per Git client
    - (So I can switch easily between Git clients)
- one pycharm project for each Git client
    - To edit the code
- one tmux session in each terminal with:
    - (So I can switch easily between dirs of the project)
    - one shell cd-ed in `commodity_research*`
    - one shell running jupyter
    - one shell cd-ed `commodity_research*/amp`
    - See details `//Amp/dev_scripts/tmux.sh`
