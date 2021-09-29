<!--ts-->
   * [How to setup](#how-to-setup)
      * [Documentation](#documentation)
      * [Setting up a tmux session](#setting-up-a-tmux-session)
      * [Development flow](#development-flow)
      * [Setting up](#setting-up)
      * [Docker](#docker)
   * [AWS credentials](#aws-credentials)



<!--te-->

# How to setup

## Documentation

- Documentation is under `documentations/notes` and in the directory of the
  corresponding tools

## Setting up a tmux session

- Create a tmux session for amp set-up
  ```bash
  > cd ~/src
  > ./dev_scripts/tmux.sh 1
  ```

## Development flow

- We package in Docker containers what is needed to run AM system
  - E.g., command lines and unit tests should always run inside a container
- We don't development tools save inside container, but we use the local machine
  to develop (e.g., running PyCharm)
- Some workflows are run on a development machine without Docker
  - We install a light virtual environment with the minimum set of dependency
    - We use [pyinvoke](http://www.pyinvoke.org/), a replacement for `make`
      written in Python, to create workflows
  - `invoke` always runs outside Docker and in a shell on the dev machine
  - The code for the client setup is under `dev_scripts/client_setup`

## Cloning the repo
- Path to the local repo folder should look like this:
   ```bash
   ~/src/amp{IDX}
   ```
where `IDX` is an integer, e.g. `~/src/amp1`, `~/src/amp2`
   - This is required to run `go_amp.sh` script
   - To clone a repo, use SSH cloning command described [here in Github official documentation](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository-from-github/cloning-a-repository).
      ```bash
      > git clone git@github.com:cryptomtc/amp.git /your/path/amp1
      ```
## Setting up the env
- After cloning the repo, copy `dev_scripts/go_amp.sh` script to your home (`~`) folder:
   ```bash
   > cp dev_scripts/go_amp.sh ~
   ```

- Build the thin virtual env on the dev machine

  ```bash
  > dev_scripts/client_setup/build.sh
  ```

- Configure the env
  - `source dev_scripts/setenv_amp.sh`

## Docker

- Create a docker bash to run interactively (e.g., `pytest` or command lines)

  ```bash
  # Pull the container.
  > invoke docker_pull
  > invoke docker_bash
  ```

- Run fast tests:
  ```bash
  > invoke run_fast_tests
  ```

# AWS credentials

- AWS credentials are passed from the user `~/.aws` directory that is
  bind-mounted to the Docker container
- The container passes also through the env vars "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"
  - These env vars are empty by default
  - For GitHub Actions we pass the credentials through the env vars and GH
    secrets
