# How to setup

## Documentation

- Documentation is under `documentations/notes` and in the directory of the
  corresponding tools

## Setting up a tmux session

- Create a tmux session for lem + amp set-up
  ```bash
  > cd ~/src
  > ./dev_scripts_p1/tmux_p1.sh part1
  ```
- TODO(gp): It might be dependent on GP's set-up and needs to be generalized,
  if worth it for general consumption

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

## Setting up
- Build the thin virtual env on the dev machine
  ```
  > dev_scripts/client_setup/build.sh
  ```

- Activate the virtual env
  ```bash
  > source dev_scripts/client_setup/activate.sh

- Configure the env
  - `source dev_scripts/setenv_amp.sh`

## Docker

- Create a docker bash to run interactively (e.g., `pytest` or command lines)
  ```
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
