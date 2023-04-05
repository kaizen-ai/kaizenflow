# The `defi` container

## Build the `defi` container

- To build the container storing all the DeFi toolchain:
  ```
  > cd $GIT_ROOT/defi/devops
  > docker_build.sh
  ```

- The container is controlled with the scripts
  - `docker_bash.sh`: start a new `defi` container
  - `docker_exec.sh`: start a new bash in the `defi` container
  - `docker_build.sh`: build the container
  - `docker_kill.sh`: kill all the `defi` containers

## Run the `defi` container

- Typically you want to have one `defi` container with multiple terminals
  attached to it to run servers (e.g., Ganache) and clients (e.g., Jupyter, bash)
- We suggest to use `tmux` to keep all the terminals in a single window
  and make it easy to control via keyboard
- Note that you have to run docker commands exactly from the `/defi` subdir
  ```
  > cd $GIT_ROOT/defi
  > devops/docker_bash.sh

  # Go to a new terminal.
  # Start a new bash on the same container.
  > devops/docker_exec.sh

  ...

  # Go to a new terminal.
  # Start a new bash on the same container.
  > devops/docker_exec.sh
  ```

## Install dependencies

- To complete the set-up you need to install node libraries to each project
- Open a new window and start a new bash in the running container
  ```
  > devops/docker_exec.sh
  docker> cd data/
  docker> devops/install_node_modules.sh
  ```

## Start Ganache

- Open a new window and start a new bash in the running container
  ```
  > devops/docker_exec.sh
  docker> /data/devops/run_ganache.sh
  ```

- The output of Ganache is in `/root/ganache.log`

## Start Jupyter

- Open a new window and start a new bash in the running container
  ```
  > devops/docker_exec.sh
  docker> /data/devops/run_jupyter.sh
  ```
- Go to a link that corresponds to your server to make sure Jupyter is running:
  - On your laptop, localhost:8888
  - On `dev1`, http://172.30.2.136:8889/tree?
  - On `dev2`, http://172.30.2.128:8889/tree?
