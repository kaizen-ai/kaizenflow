

<!-- toc -->

- [Sorrentum Dev Docker container (aka dev container, cmamp container)](#sorrentum-dev-docker-container-aka-dev-container-cmamp-container)
- [Getting a first issue to work on (aka warm-up issue)](#getting-a-first-issue-to-work-on-aka-warm-up-issue)

<!-- tocstop -->

Refer to
[docs/work_tools/data605.basic_development.how_to_guide.md](docs/work_tools/data605.basic_development.how_to_guide.md)
for initial setup and guidance, then transition to this document for further
instructions

# Sorrentum Dev Docker container (aka dev container, cmamp container)

- Get familiar with Docker if you are not (e.g.,
  https://docs.docker.com/get-started/overview/)

- We work in a Docker container that has all the required dependencies installed
  - You can use PyCharm / VS code on your laptop to edit code, but you want to
    run code inside the dev container since this makes sure everyone is running
    with the same system, and it makes it easy to share code and reproduce
    problems

- Install Docker Desktop on your PC
  - Links:
    - [Mac](https://docs.docker.com/desktop/install/mac-install/)
    - [Linux](https://docs.docker.com/desktop/install/linux-install/)
    - [Windows](https://docs.docker.com/desktop/install/windows-install/)
  - Check the installation by running:
    ```
    > docker pull hello-world
    Using default tag: latest
    latest: Pulling from library/hello-world
    Digest: sha256:fc6cf906cbfa013e80938cdf0bb199fbdbb86d6e3e013783e5a766f50f5dbce0
    Status: Image is up to date for hello-world:latest
    docker.io/library/hello-world:latest
    ```

- Common problems with Docker
  - Mac DNS problem, try step 5 from the
    [article](https://medium.com/freethreads/mac-os-docker-error-response-from-daemon-net-http-request-canceled-while-waiting-for-connection-7d1069eb4ca9)
    and repeat the cmd below:
    ```
    > docker pull hello-world
    Error response from daemon: net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)
    ```
  - Linux sudo problem, see
    [here](https://stackoverflow.com/questions/48568172/docker-sock-permission-denied)
    for the solution
    ```
    > docker pull hello-world
    Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock: Get   http://%2Fvar%2Frun%2Fdocker.sock/v1.40/containers/json: dial unix /var/run/docker.sock: connect: permission denied
    ```

- Docker image

  1. Pull the latest cmamp image; this is done once
     ```
     > i docker_pull
     or
     > docker pull sorrentum/cmamp:latest
     ```

  2. Pull the latest dev_tools image; this is done onec
     ```
     > i docker_pull_dev_tools
     or
     > docker pull sorrentum/dev_tools:prod
     ```

- Git

  1. Get the latest version of `master`
     ```
     # To update your feature branch with the latest changes from master run
     # the cmd below from a feature branch, i.e. not from master.
     > i git_merge_master
     # If you are on `master` just pull the remote changes.
     > i git_pull
     ```

- Basic Docker commands

  1. Start a Docker container
     ```
     > i docker_bash
     ```

  Ignore all the warnings that do not prevent you from running the tests, e.g.,
  ```
  WARNING: The AM_AWS_ACCESS_KEY_ID variable is not set. Defaulting to a blank string.
  WARNING: The AM_AWS_DEFAULT_REGION variable is not set. Defaulting to a blank string.
  WARNING: The AM_AWS_SECRET_ACCESS_KEY variable is not set. Defaulting to a blank string.
  WARNING: The AM_FORCE_TEST_FAIL variable is not set. Defaulting to a blank string.
  WARNING: The CK_AWS_ACCESS_KEY_ID variable is not set. Defaulting to a blank string.
  WARNING: The CK_AWS_DEFAULT_REGION variable is not set. Defaulting to a blank string.
  WARNING: The CK_AWS_SECRET_ACCESS_KEY variable is not set. Defaulting to a blank string.
  WARNING: The CK_TELEGRAM_TOKEN variable is not set. Defaulting to a blank string.
  ```
  ```
  -----------------------------------------------------------------------------
  This code is not in sync with the container:
  code_version='1.4.1' != container_version='1.4.0'
  -----------------------------------------------------------------------------
  You need to:
  - merge origin/master into your branch with `invoke git_merge_master`
  - pull the latest container with `invoke docker_pull`
  ```

  2. Start a Jupyter server
     ```
     > i docker_jupyter
     ```

  To open a Jupyter notebook in a local web-browser:
  - In the output from the cmd above find an assigned port, e.g.,
    `[I 14:52:26.824 NotebookApp] http://0044e866de8d:10091/` -> port is `10091`
  - Add the port to the link like so: `http://localhost:10091/` or
    `http://127.0.0.1:10091`
  - Copy-paste the link into a web-browser and update the page

# Getting a first issue to work on (aka warm-up issue)

- The goal is to get comfortable with the development system

- We mark certain bugs as "good-as-first-bug"

- Typical warm-up issues are:
  - Write unit tests
  - Copy-paste-modify (ideally refactor and then change) code
  - Simple refactoring
