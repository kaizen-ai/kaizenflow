# Supporting OS
- We support Mac x86, Apple Silicon and Linux Ubuntu
- If you are using Windows,
  - Install VMWare software
  - Reference video for installing [ubuntu](https://www.youtube.com/watch?v=NhlhJFKmzpk&ab_channel=ProgrammingKnowledge) on VMWare software
  - Make sure you set up your git and github
  - Install [docker](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository) on your Ubuntu VM

# Getting a first issue to work on (aka warm-up issue)

- The goal is to get comfortable with the development system

- We mark certain bugs as "good-as-first-bug"

- Typical warm-up issues are:
  - Write unit tests
  - Copy-paste-modify (ideally refactor and then change) code
  - Simple refactoring

# How to contribute code (short version)

- Make sure you are familiar with our coding style
- Create a branch of your assigned issues/bugs
  - E.g., for a GitHub issue with the name "Expose the linter container to
    Sorrentum contributors #63", the branch name should be
    `SorrTask63_Expose_the_linter_container_to_Sorrentum_contributors`
  - This step is automated through the `invoke` flow (see docs for more info)
- Run the [linter](https://github.com/sorrentum/sorrentum/blob/master/docs/First_review_process.md#run-linter) on your code before pushing.
- Push the code to your branch
- Make sure your branch is up-to-date with the master branch
- Create a Pull Request (PR) from your branch
- Add your assigned reviewers for your PR so that they are informed of your PR
- After being reviewed, the PR will be merged to the master branch by your
  reviewers

# Cloning the code

- All the source code should go under `~/src` (e.g., `/Users/<YOUR_USER>/src` on a Mac PC)
- The path to the local repo folder should look like this `~/src/{REPO_NAME}{IDX}` where
  - `IDX` is an integer
  - `REPO_NAME` is a name of the repository
- To clone the repo, use the cloning command described in [the Github official documentation](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository-from-github/cloning-a-repository)
- Example of cloning command:
```
# Sometimes it does not work.
> git clone git@github.com:sorrentum/sorrentum.git ~/src/sorrentum1
# Alternative command.
> git clone https://github.com/sorrentum/sorrentum.git ~/src/sorrentum1
```
  

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
   Using default tag: latest
   latest: Pulling from library/hello-world
   Digest: sha256:fc6cf906cbfa013e80938cdf0bb199fbdbb86d6e3e013783e5a766f50f5dbce0
   Status: Image is up to date for hello-world:latest
   docker.io/library/hello-world:latest
   ```

- Common problems with Docker
   - Mac DNS problem, try step 5 from the [article](https://medium.com/freethreads/mac-os-docker-error-response-from-daemon-net-http-request-canceled-while-waiting-for-connection-7d1069eb4ca9) and repeat the cmd below:
   ```
   > docker pull hello-world
   Error response from daemon: net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)
   ``` 
   - Linux sudo problem, see [here](https://stackoverflow.com/questions/48568172/docker-sock-permission-denied) for the solution
   ```
   > docker pull hello-world
   Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock: Get http://%2Fvar%2Frun%2Fdocker.sock/v1.40/containers/json: dial unix /var/run/docker.sock: connect: permission denied
   ```


- Thin environment

  1. Build the thin environment; this is done once

  ```
  > source dev_scripts/client_setup/build.sh
  ```

  2. Activate the thin environment; make sure it is always activated

  ```
  > source dev_scripts/setenv_amp.sh
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
  - in the output from the cmd above find an assigned port, e.g., `[I 14:52:26.824 NotebookApp] http://0044e866de8d:10091/` -> port is `10091`
  - add the port to the link like so: `http://localhost:10091/` or `http://127.0.0.1:10091`
  - copy-paste the link into a web-browser and update the page
