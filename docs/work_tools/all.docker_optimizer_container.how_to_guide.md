

<!-- toc -->

- [Optimizer container](#optimizer-container)
  * [Rationale](#rationale)
  * [Build and run a local version of `opt`](#build-and-run-a-local-version-of-opt)
  * [Internals](#internals)
    + [One container per Git repo](#one-container-per-git-repo)
    + [Multiple containers per Git repo](#multiple-containers-per-git-repo)
      - [Mounting only `optimizer` dir inside Docker](#mounting-only-optimizer-dir-inside-docker)
      - [Mounting the supermodule (e.g., lime, lemonade, amp) inside Docker](#mounting-the-supermodule-eg-lime-lemonade-amp-inside-docker)
  * [Invariants](#invariants)
  * [Release and ECR flow](#release-and-ecr-flow)
  * [Unit testing code inside `opt` container](#unit-testing-code-inside-opt-container)
    + [Avoid compiling code depending from cvxopt when running amp](#avoid-compiling-code-depending-from-cvxopt-when-running-amp)
    + [Run optimizer tests in a stand-alone `opt` container](#run-optimizer-tests-in-a-stand-alone-opt-container)
    + [Run optimizer tests as part of running unit tests for `cmamp`](#run-optimizer-tests-as-part-of-running-unit-tests-for-cmamp)
  * [Call a Dockerized executable from a container](#call-a-dockerized-executable-from-a-container)

<!-- tocstop -->

# Optimizer container

## Rationale

- The high-level goal is to move towards containerized Python scripts running in
  smaller containers instead of keep adding packages to `amp` / `cmamp`, which
  makes the `amp` / `cmamp` container bloated and risky to build
- Along this design philosophy similar to microservices, we want to have a
  Docker container, called `opt` with a Python script that uses some packages
  that are not compatible with `amp` (specifically cvxopt, cvxpy)
- This is similar to what we do for the `dev_tools`, which is like a
  containerized Python script for the linter

## Build and run a local version of `opt`

- You can build the container locally with:
  ```
  > cd optimizer
  > i opt_docker_build_local_image --version 0.1.0
  ```
- This process takes around 5 mins and then you should have the container
  ```
  docker image ls 665840871993.dkr.ecr.us-east-1.amazonaws.com/opt:local-saggese-0.1.0
  REPOSITORY                                         TAG                   IMAGE ID       CREATED         SIZE
  665840871993.dkr.ecr.us-east-1.amazonaws.com/opt   local-saggese-0.1.0   bb7d60d6a7d0   7 seconds ago   1.23GB
  ```
- Run the container as:
  ```
  > i opt_docker_bash --stage local --version 0.1.0
  ```
- To run a Jupyter notebook in the `opt` container:

## Internals

### One container per Git repo

- A simple approach is to have each deployable unit (i.e., container)
  corresponding to a Git repo
  - The consequence would be:
    - A multiplication of repos
    - No implicit sharing of code across different containers
    - Some mechanism to share code (e.g., `helpers`) across repos (e.g., using
      bind mount)
    - Not playing nice with Git subrepo mechanism since Docker needs to see the
      entire repo

- So the code would be organized in 4 repos:
  ```
  - lemonade / lime
      - helpers
      - optimizer
      - oms
      - models in amp
  ```
  - Where the dependency between containers are
    - Lemonade -> amp
    - Amp -> optimizer, helpers
    - Optimizer -> helpers, core

### Multiple containers per Git repo

- Another approach is to have `optimizer` as a directory inside `amp`
  - This keeps `amp` and `optimizer` in a single repo
  - To build / run optimizer code in its container one needs to `cd` in the dir
  - The problem then becomes how to share `helpers`

#### Mounting only `optimizer` dir inside Docker

- From `devops/compose/docker-compose.yml`
  ```
  42 volumes:
  43  # Move one dir up to include the entire git repo (see AmpTask1017).
  44  - ../../:/app
  45 # Move one dir down to include the entire git repo (see AmpTask1017).
  46 working_dir: /app
  ```
- From `devops/docker_build/dev.Dockerfile`
- ENTRYPOINT ["devops/docker_run/entrypoint.sh"]
- The problem is that Git repo doesn't work anymore
  ```
  git --version: git version 2.30.2
  fatal: not a git repository (or any parent up to mount point /)
  Stopping at filesystem boundary (GIT_DISCOVERY_ACROSS_FILESYSTEM not set).
  ```
- A work around is to inject .git in /git of the container and then point git to
  that
  ```
  environment:
  ...
  - GIT_DIR=/git

  volumes:
    # Move one dir up to include the entire git repo (see AmpTask1017).
    - ../../:/app
    - ../../../../.git:/git
    - ../../../../amp/helpers:/app/helpers
  ```

- Git works but it gets confused with the paths
  ```
      modified: .dockerignore
      deleted: .github/gh_requirements.txt
      deleted: .github/workflows/build_image.yml.DISABLED
      deleted: .github/workflows/fast_tests.yml
      deleted: .github/workflows/linter.yml.DISABLED
      deleted: .github/workflows/slow_tests.yml
      deleted: .github/workflows/superslow_tests.yml.DISABLED
      deleted: .gitignore
  ```

#### Mounting the supermodule (e.g., lime, lemonade, amp) inside Docker

- From `devops/compose/docker-compose.yml`
  ```
  42 volumes:
  43  # Move one dir up to include the entire git repo (see AmpTask1017).
  44  - ../../../:/app
  45 # Move one dir down to include the entire git repo (see AmpTask1017).
  46 working_dir: /app/amp
  ```
- From `devops/docker_build/dev.Dockerfile`
- ENTRYPOINT ["optimizer/devops/docker_run/entrypoint.sh"]
- This approach mounts 4 dirs up from devops/compose/docker-compose.yml, i.e.,
  //lime
- The problem with this approach is that now repo_config.py is incorrect
- `i opt_docker_build_local_image --version 0.4.0`
  ```
  32 - ../../../helpers:/app/amp/optimizer/helpers
  33
  34 # Shared cache. This is specific of lime.
  35 - /local/home/share/cache:/cache
  36
  37 # Mount `amp` when it is used as submodule. In this case we need to
  38 # mount the super project in the container (to make git work with the
  39 # supermodule) and then change dir to `amp`.
  40 app:
  41  extends:
  42    base_app
  43 volumes:
  44  # Move one dir up to include the entire git repo (see AmpTask1017).
  45  - ../../../../:/app
  46 # Move one dir down to include the entire git repo (see AmpTask1017).
  47 working_dir: /app/amp/optimizer
  48 #entrypoint: /bin/bash -c "ls helpers"
  ```

## Invariants

- A deployable dir is a dir under a Git repo
  - It corresponds to a software component (code + library = Docker container)
  - Anything that has a devops dir is "deployable"
- Each Docker container is run from its corresponding dir, e.g.,
  - Amp container from the amp dir
  - Amp container from the lemonade dir (this is just a shortcut since lemonade
    has the same deps right now as amp)
- Always mount the outermost Git repo under `/app`
- Set the Docker working dir as the current dir
- Each deployable dir specifies all the needed information in `repo_config.py`
  (which is the one in the current dir)
  - What container to run
  - What functionality is supported on different servers (e.g., privileged way)
- The `changelog.txt` file is in the deployable dir (e.g.,
  optimizer/changelog.txt)
- Each

One run the invoke commands from optimizer dir

When the Docker container starts the current dir is optimizer

helpers, core is mounted in the same dir

You can't see code outside optimizer

TODO(gp): running in amp under lemonade should use the local repo_config

## Release and ECR flow

TODO(gp): Implement this

## Unit testing code inside `opt` container

- Since we want to segregate the package dependencies in different containers,
  tests that have a dependency from cvxopt /cvxpy can't be run inside the `amp`
  container but need to be run inside `opt`.
- We want to:
  1. (as always) write and run unit tests for the optimizer code in isolation,
     i.e., test the code in the directory `optimizer` by itself
  2. Run all the tests for the entire repo (relying on both containers `amp` and
     `optimizer` with a single command invocation)
  3. Be able to run tests belonging to only one of the containers to shorten the
     debugging cycle
- To achieve this we need to solve the 3 problems below.

### Avoid compiling code depending from cvxopt when running amp

- We can't parse code (e.g., in `pytest`) that includes packages that are not
  present in a container
  - E.g., `pytest` running in `amp` should not parse code in `//amp/optimizer`
    since it contains imports that will fail

- **Solution 1**
  - We use the pytest mechanism `cvx = pytest.importorskip("cvxpy")` which is
    conceptually equivalent to:
    ```
    try:
      import cvxopt
      has_cvxopt = True
    except ImportError:
      has_cvxopt = False

    if has_cvxopt:
            def utils1():
                     cvxopt…
    ```

- **Solution 2**
  - Test in eachfile for the existence of the needed packages and enclose the
    code in an `if _has_package`
    - Pros:
      - We can skip code based dynamically on a `try ... except ImportModule` to
        check what packages are present
    - Cons:
      - Repeat the same piece of `try ... except` in many places
        - Solution: we can factor it out in a function
      - We need to enclose the code in a `if ...` that screws up the indentation
        and makes the code weird

- **Solution 3**
  - Exclude certain directories (e.g., `//amp/optimizer`) from `pytest`
    - Pros:
      - We don't have to spread the `try ... except` and `if \_has_package` in
        the code
    - Cons:
      - The directory is relative to the top directory
        - Solution: we can use a regex to specify the dir without the full path
      - Which directories are included and excluded depends on where `pytest` is
        run
        - E.g., running `pytest` in an `amp` container we need to skip the
          `optimizer` dir, while `pytest` in an `optimizer` container should
          skip everything but the `optimizer` dir

- **Solution 4**
  - Exclude certain directories or files based on which container we are running
    in
    - Cons:
      - We need to have a way to determine in which container we are running
        - Solution: we can use the env vars we use for versioning
        ```
        > echo $AM_CONTAINER_VERSION
        amp-1.0.3-
        ```
- Given the pros and cons, we decided to follow Solution 1 and Solution 3

### Run optimizer tests in a stand-alone `opt` container

- To run the optimizer tests, you can create an `opt ` container and then run
  `pytest`
  ```
  > cd optimizer
  > i opt_docker_bash
  docker> pytest .
  ```
- We wrap this in an invoke target like `i opt_run_fast_tests`

**Alternative solution**

- We can use dind to run the `opt` container inside a `cmamp` one
  - Cons:
    - Dind complicates the system
    - Dind is not supported everywhere (one needs privileged containers)
    - Dind is slower since there are 2 levels of (relatively fast)
      virtualization

### Run optimizer tests as part of running unit tests for `cmamp`

- We use the same mechanism as `run_fast_slow_superslow_tests` to pull together
  different test lists

## Call a Dockerized executable from a container

- From
  [https://github.com/cryptokaizen/cmamp/issues/1357](https://github.com/cryptokaizen/cmamp/issues/1357)
- We need to call something from `amp` to `opt` Docker

- **Solution 1**
  - Inside the code we build the command line
    `cmd = 'docker run -it ... '; system(cmd)`
    - Cons:
      - There is code replicated between here and the invoke task (e.g., the
        info about the container, ...)

- **Solution 2**
  - Call the Dockerized executable using the `docker_cmd` invoke target
    ```
    cmd = "invoke opt_docker_cmd -cmd '...'"
    system(cmd)
    ```
    - Pros:
      - All the Docker commands go through the same interface inside invoke
    - Cons
      - Bash interpolation in the command
      - Another level of indirection: do a system call to call `invoke`,
        `invoke` calls docker, docker does the work
      - `invoke` needs to be installed inside the calling container

- **Solution 3**
  - Call opt_lib_tasks.py `opt_docker_cmd(cmd, ...)`
    - Pros
      - Avoid doing a call to invoke
      - Can deal with bash interpolation in Python

- We should always use Solution 3, although in the code sometimes we use
  Solution 1 and 2 (but we should replace in favor of Solution 3).

##

- The interface to the Dockerized optimizer is in `run_optimizer` in
  `//amp/oms/call_optimizer.py`
- To run the examples
  ```
  > cd //lime
  > i docker_bash
  > pytest ./amp/oms/test/test_call_optimizer.py::Test_run_dockerized_optimizer1
  ```
