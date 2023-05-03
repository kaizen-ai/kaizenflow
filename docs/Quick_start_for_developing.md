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
> git clone git@github.com:sorrentum/sorrentum.git ~/src/sorrentum1
```
  

# Sorrentum Docker container

- Get familiar with Docker if you are not (e.g.,
  https://docs.docker.com/get-started/overview/)

- We work in a Docker container that has all the required dependencies installed

  - You can use PyCharm / VS code on your laptop to edit code, but you want to
    run code inside the dev container since this makes sure everyone is running
    with the same system, and it makes it easy to share code and reproduce
    problems

  0. Build the thin environment

  ```
  > source dev_scripts/client_setup/build.sh
  ```

  1. Activate the thin environment

  ```
  > source dev_scripts/setenv_amp.sh
  ```

  2. Pull the latest cmamp image

  ```
  > i docker_pull
  or
  > docker pull sorrentum/cmamp:latest
  ```

  3. Merge the latest version of master into your branch

  ```
  > i git_merge_master
  ```

  To start a Docker container:

  ```
  > i docker_bash
  ```

  To start a Jupyter server:

  ```
  > i docker_jupyter
  ```
