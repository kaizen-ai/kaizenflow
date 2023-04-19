<!--ts-->




<!--te-->

<img width="100" alt="image" src="https://user-images.githubusercontent.com/33238329/216777823-851b28ed-7d7a-4b52-9d71-ab38d146edc3.png">

Sorrentum is an open-Source DeFi protocol to build advanced financial applications

- [Website](https://www.sorrentum.org)
- [Welcome page](https://github.com/sorrentum/sorrentum/wiki/Welcome-to-the-Sorrentum-Project)
- Some info on the procedures and organizations of the project:
  https://github.com/sorrentum/sorrentum/wiki/Organization-and-procedures

## On-boarding process
- Create an issue in the repo https://github.com/sorrentum/sorrentum with the
  following checklist
  ```
  [ ] Subscribe to the Research Office hours calendar
  [ ] Subscribe to Telegram channels (see below)
  [ ] Read Sorrentum Python coding style guide (see below)
  [ ] Learn how to do contribute code via GitHub Pull Requests (see below)
  [ ] Get assigned a warm-up issue (see below)
  ```
  and go through the list one-by-one marking each item as done when it's actually
  done

## Python coding style guide
- Read Sorrentum [Python coding style guide](https://docs.google.com/document/d/1R6jhFDbZKvyDdbnSQ9DA_N8417YF13hMI_Uw4quO4Xk)

## Subscribing to the Telegram channels
- Sorrentum - All: https://t.me/+a9XMN1ixUjwwNjBh
- Sorrentum - Model Research: https://t.me/+ClGbWSHyVIU3MmVh
- Sorrentum - NLP Research: https://t.me/+i9K52TnNnBFhNGQx
- Sorrentum - Web3 Research: https://t.me/+VPfKSu526MA1ZGNh
- Sorrentum - Arbitrage Research: https://t.me/+fidFMZ9fYts5ODQ5
- Sorrentum - Growth hacking: https://t.me/+Ey0tteEb8iNmNmVh

## Research office hours
- Meeting room: https://umd.zoom.us/j/7447082187
  - Ask for the password
- The latest calendar is posted at
  - TODO(gp): Add link

## Technologies used
- [UMD DATA605 Big Data Systems](https://github.com/gpsaggese/umd_data605)
  contains
   [lectures](https://github.com/gpsaggese/umd_data605/tree/main/lectures) and
   [tutorials](https://github.com/gpsaggese/umd_data605/tree/main/tutorials)
   about most of the technologies we use in Sorrentum, e.g.,
     - Dask
     - Docker
     - Docker Compose
     - Git, github
     - Jupyter
     - MongoDB
     - Pandas
     - Postgres
     - Apache Spark
- You can go through the lectures and tutorials on a per-need basis, depending on
  what it's useful for you to develop

## How to contribute code (short version)
- The proper way to contribute to the Sorrentum project is as follows:
  - Create a branch of your assigned issues/bugs
    - E.g., for a GitHub issue with the name "Expose the linter container to
      Sorrentum contributors #63", the branch name should be
      `SorrTask63_Expose_the_linter_container_to_Sorrentum_contributors`
    - This step is automated through the invoke flow
  - Push the code to your branch
  - Make sure you are following our coding practices (see above)
  - Make sure your branch is up-to-date with the master branch
  - Create a Pull Request (PR) from your branch
  - Add your assigned reviewers for your PR so that they are informed of your PR
  - After being reviewed, it will be merged to the master branch by your reviewers

## Sorrentum Docker container

- We work in a Docker container that has all the required dependencies installed
  - You can use PyCharm / VS code on your laptop to edit code, but you want to
    run code inside the container since this makes sure everyone is running with
    the same system and it makes it easy to share code and reproduce problems

  0) Build the thin environment
  ```
  > source dev_scripts/client_setup/build.sh
  ```

  1) Activate the thin environment
  ```
  > source dev_scripts/setenv_amp.sh
  ```

  2) Pull the latest cmamp image
  ```
  > i docker_pull
  or
  > docker pull sorrentum/cmamp:latest
  ```

  3) Merge the latest version of master into your branch

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

## Getting a first issue to work on (aka warm-up issue)
- The goal is to get comfortable with the development system

- Mark certain bugs as “good-as-first-bug”
- Write unit tests
- Copy-paste-modify
- Simple refactoring

## Web3 / DeFi resources
- The code for Web3 / DeFi projects is in
  https://github.com/sorrentum/sorrentum/tree/master/defi
- You can follow the [readme](https://github.com/sorrentum/sorrentum/tree/master/defi/README.md)
