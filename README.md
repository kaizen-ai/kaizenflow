<!--ts-->




<!--te-->

<img width="100" alt="image" src="https://user-images.githubusercontent.com/33238329/216777823-851b28ed-7d7a-4b52-9d71-ab38d146edc3.png">

Sorrentum is an open-Source DeFi protocol to build advanced financial applications

- [Website](https://www.sorrentum.org)
- [Welcome page](https://github.com/sorrentum/sorrentum/wiki/Welcome-to-the-Sorrentum-Project)

## Onboarding process
- Create an issue in the repo https://github.com/sorrentum/sorrentum with the
  following checklist
  ```
  [ ] Subscribe to Research Office hours and Telegram channels
  TODO(gp): @all add link
  [ ] Read Sorrentum [Python coding style guide](https://docs.google.com/document/d/1R6jhFDbZKvyDdbnSQ9DA_N8417YF13hMI_Uw4quO4Xk)
  [ ] Learn how to do contribute code via Pull Requests
  ```
  and go through the list one-by-one marking as done when it's actually done

## Coding style
- Read Sorrentum [Python coding style guide](https://docs.google.com/document/d/1R6jhFDbZKvyDdbnSQ9DA_N8417YF13hMI_Uw4quO4Xk)

## Technologies
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
