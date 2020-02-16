<!--ts-->
      * [Conda bloat](#conda-bloat)
      * [Minimize conda bloat](#minimize-conda-bloat)
      * [Experimental conda environment](#experimental-conda-environment)
      * [Releasing a new conda environment](#releasing-a-new-conda-environment)



<!--te-->

# Conda bloat

- "Conda bloat" refers to the situation when there are more packages in the
  conda recipe than what strictly needed to allow us to make progress.

- The effects of conda bloat are:
  - Longer time for `conda` to solve the dependencies, download the packages,
    and update the environment
  - Additional dependencies forcing `conda` to install older packages that can
    create subtle issues in our environment (we want our code always to be
    forward compatible, but we can't afford our code to be back compatible)
  - People starting pinning down packages to get deterministic library behaviors
  - In the worst case, not being able to install the environment at all due to
    conflicting `conda` requirements

- On the one side, we want to minimize "conda bloat".
- On the other side, we want to be able to experiment with packages.

## Minimize conda bloat

- To minimize conda bloat, our process consists of adding a package to the conda
  recipe when a new package is actually needed by code and to run unit tests
  - Having code using the library and running unit tests for that code should
    happen together
  - The rule is "Important code should be tested, and if code is not important
    it should not be in the repo at all"
  - Thus a corollary is that all code in the repo should be tested

# Conda environment lifecycle

## Experimental conda environment

- On the other side we want to be free to experiment with a package that can
  save us tons of development time.

- The proposed approach is:
  - Install the package in whatever way you want on top of your standard conda
    environment (`conda`, `pip`, whatever)
  - Be aware that some packages are not easy to add to our production system
    (e.g. packages written by students trying to graduate, non-coding savvy PhDs
    and professors)
  - Create a new local experimental conda environment with:
    `create_conda.py --env_name p1_develop.nlp`
  - If you want to reproduce your environment or share it (e.g., among the NLP
    team) you can branch `dev_scripts/install/requirements/p1_develop.yaml` and
    modify it, then you can create an environment programmatically with:
    `create_conda.py --env_name p1_develop.nlp --req_file dev_scripts/install/requirements/p1_develop.nlp.yaml`

- We can make this process more automated by generalizing the scripts we already
  have.

## Releasing a new conda environment

- Once the new package is added to the official conda environment, we should:
  - Test the new conda environment locally, by creating a fresh environment and
    running all the tests
  - Compare the list of packages installed before / after the change:
    `git diff amp/dev_scripts/install/conda_envs/amp_develop.saggese.Darwin.gpmac.lan.txt`
    - E.g., Check whether installed pandas went from 0.25 to 0.14.1
  - Use the `dev` build to make sure Jenkins is happy with it, especially when
    we develop on a different OS than Linux
  - Send an email to the team asking them to recreate the environment

- Typically GP takes care of getting all this fun stuff to work, but you are
  welcome to try locally to minimize surprises.

# Conda maintenance (only for admins)

## Updating conda itself

- To update conda itself you can run:
```bash
> conda activate base

> conda --version
3.7.1

> conda update anaconda

> conda --version
3.8.0
```

## Cleaning conda packages
- One can clean up the entire cache of packages with:
```bash
> conda clean --yes --all
```

- This operation:
  - affects the conda system for all the users
  - is just about deleting cached artifacts so it should not have destructive
    effects
