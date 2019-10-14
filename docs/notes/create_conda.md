# `create_conda` design notes

- `create_conda.py` is used to create a complete dev environment in a
  reproducible end-to-end way
  
- The problem of bootstrapping
    - `create_conda`
        - can only rely on standard Python libraries (otherwise it would depend on
          installing other packages)
        - use `amp` libraries
            - this is achieved by changing the running python path, before
             importing the `amp` libs from `helpers`
        
- Environment specification files are under
  `//amp/dev_scripts/install/requirements/`
  
- Jenkins runs a build to test a few `create_conda` environments

- It allows to select from different environments
    - E.g., `develop` is the official one
    - One can have special purpose environments (e.g., one with all
      experimental NLP libraries before they go in the main code)

- It allows to merge different environments (e.g., one from `//amp` and one
  from `p1`)
  
- It allows to save in the repo a list of all packages installed for future
  reference
  
# Using YAML files

- YAML files (instead of `.txt`) allow specifying also pip packages
- Refs:
    - [https://stackoverflow.com/questions/35245401]
    - [https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually]
    
- One can override the name of the package with:
    ```bash
    > conda env create -f dev_scripts/install/requirements/develop.yaml -n test
    ```
  
## Merging multiple YAML
- One can merge different YAML files automatically with multiple `-f` options

## Does pip install works?
- It does work, e.g.,
    ```yaml
    ...
    - pip
    - pip:
          # works for regular pip packages
          - docx
          - gooey
          # and for wheels
          - http://www.lfd.uci.edu/~gohlke/pythonlibs/bofhrmxk/opencv_python-3.1.0-cp35-none-win_amd64.whl
    ```
- In our case:
    ```yaml
    name: amp_develop
    dependencies:
      - python >= 3.6
      - networkx
      - pip
      - pip:
         #pip install ta
         - ta
    ```
      
## How to specify multiple conda channel?
- One can use the `channels` statement
    ```yaml
    name: amp_develop
    channels:
      - quantopian
    dependencies:
    ```

## Commented out packages
- We comment out some packages to remember we used them in the past and for some
  reason we are not using them anymore or because they are creating problems
    ```yaml
    #- ta                   # Technical analysis package.
    #- python-graphviz      # To plot pymc3 graphical models.
    ```

## Comments
- We had comments to track why we need packages and what they are
    ```yaml
    - arviz                 # Needed by pymc3 for some plotting functionality.
    - mkl-service           # pymc3 expects it; not sure why conda doesn't solve for it.
    - pandas-datareader=0.8.0     # PartTask344.
    ```
- We use some tags, e.g., `# Not on Mac.`, to do conditional builds, since
  `conda` doesn't support them out of the box
