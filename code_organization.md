# Code org of amp

```
> tree -d -n -I "*test*"
.
├── aws
  - scripts for managing AWS infra
├── core
  - helpers that are specific of data science and finance projects
├── dev_scripts
  - scripts used for development
├── features
  - code for features
├── helpers
  - low-level helpers that are general and not specific of any project
├── infra
    - scripts to handle infrastructure
├── install
    - scripts for installing the data
│   ├── conda_envs
         - list of packages installed
│   └── requirements
        - list of packages needed for each environment
├── ipynb_scripts
    - scripts for managing jupyter notebooks
    - TODO(gp): move to dev_scripts
├── rolling_model
  - TODO(gp): move into core
└── to_clean
```
