# Code org of amp

```
> tree -d -n -I "*test*"
.
├── amp_research
├── core                            - helpers specific of data science and
│   │                                 finance projects
│   └── notebooks
├── dev_scripts                     - scripts used for development
│   ├── aws                         - scripts for managing AWS infra
│   ├── git_hooks
│   ├── infra                       - scripts to handle infrastructure
│   ├── install                     - scripts for installing environment
│   │   ├── conda_envs              - list of packages installed
│   │   └── requirements            - list of packages needed for each environment
│   ├── jenkins
│   ├── notebooks                   - scripts for managing jupyter notebooks
│   └── to_clean
├── docs
│   ├── notes
│   └── scripts
├── helpers                         - low-level helpers that are general and not
│                                     specific of any project
├── research                        - all general code / notebooks
├── rolling_model                   - TODO(gp): Merge into core / remove it
├── vendors                         - production-ready code / notebooks specific of a vendor
│   ├── kibot
│   │   └── data
    ...
└── vendors2                         - under-development code / notebooks specific of a vendor
```
