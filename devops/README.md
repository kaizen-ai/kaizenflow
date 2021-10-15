- See changelog in `version.txt`

# Code organization
- The organization of a `devops` dir is like in the following
  - `compose`
    - Docker compose files
  - `docker_build`
    - Everything related to building a Docker image
  - `docker_scripts`
    - TODO(gp): -> docker_run
    - Everything related to running a Docker image
  - `env`
    - Docker env files

- An example is below:
  ```
  > tree devops/
  devops/
  ├── README.md
  ├── compose
  │   ├── docker-compose.yml
  │   └── docker-compose_as_submodule.yml
  ├── debug
  │   └── repo_compare.sh
  ├── docker_build
  │   ├── dev.Dockerfile
  │   ├── fstab
  │   ├── install_dind.sh
  │   ├── install_jupyter_extensions.sh
  │   ├── install_os_packages.sh
  │   ├── install_python_packages.sh
  │   ├── old
  │   │   └── conda.yml
  │   ├── poetry.lock
  │   ├── poetry.toml
  │   └── pyproject.toml
  ├── docker_run
  │   ├── aws_credentials.sh
  │   ├── bashrc
  │   ├── entrypoint.sh
  │   ├── run_jupyter_server.sh
  │   ├── setenv.sh
  │   └── test_setup.sh
  ├── env
  │   └── default.env
  └── makefiles
      ├── development.mk
      ├── general.mk
      └── repo_specific.mk  
 ```
