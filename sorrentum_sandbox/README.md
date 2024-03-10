# Sorrentum Sandbox

- This dir `sorrentum_sandbox` contains examples for Sorrentum data nodes
  - It allows to experiment with and prototype Sorrentum nodes, which are
    comprised of multiple services (e.g., Airflow, MongoDB, Postgres)
  - All the code can be run on a local machine with `Docker`, without the need of
    any cloud production infrastructure

- The current structure of the `sorrentum_sandbox` directory is as follows:
  ```markdown
  > $GIT_ROOT/dev_scripts/tree.sh -p sorrentum_sandbox

  sorrentum_sandbox/
  |-- common/
  |   |-- __init__.py
  |   |-- client.py
  |   |-- download.py
  |   |-- save.py
  |   `-- validate.py
  |-- devops/
  |   |-- airflow_data/
  |   |   |-- dags/
  |   |   |   |-- __init__.py
  |   |   |   |-- airflow_tutorial.py
  |   |   |   |-- download_forecast_bitcoin_prices.py
  ...
  |   |   `-- __init__.py
  |   |-- Dockerfile
  |   |-- __init__.py
  |   |-- docker-compose.yml
  |   |-- docker_bash.sh*
  |   |-- docker_build.sh*
  |   |-- docker_clean.sh
  |   |-- docker_cmd.sh*
  |   |-- docker_exec.sh*
  |   |-- docker_kill.sh
  |   |-- docker_name.sh
  |   |-- docker_prune.sh*
  |   |-- docker_prune_all.sh*
  |   |-- docker_pull.sh*
  |   |-- docker_push.sh*
  |   |-- init_airflow_setup.sh*
  |   |-- reset_airflow_setup.sh*
  |   `-- setenv.sh
  |-- docker_common/
  |   |-- README.md
  |   |-- bashrc
  |   |-- create_links.sh*
  |   |-- etc_sudoers
  |   |-- install_jupyter_extensions.sh*
  |   |-- repo_diff.sh*
  |   |-- update.sh*
  |   |-- utils.sh
  |   `-- version.sh*
  |-- examples/
  |   |-- systems/
  |   |   |-- binance/
  |   |   |   |-- test/
  |   |   |   |   `-- test_download_to_csv.py
  |   |   |   |-- __init__.py
  |   |   |   |-- db.py
  |   |   |   |-- download.py
  |   |   |   |-- download_to_csv.py*
  |   |   |   |-- download_to_db.py*
  |   |   |   |-- load_and_validate.py*
  |   |   |   |-- load_validate_transform.py*
  |   |   |   `-- validate.py
  |   |   |-- reddit/
  |   |   |   |-- __init__.py
  |   |   |   |-- db.py
  |   |   |   |-- download.py
  |   |   |   |-- download_to_db.py*
  |   |   |   |-- load_validate_transform.py*
  |   |   |   |-- transform.py
  |   |   |   `-- validate.py
  |   |   `-- __init__.py
  |   |-- Dockerfile
  |   |-- __init__.py
  |   |-- bashrc
  |   |-- docker_bash.sh*
  |   |-- docker_build.sh*
  |   |-- docker_build.version.log
  |   |-- docker_clean.sh*
  |   |-- docker_exec.sh*
  |   |-- docker_jupyter.sh*
  |   |-- docker_push.sh*
  |   |-- etc_sudoers
  |   |-- install_jupyter_extensions.sh*
  |   |-- run_jupyter.sh*
  |   |-- set_env.sh
  |   |-- tutorial_jupyter.ipynb
  |   `-- version.sh*
  |-- projects/
  |   |-- research/
  ...
  |   |-- spring2023/
  |   |   |-- altdata_notebooks/
  ...
  |   |   `-- ml_projects/
  ...
  |   `-- spring2024/
  |       |-- SorrTask645_Redis_cache_to_fetch_user_profiles/
  |       |   |-- docker/
  |       |   |   |-- Dockerfile
  |       |   |   |-- Redis_cache_to_fetch_user_profiles.ipynb
  |       |   |   `-- docker-compose.yaml
  |       |   `-- README.md
  |       |-- project_template/
  |       |   `-- README.md
  |       `-- README.md
  |-- README.md
  `-- __init__.py

  70 directories, 342 files
  ```

- Focusing on the directory structure:
  ```
  > tree --dirsfirst -d -n -F --charset unicode sorrentum_sandbox
  sorrentum_sandbox
  |-- common
  |-- devops
  |   `-- airflow_data
  |       `-- dags
  |-- docker_common
  |-- examples
  |   `-- systems
  |       |-- binance
  |       |   `-- test
  |       `-- reddit
  `-- projects
      |-- research
      |-- spring2023
      `-- spring2024
          |-- SorrTask645_Redis_cache_to_fetch_user_profiles
          |   `-- docker
          `-- project_template
  ```

- `common/`: contains abstract system interfaces for the different blocks of the 
   Sorrentum ETL pipeline
  - Read the code top to bottom to get familiar with the interfaces
    ```
    > vi $GIT_ROOT/sorrentum_sandbox/common/*.py
    ```
- `devops/`: contains the Dockerized Sorrentum data node
  - it contains the Airflow task scheduler and its DAGs
  - it can run any Sorrentum data nodes, like the ones in `examples/systems`
- `docker_common/`: common code for Docker tasks
- `examples/`:
  - `systems`: contains several examples of end-to-end Sorrentum data nodes
    - E.g., downloading price data from Binance and posts/comments from Reddit
    - Each system implements the interfaces in `common`
- `projects`: code for various projects
  - `research`: Sorrentum research projects
  - `spring2023`: class projects for Spring 2023 (team projects about building
    Sorrentum systems)
  - `spring2024`: class projects for Spring 2024 DATA605 class (individual
    projects about building examples of big data technologies)
