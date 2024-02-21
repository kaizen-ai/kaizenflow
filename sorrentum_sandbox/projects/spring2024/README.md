# DATA605 Class Projects

- Each student picks one of the projects based on the complexity 1-3
  - Each student pick the complexity so that they kind self-select based on
    complexity
- One for the midterm and one for the final
- There is a sign-up sheet on Excel
  [here](https://docs.google.com/spreadsheets/d/1nwjIvXgEaxH_M21k8hYebVbFPLWh7UNglLUlh11psAs/edit#gid=0)
- We prefer students not to pick the same project

## Project assignment

- Each project requires the following steps
  - Create a Docker container installing the needed tools (e.g., Redis and `redis-py`)
  - You should use Docker Compose to build single or multi-container systems
  - Jupyter notebook (if possible), otherwise a Python script implementing the project
  - Only Python3 on Linux is allowed

### Documentation
- Write a 5 to 10-page report in markdown covering
  - At least 1 page (60 lines): short description of the technology used (e.g.,
    Redis)
  - At least 1 page (60 lines): describe and explain the logic of the Docker
    system that you have built
  - At least 1 page (60 lines): explain how to run the system by starting the
    container system
  - At least 3 pages (60 lines): describe the script/notebook with examples of
    the output, using diagrams (e.g., use `mermaid`), describe the schema used in
    the DB, etc

The script/notebook should be able to run end-to-end without errors, otherwise
the project is not considered working

- Each project will need to be checked in https://github.com/sorrentum/sorrentum
  filing bugs, with PR, etc like in an open source project

- The layout of each project is the following
  sorrentum_sandbox/projects/spring2024/SorrTaskXYZ_...
    README.md
    docker

- The tag of your projects follows the schema
  `Spring{year}_{project_title_without_spaces}`
- E.g., if the project title is "Redis cache to fetch user profiles", the name of
  `Spring2024_Redis_cache_to_fetch_user_profiles`

- Create a GitHub issue with the project tag (e.g.,
  `Spring2024_Redis_cache_to_fetch_user_profiles`) and assign the issue to
  yourself
  - We will use this issue to communicate

- Create a branch in Git named after your project (e.g.,
  `SorrTask645_Redis_cache_to_fetch_user_profiles`)
- You should add files only under the directory corresponding to your project
  `{GIT_ROOT}/sorrentum_sandbox/projects/{project_tag}`, e.g.,
  `{GIT_ROOT}/sorrentum_sandbox/projects/Spring2024_Redis_cache_to_fetch_user_profiles`
  - E.g., on the dir cloned on my laptop the dir is named
    `~/src/sorrentum1/sorrentum_sandbox/projects/SorrTask645_Redis_cache_to_fetch_user_profiles`

- Copy the files from 
