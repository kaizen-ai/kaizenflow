# GitHub Actions workflows Reference

<!-- toc -->

- [All-Repo Workflows](#all-repo-workflows)

<!-- tocstop -->

## All-Repo Workflows

| workflow/repo                           | cmamp | dev_tools | orange | lemonade |
| --------------------------------------- | ----- | --------- | ------ | -------- |
| Fast tests                              | x     | x         | x      | x        |
| Slow tests                              | x     | x         | x      | x        |
| Superslow tests                         | x     | x         | x      | x        |
| Check if the linter was run             | x     |           |        |          |
| Allure fast tests                       | x     |           |        |          |
| Allure slow tests                       | x     |           |        |          |
| Allure superslow tests                  | x     |           |        |          |
| Build dev image                         | DW    |           |        |          |
| Build production cmamp image            | DW    |           |        |          |
| Test coverage                           | x     |           |        |          |
| Import cycles detector                  | x     |           |        |          |
| Release new ECS preprod task definition | x     |           | x      |          |
| Release new ECS prod task definition    | x     |           | x      |          |
| Update amp submodule                    | NA    | x         | x      | x        |

- `x`: Enabled Workflow
- `DW`: Disabled Workflow
- `NA`: Not Applicable
