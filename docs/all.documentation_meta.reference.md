# Documentation Meta

<!-- toc -->

- [How to organize the docs](#how-to-organize-the-docs)
  * [Dir vs no-dirs](#dir-vs-no-dirs)
  * [Tracking reviews and improvements](#tracking-reviews-and-improvements)
  * [How to search the documentation](#how-to-search-the-documentation)
  * [Ensure that all the docs are cross-referenced in the indices](#ensure-that-all-the-docs-are-cross-referenced-in-the-indices)
- [List of files](#list-of-files)
  * [Description](#description)

<!-- tocstop -->

## How to organize the docs

- Documentation can be organized in multiple ways:
  - By software component
  - By functionality (e.g., infra, backtesting)
  - By team (e.g., trading ops)

- We have decided that
  - For each software component there should be a corresponding documentation
  - We have documentation for each functionality and team

- Processes
  - `onboarding`
  - `general_background`
  - `work_organization`
  - `work_tools`
  - `coding`
  - ...

- Software components
  - `build`
  - `kaizenflow`
  - `datapull`
  - `dataflow`
  - `trade_execution`
  - `infra`
  - ...

### Dir vs no-dirs

- Directories make it difficult to navigate the docs
- We use “name spaces” until we have enough objects to create a dir

### Tracking reviews and improvements

- Doc needs to be reviewed "actively", e.g., by making sure someone checks them
  in the field
- Somebody should verify that is "executable"

- There is a
  [Master Documentation Gdoc](https://docs.google.com/document/d/1sEG5vGkaNIuMEkCHgkpENTUYxDgw1kZXb92vCw53hO4)
  that contains a list of tasks related to documentation, including what needs
  to be reviewed

- For small action items we add a markdown TODO like we do for the code
  ```
  <!-- TODO(gp): ... -->
  ```

- To track the last revision we use a tag at the end of the document like:
  ```markdown
  Last review: GP on 2024-04-20, ...
  ```

### How to search the documentation

- Be patient and assume that the documentation is there, but you can't find it
  because you are not familiar with it and not because you think the
  documentation is poorly done or not organized

- Look for files that contain words related to what you are looking for
  - E.g., `ffind.py XYZ`
- Grep in the documentation looking for words related to what you are looking
  for
  - E.g., `jackmd trading`
- Scan through the content of the references
  - E.g., `all.code_organization.reference.md`
- Grep for the name of a tool in the documentation

### Ensure that all the docs are cross-referenced in the indices

- There is a script to check and update the documentation cross-referencing
  files in a directory and a file with all the links to the files
  ```
  /Users/saggese/src/dev_tools1/linters/amp_fix_md_links.py
  docs/all.amp_fix_md_links.explanation.md
  ```

## List of files

- The current structure of files is given by:

  ```bash
  > tree docs -I '*figs*|test*' --dirsfirst -n -F --charset unicode | grep -v __init__.py
  ```

- The simple list is:
  ```bash
  > ls -1 docs
  all.code_organization.reference.md
  all.documentation_meta.reference.md
  all.software_components.reference.md
  all.workflow.explanation.md
  build
  ck.components.reference.md
  coding
  dash_web_apps
  dataflow
  datapull
  deploying
  dev_tools
  documentation_meta
  general_background
  infra
  kaizenflow
  marketing
  monitoring
  oms
  onboarding
  trading_ops
  work_organization
  work_tools
  ```

### Description

- Please keep the directory in alphabetical order

- `all.documentation_meta.reference.md`: contains rules and conventions for all
  the documentation under `docs`
- `all.code_organization.reference.md`: describes how the code is organized in
  terms of components, libraries, and directories
- `all.software_components.reference.md`: lists all the software components in
  the codebase
- `all.workflow.explanation.md`: describes all the workflows for quants, quant
  devs, and devops
- `build`: information related to the build system and GitHub actions
- `ck.components.reference.md`: list software components and maintainers
- `coding`
  - Guidelines and good practices for coding and code-adjacent activities (such
    as code review)
  - This includes general tips and tricks that are useful for anybody writing
    any code (e.g., how to use type hints) as well as in-depth descriptions of
    specific functions and libraries
- `dash_web_apps`
- `dataflow`: docs related to the framework of implementing and running machine
  learning models
- `datapull`: docs related to dataset handling: downloading, onboarding,
  interpretation, etc.
- `deploying`
- `dev_tools`
- `documentation_meta`: how to write documentation for code and workflows
- `general_background`: documents that provide general reference information,
  often across different topics
  - E.g., glossaries, reading lists
- `infra`: docs related to the company’s infrastructure
  - E.g., AWS services, code deployment, monitoring, server administration, etc.
- `kaizenflow`: docs related to high-level packages that are used across the
  codebase , as well as overall codebase organization.
  - E.g., `helpers`, `config`
- `marketing`
- `monitoring`
- `oms`
- `onboarding`: practicalities of on-boarding new team members
  - E.g., things typically done only once at the beginning of joining the team
- `trading_ops`: docs related to placing and monitoring trading orders to market
  or broker
- `work_organization`: how the work is organized on a general level
  - E.g., the company's adopted practices spanning coding and development
- `work_tools`: how to set up, run and use various software needed for
  development
  - E.g., IDE

Last review: GP on 2024-08-11
