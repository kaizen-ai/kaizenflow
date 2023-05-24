# GitHub/ZenHub workflows

<!--ts-->
   * [Introduction](#introduction)
   * [Concepts](#concepts)
      * [Epic](#epic)
         * [Master Epics](#master-epics)
         * [Sub-epics](#sub-epics)
      * [Issue](#issue)
      * [Label](#label)
      * [Pipeline](#pipeline)
      * [PR](#pr)
   * [Issue workflows](#issue-workflows)
      * [Filing a new issue](#filing-a-new-issue)
      * [Updating an issue](#updating-an-issue)
      * [Closing an issue](#closing-an-issue)
   * [PR workflows](#pr-workflows)
      * [PR labels](#pr-labels)
      * [Filing a new PR](#filing-a-new-pr)
         * [General tips](#general-tips)
         * [Filing process](#filing-process)
      * [Review](#review)
      * [Addressing comment](#addressing-comment)
      * [Coverage reports in PRs - discussion](#coverage-reports-in-prs---discussion)

<!--te-->

# Introduction

- In the following we use the abbreviations below:
  - GH = GitHub
  - ZH = ZenHub
  - PR = Pull Request
  - RP = Responsible party (Team Leader)
- Everything we work on comes as a GH task
  - We file tasks and then prioritize and distribute the workload
  - We try to always work on high priority (aka, P0) tasks
  - Issues vs bugs vs tasks
    - We call GH Issues "issues", and "tasks", (sometimes "tickets") interchangeably. We avoid to call them bugs since many times we use GH to track ideas, activities, and improvements, and not only defects in the code
    - The best names are "tasks" and "issues"
- [ZenHub](https://app.zenhub.com/workspaces/cm-615371012ed326001e044788/board?repos=586614470)
  for project management
  - We use ZH as project management layer on top of GH
  - Please install the [ZH extension](https://www.zenhub.com/extension) for GH. This is going to make your life easier

# Concepts

## Epic

- An Epic pulls together Issues that are somehow related by their topic
- We distinguish Master Epics (e.g., `WEB3`) and sub-Epics (e.g., `WEB3 - DaoCross v0.1`)
- See the current list of Epics on GH
  [here](https://github.com/sorrentum/sorrentum/issues?q=is%3Aopen+is%3Aissue+label%3AEpic+)
- We maintain all the information about what the Epic is about in its description

### Master Epics

- Master Epics are long-running Epics (i.e., projects)
  - E.g., `WEB3`
- Each issue should belong to at least one Epic: either a sub-epic or a master Epic
  - There is no need to add an issue to a Master Epic if it is already added to a sub-epic

### Sub-epics

- Master Epics can be broken down into smaller Epics (=sub-epics)
  - E.g., `WEB3 - DaoCross v0.1`
- Their titles should follow the pattern: `XYZ - ABC v*`, where:
  - `XYZ` - master Epic title
  - `ABC` - sub-epic title
  - `v*` - version
- Epics and sub-epics are typed as `EPIC - Sub-Epic`, i.e., the master epic is capitalized and the sub-epics are capitalized lower-case
- Sub-epics should have a short title and a smaller scope
- Some sub-epics are related to short term milestones or releases (e.g.,
  `WEB3 - DaoCross - v0.1`), other sub-epics are for long-running activities
  (e.g., `WEB3 - Good First Issue`)
- Sub-epics should belong to a Master Epic in ZH
  - A sub-epic can be moved to `Done/Pre-prod` only if all issues nested in it
    are moved to `Done/Pre-prod`

## Issue

- Issue is a piece of work to be done.
- Issues are combined into Epics by topic
- An issue has certain characteristics, i.e. labels
- An issue has a progress status, i.e. ZH pipeline (e.g.,
  `Product backlog (P1)`, `In progress`, `Done/Pre-prod`)
- PRs are linked to work needed to complete an issue

## Label

- Labels are attributes of an issue (or PR), e.g., `good first issue`,
  `PR_for_reviewers`, `duplicate`, etc.
- See the current list of labels and their descriptions
  [here](https://github.com/sorrentum/sorrentum/labels)

## Pipeline

- A ZH Pipeline represents the "progress" status of an Issue in our process
- We have the following Pipelines on the ZH board:
  - `New Issues`
    - Any new GH Issue with unclear Epic / Pipeline goes here
  - `Icebox (P2)`
    - Low priority, unprioritized issues
  - `Product Backlog (P1)`
    - Issues of medium priority at the moment
  - `Background`
    - Tasks one can do in background, e.g. reading, updating documentation, etc.
  - `Sprint backlog (P0)`
    - Issues of high priority at the moment
  - `In Progress`
    - Issues that we are currently working on
  - `Review/QA`
    - Issues opened for review and testing
    - Code is ready to be deployed pending feedback
  - `Done/Pre-prod`
    - Issues that are done and are waiting for closing
  - `Epics`
    - Both Master Epics and Sub-epics
  - `Closed`
    - Issues that are done and don't need a follow-up
    - GP/RPs are responsible for closing

## PR

- A pull request is an event where a contributor asks to review code they want
  to merge into a project

# Issue workflows

## Filing a new issue

- Use an informative description (typically an action "Do this and that")
  - We don't use a period at the end of the title
- If it is a “serious” problem (bug) put as much information about the Issue as
  possible, e.g.,:
  - What you are trying to achieve
  - Command line you ran, e.g.,
    ```
    > i lint -f defi/tulip/test/test_dao_cross_sol.py
    ```
  - **Copy-paste** the error and the stack trace from the cmd line, no screenshots, e.g.,
    ```
    Traceback (most recent call last):
      File "/venv/bin/invoke", line 8, in <module>
        sys.exit(program.run())
      File "/venv/lib/python3.8/site-packages/invoke/program.py", line 373, in run
        self.parse_collection()
      File "/venv/lib/python3.8/site-packages/invoke/program.py", line 465, in parse_collection
        self.load_collection()
      File "/venv/lib/python3.8/site-packages/invoke/program.py", line 696, in load_collection
        module, parent = loader.load(coll_name)
      File "/venv/lib/python3.8/site-packages/invoke/loader.py", line 76, in load
        module = imp.load_module(name, fd, path, desc)
      File "/usr/lib/python3.8/imp.py", line 234, in load_module
        return load_source(name, filename, file)
      File "/usr/lib/python3.8/imp.py", line 171, in load_source
        module = _load(spec)
      File "<frozen importlib._bootstrap>", line 702, in _load
      File "<frozen importlib._bootstrap>", line 671, in _load_unlocked
      File "<frozen importlib._bootstrap_external>", line 848, in exec_module
      File "<frozen importlib._bootstrap>", line 219, in _call_with_frames_removed
      File "/src/tasks.py", line 10, in <module>
        from helpers.lib_tasks import (  # This is not an invoke target.
      File "/app/amp/helpers/lib_tasks.py", line 14, in <module>
        from helpers.lib_tasks_docker import *  # isort:skip  # noqa: F401,F403 # pylint: disable=unused-import,unused-wildcard-import,wildcard-             import
      File "/app/amp/helpers/lib_tasks_docker.py", line 21, in <module>
        import helpers.henv as henv
      File "/app/amp/helpers/henv.py", line 16, in <module>
        import helpers.hversion as hversio
      File "/app/amp/helpers/hversion.py", line 23, in <module>
        import helpers.hserver as hserver
      File "/app/amp/helpers/hserver.py", line 212, in <module>
        _dassert_setup_consistency()
      File "/app/amp/helpers/hserver.py", line 203, in _dassert_setup_consistency
        raise ValueError(msg)
    ValueError: One and only one set-up config should be true:
    ```
  - The log of the run
    - Maybe the same run using `-v DEBUG` to get more info on the problem
  - What the problem is
  - Why the outcome is different from what you expected
- Use check boxes for "small" actions that need to be tracked in the issue (not
  worth their own bug)
  - An issue should be closed only after all the checkboxes have been addressed
- We use the `FYI @...` syntax to add "watchers"
  - E.g., `FYI @cryptomtc` so that he receives notifications for this issue
  - Authors and assignees receive all the emails in any case
  - In general everybody should be subscribed to receiving all the notifications
    and you can quickly go through them to know what's happening around you
- Assign an Issue to the right person for re-routing
  - There should be a single assignee to a bug so we know who needs to do the
    work
  - Assign GP/current RPs if not sure
- Assign an Issue to one of the pipelines, ideally based on the urgency
- If you are not sure, leave it unassigned but `@tag` GP / RPs to make sure we
  can take care of it
- Assign an Issue to the right Epic and Label
  - Use `Blocking` label when an issue needs to be handled immediately, i.e. it
    prevents you from making progress
  - If you are unsure then you can leave it empty, but `@tag` GP / RPs to make
    sure we can re-route and improve the Epics/Labels

## Updating an issue

- When you start working on an Issue, move it to the `In Progress` pipeline on
  ZH
  - Try to use `In Progress` only for Issues you are actively working on
  - A rule of thumb is that you should not have more than 2-3 `In Progress`
    Issues
  - Give priority to Issues that are close to being completed, rather than
    starting a new Issue
- Update an Issue on GH often, like at least once a day of work
  - Show the progress to the team with quick updates
  - Update your Issue with pointers to gdocs, PRs, notebooks
  - If you have questions, post them on the bug and tag people
- Once the task, in your opinion, is done, move an issue to `Review/QA` pipeline
  so that GP/RPs can review it
- If we decide to stop the work, add a `Paused` label and move it back to the
  backlog, e.g., `Sprint backlog (P0)`, `Product backlog (P1)`, `Icebox (P2)`

## Closing an issue

- A task is closed when PR has been reviewed and merged into `master`
- When, in your opinion, there is no more work to be done on your side on an
  Issue, please move it to the `Done/Pre-prod` or `Review/QA` pipeline, but do
  not close it
  - GP/RPs will close it after review
- If you made specific assumptions, or if there are loose ends, etc., add a
  `TODO(user) `or file a follow-up bug
- Done means that something is DONE, not 99% done
  - It means that the code is tested, readable and usable by other teammates
- Together we can decide that 99% done is good enough, but it should be a
  conscious decision and not come as a surprise

# PR workflows

## PR labels

- `PR_for_authors`
  - There are changes to be addressed by an author of a PR
- `PR_for_reviewers`
  - PR is ready for review by RPs
- `PR_for_integrators`
  - PR is ready for the final round of review by GP, i.e. close to merge

## Filing a new PR

### General tips

- Implement a feature in a branch (not master), once it is ready for review push
  it and file a PR via GH interface
- We have invoke tasks to automate some of these tasks:
  ```
  > i git_create_branch -i 828
  > i git_create_branch -b Cmamp723_hello_world
  > i gh_create_pr
  ```
- If you want to make sure you are going in a right direction or just to confirm
  the interfaces you can also file a PR to discuss
- Mark PR as draft if it is not ready, use the `convert to draft` button

### Filing process

- Add a description to help reviewers to understand what it is about and what
  you want the focus to be
  - Add a pointer in the description to the issue that PR is related to - this
    will ease the GH navigation for you and reviewers
- Leave the assignee field empty
  - This will be done by RPs
- Add reviewers to the reviewers list
  - For optional review just do `@FYI` `person_name` in the description
- Add a corresponding label
  - Usually the first label in the filed PR is `PR_for_reviewers`
  - If it is urgent/blocking, use the `Blocking` label
- Make sure that the corresponding tests pass
- Always lint before asking for a review
- Link a PR to an issue via ZH plugin feature
  - `# TODO(Dan): Add a pic with example.`
- If the output is a notebook:
  - Publish a notebook, see
    [here](https://docs.google.com/document/d/1b3RptKVK6vFUc8upcz3n0nTZhTO0ZQ-Ay5I01nCp5WM/edit#heading=h.oi342wm38z0a)
  - Attach a cmd line to open a published notebook, see
    [here](https://docs.google.com/document/d/1b3RptKVK6vFUc8upcz3n0nTZhTO0ZQ-Ay5I01nCp5WM/edit#heading=h.i7m2jg6llfl2)

## Review

- A reviewer should check the code:
  - Architecture
  - Conformity with specs
  - Code style conventions
  - Interfaces
  - Mistakes
  - Readability
- There are 2 possible outcomes of a review:
  - There are changes to be addressed by author
    - A reviewer leaves comments to the code
    - Marks PR as `PR_for_authors`
  - A PR is ready to be merged:
    - Pass it to integrators and mark it as `PR_for_integrators`
      - Usually is placed by RPs after they approve PR

## Addressing comment

- If the reviewer's comment is clear to the author and agreed upon:
  - The author addresses the comment with a code change and after changing the
    code (everywhere the comment it applies) marks it as `RESOLVED` on the GH
    interface
  - Here we trust the authors to do a good job and to not skip / lose comments
  - If the comment needs further discussion, the author adds a note explaining
    why he/she disagrees and the discussion continues until consensus is reached
- Once all comments are addressed:
  - Re-request the review
  - Mark it as `PR_for_reviewers`

## Coverage reports in PRs - discussion

We should start posting coverage reports in PRs.

The suggested process is:

- PR’s author posts coverage stats before (from master) and after the changes in
  the format below. The report should contain only the files that were touched
  in a PR.
  - We have `run_coverage_report` invoke
    - `TODO(*): Enable for Sorrentum and add usage examples.`
  - Maybe we can automate it somehow, e.g., with GH actions. But we need to
    start from something.
    ```
    Name                                    Stmts   Miss Branch BrPart  Cover
    -------------------------------------------------------------------------
    oms/locates.py                              7      7      2      0     0%
    oms/oms_utils.py                           34     34      6      0     0%
    oms/tasks.py                                3      3      0      0     0%
    oms/oms_lib_tasks.py                       64     39      2      0    38%
    oms/order.py                              101     30     22      0    64%
    oms/test/oms_db_helper.py                  29     11      2      0    65%
    oms/api.py                                154     47     36      2    70%
    oms/broker.py                             200     31     50      9    81%
    oms/pnl_simulator.py                      326     42     68      8    83%
    oms/place_orders.py                       121      8     18      6    90%
    oms/portfolio.py                          309     21     22      0    92%
    oms/oms_db.py                              47      0     10      3    95%
    oms/broker_example.py                      23      0      4      1    96%
    oms/mr_market.py                           55      1     10      1    97%
    oms/__init__.py                             0      0      0      0   100%
    oms/call_optimizer.py                      31      0      0      0   100%
    oms/devops/__init__.py                      0      0      0      0   100%
    oms/devops/docker_scripts/__init__.py       0      0      0      0   100%
    oms/order_example.py                       26      0      0      0   100%
    oms/portfolio_example.py                   32      0      0      0   100%
    -------------------------------------------------------------------------
    TOTAL                                    1562    274    252     30    80%
    ```
- PR’s author also sends a link to S3 with the full html report so that a
  reviewer can check that the new lines added are covered by the tests
