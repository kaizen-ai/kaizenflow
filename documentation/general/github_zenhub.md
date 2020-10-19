<!--ts-->

* [ZenHub](#zenhub)
* [Agile concepts](#agile-concepts)
* [Mapping Agile concept onto GH](#mapping-agile-concept-onto-gh)
* [ZenHub](#zenhub-1)
* [Pipelines](#pipelines)
* [How to transition from our project management to ZH](#how-to-transition-from-our-project-management-to-zh)
* [Our conventions](#our-conventions)
* [Sprint](#sprint)
* [Sprint Backlog](#sprint-backlog)
* [Workflow](#workflow)
* [Sprint Planning workflow](#sprint-planning-workflow)
* [Moving Tasks Between Pipelines Workflow](#moving-tasks-between-pipelines-workflow)
* [Epics](#epics)
* [Story Points aka Task Estimation](#story-points-aka-task-estimation)
* [Labels](#labels)
* [Sprint Meetings](#sprint-meetings)
  <!--te-->

# Task management with GitHub

## GH stands for GitHub

## Everything we work on comes as a GitHub task

- We file tasks and then prioritize and distribute the workload
- We try to work always on P0 tasks

## Issues vs bugs vs tasks

- We call GitHub issues interchangeably "issues", "bugs", and "tasks"
- "Bugs" is a bit improper since many times we use GitHub to track ideas,
  activities, and improvements, and not only defect in the code
- The best names are "tasks" and "issues"

## Life cycle of a bug / task

- When you start working on a bug, mark it as in `PROGRESS`
- Make sure the label, the description, and the assignees are up to date
- Try to have `IN_PROGRESS` only for the bugs on which you are actually doing
  work
- A rule of thumb is that you should not have more than 2-3 `IN_PROGRESS` bugs
- Give priority to bugs that are close to be completed, rather than starting a
  new bug

- Update a bug often, like at least once a day of work
  - Show the progress with quick updates
  - Update your bug with pointers to gdocs, PRs, notebooks
  - If you have questions, post them on the bug and

- When for a bug, in your opinion, there is no more work to be done on your side
  (besides somebody to review it) please mark it as `TO_CLOSE` or `TO_REVIEW`
  label and remove `IN_PROGRESS` labels
- If we decide to stop the work, replace `IN_PROGRESS` with `PAUSED` label
- If there is something that needs to be done, please update the bug summarizing
  status and next actions
- If the code needs to be reviewed then file a PR, add the link to the bug and
  mark it as `TO_REVIEW`

- We leave bugs in the `TO_CLOSE` state when we need to so some other work after
  it, and we don't want to forget about this by closing it
- The rule is that only who filed the bug should close the bug, after verifying
  that all work has done up to our standards

## Done means "DONE"

- A task is closed when the pull request has been reviewed and merged into
  `master`
- If you made specific assumptions, if there are loose ends and so forth, add a
  `TODO(user)`
- Done means that something is DONE, not 99% done
  - It means that the code is tested, readable and usable by other teammates
- Together we can decide that 99% done is good enough, but it should be a
  conscious decision and not a come as a surprise

## Tend your tasks

- Periodically (ideally every single day) go over your tasks and update them,
  e.g.,
  - Add the branch you are working on, when you start
  - Add info on what issues you are facing
  - Point to gdocs
  - Add a pull request link if it's in review
  - Add relevant commits that implemented the work

## File descriptive GH tasks

- For any "serious" problem, file a task describing the problem and, ideally,
  giving a "repro case" (i.e., a clear description of how to reproduce the
  problem)
  - Put as many information about the data as possible, e.g.,
    - The command line you ran
    - The log of the run, maybe the same run using `-v DEBUG` to get more info
    - What is the problem and why it is different from what you expected
  - Try to make life easy for the person who is going to have to fix the bug

## Do not change the assignee for a task

- If you need someone to do something just @mention it

- The rationale is that we want one person to be responsible for the task from
  beginning to end

# Code review with GitHub

- TODO(gp): Move it code_review.md

## Avoid committing to `master`

- Exceptions are small commits that are not part of a feature

## Post-commit comments

- We follow commits to `master` and may ask for fixes post-commit
- Do not miss those emails, in spite of the large number of emails we get
- Solutions:
  - Improve your email workflow, e.g., by using "flags" in Gmail web interface
    and / or an email client. Do not rely on "unread" emails as a reminder of
    what to do. It's easy to mark an email as read by mistake
  - Try to do the fix as soon as possible, so you don't forget
  - If you disagree with the proposed change say so and do not linger the
    comment in the oblivion

## Addressing post-commit comments

- Once the commit addressing the reviewer's comments is ready:
  - Add a comment to the commit like "Do-this-and-that as per reviewers
    request", so that the reviewer can see that his/her comments were addressed
  - Reply to the GitHub email with the comment with "DONE" to notify the
    reviewer that the comment was addressed

## Use branches and PR

- All code should be reviewed before it is merged into `master`

## Optional PR review

- Sometimes we want to do an "optional" review in GitHub

- The process is:
  - Create a PR
  - Tag the reviewers, adding a description if needed, like in the normal PR
    flow
  - Merge the PR without waiting for the review

- Unfortunately merging the PR automatically closes the PR

- The problem is that once the reviewers get to that PR and adds comments,
  emails are sent, but GitHub doesn't track the PR as open
  - The comments are there but not "resolved"
  - One needs to go to the PR page, e.g.,
    `https://github.com/alphamatic/amp/pull/52` to see the comments
  - There is no way for the reviewer to reopen the PR to signal that there is
    something to address
  - Solutions:
    - Unfortunately this requires discipline and organization in the email
      management of the author and reviewer
    - Maybe author / reviewer can mark the email from GitHub about the
      post-commit review using a "flag" as a reminder that something needs to be
      addressed

- As usual for all the post-commit review, the author shoud:
  - Address the comments as soon as possible
  - Close the conversation on GH, marking them as resolved or engage in
    discussion
  - Tag commits as addressing reviewers' comments

## Apply reviewers' comments for post-commit / optional review

- You can check in directly in `master` using the right task number (e.g.,
  `PartTask351: ...`)

## Reviewers don't follow a branch

- We don't expect code in a branch to be reviewed until a PR
- It's ok to cut corners during the development of the code (e.g., running all
  tests or linting after every commit)
  - The code needs to be production quality when it's merged into `master`

## Reviewers vs assignees

- In a GitHub PR mark people as reviewers and leave assignee fields empty
- In few words assignees are people that are requested to merge the branch after
  the PR
- The difference between them is explained here
  [here](https://stackoverflow.com/questions/41087206)

## Reviewers and authors interactions

- If the reviewer's comment is clear to the author and agreed upon
  - The author addresses the comment with a code change and _after_ changing the
    code (everywhere the comment it applies) marks it as RESOLVED
  - Here we trust the authors to do a good job and to not skip comments,
    otherwise a review will take forever to check on the fixes and so on
  - This mechanism only works if the author is diligent

- If the comment needs further discussion, the author adds a note explaining why
  he/she disagrees and the discussion continues until consensus is reached

- We don't want to leave comments unaddressed since otherwise we don't know if
  it was agreed upon and done or forgotten

- We are ok with doing multiple commits in the branch or a single commit for all
  the comments
  - The goal is for the author to keep the PR clear and minimize his / her
    overhead

## "Pending" comments

- Comments that are marked as "pending" in GitHub are not published yet and
  visible only to the author
- Once you publish the review then an email is sent and comments become visible

# Conventions

## Labels

- `1HR`: Something that can be fixed quickly, if you have 1 hr of spare time
- `Bug`: Report a bug with a proper repro case
- `Cleanup`: Tasks related to improving, refactoring, reorganizing code
- `Design`: Design a new or re-design an old software component
- `Enhancement`: Request an improvement to an existing features
- `Feature`: Implement a new feature
- `Blocking`
- `Blocked`
- `P0`: High priority (it should be picked up as soon as there is time)
- `P1`: Medium priority (icebox)
- `P2`: Low priority (freezer)
- `Paused`: An Issue that was started, we made some progress on it, and then we
  stopped without getting to completion
- `Duplicate`:
- `Discussion`:
- `Question`: How to do XYZ

# ZenHub

## Refs

- [Help](https://help.zenhub.com/support/home)

## Abbreviations
- GH = GitHub
- ZH = ZenHub
- PR = Pull Request

## Agile concepts

### Agile development

- = iterative approach to software development that emphasizes flexibility,
  interactivity, and transparency
- It focus on:
  - Frequent release of useable code
  - Continuous testing
  - Acceptance that reality is always changing and thus requirements

### Sprints

- = fixed length of time during which agreed-upon chunk of work is completed and
  shipped
- Once a Sprint begins, its scope remain fixed
  - The opposite is called "scope creep"

### User story

- = high level descriptions of features from customer's perspective

- A template of a user story is:
  - (Title): "as a <USER>, I want <GOAL> so that <BENEFIT>"
  - User story
  - Acceptance criteria
  - Definition of "Done"

### Epics

- = "big" user story of theme of work
- E.g.,
  - Epic: "Management feature"
  - User story: "As a customer, I want to be able to create an account"

### Product backlog

- Aka "Master Story List"
- = include all the work, e.g.,
  - User stories
  - Half-baked feature ideas
  - Bug fixes
- The goal is to get stuff out of our head and into GH

### Icebox

- = items that are low priority in the product backlog

### Sprint backlog

- = the work that the team is committed to tackle in a given Milestone

### Mapping Agile concepts onto GH

- User stories = GH Issues
- Scrum sprints = GH milestones
- Product backlog = GH list of issues

# ZenHub concepts

## ZH vs GH

- GH Issues are used to provide a place to talk about bugs and features
- ZH builds on top of GH Issues, PRs, Milestones to implement a project
  management layer

## ZH workspaces

- Allows you to bundle multiple GitHub repos into a single view
- Different teams (or team members) can create different pipeline structures for
  the same set of repos
  - Each team can have their own workflow

## ZH epics

- = theme of work containing several sub-tasks required to complete a larger
  goal
- Tasks are broken down into small, manageable chunks
- An Epic is a "big user story"

## Epics vs GH Issues

- GH issues have no hierarchy: they are a list
  - Which issues are related, which are blocked, or dependent?
- Epics add a layer of hierarchy on GH issues
- Epics are like "themes of work"

## Roadmaps

- Organize Projects and Epics into a Gantt-style timeline view
- This shows what is the critical part of the software project

## Sprint planning

- How much work can we actually tackle?
- Can we ship in the next two weeks?
- What issues should be de-scoped?

## Burndown

- = indicator of how projects are processing
- Each time an issue is closed the burndown chart is updated

## Velocity charts

- Reporting on how the amount of work completed fluctuates over time (i.e.,
  sprint over sprint)

## Issue cycle and control chart

- Understand how long Issues take from start to finish

## Cumulative flow diagram

- Track how much work is been done across dates

## Release reports

- Releases are used for tracking long-term and dynamic projects
- Features span multiple sprints

## Milestone vs Epics

- Epics are larger initiatives
  - Contain issues related to the same subject
  - Issues are added and removed

- Milestones are GH sprints
  - Contain issues related in terms of time
  - Issues are fixed once a sprint begins

## Pipelines

- Implement multiple workflows representing how Issues are selected,
  implemented, and completed

# Our conventions

## Epics

- We distinguish Master Epics and non-master Epics (aka "sub-epics")

### Master Epics

- Pipeline "state of progress" of an Issue
- Epics are about projects
  - There is a top level project (MISC, TOP, ORG, NAN)
- Labels are "modifiers", "kind of bugs"

- Master Epics are long-running Epics (i.e., projects)
- TODO(gp): Make examples and leave it of sync with the board

- If you don't know what is the right Epic talk to the RP

  - `AUTOML`: issues related to AutoML research
  - `BUILD`: issues related to the build system
  - `CLEANUP`: issues related to refactorings, reorganization of the code base
    (especially if there is not a more specific master Epic, e.g., a cleanup in
    AutoML will go in AutoML)
  - `CONFIG`: issues related to our configuration layer for experiments
  - `DATA`: issues related to data source exploration
  - `DATAFLOW`: issues related to dataflow design, structure, usage patterns,
    etc.
  - `DOCUMENTATION`: issues related to documenting workflow and so on (unless
    there is a more specific software component that can be applied, e.g.,
    AutoML)
  - `ETL`: issues related to ETL layer
  - `INFRA`: issues related to IT work, sysadmin, AWS
  - `INVESTIGATE`: investigation of packages, tools, papers
  - `KG`: issues related to Knowledge Graph design, schema, implementation, or
    population
  - `NLP`: issues related to NLP
  - `ORG`: anything related to organization, process, conventions
  - `RESEARCH`: general research topics
  - `TOOLS`: tools that make us more productive (e.g., linter, docker, git, etc.)
  - `WIND`: issues related to WIND terminal and data

- TODO: Remove CLEANUP


- Master Epics can be broken down into smaller Epics ( =sub-epics)
  - Ex.: `NLP - RP skateboard`
  - Their title should follow the pattern: `XYZ - `, where XYZ is a master Epic
    title

- Each issue should belong to an Epic: either a sub-epic or a master Epic
  - There is no need to add an issue to a Master Epic if it is already added to
    a sub-epic

### Non-master Epics

- Sub-epics should have a short title and a smaller scope
- Sub-epics should belong to a Master Epic
  - A sub-epic can be moved to `Done` only if all issues nested in it are moved to `Done`
- Organically it's ok to have as many levels of the Epic as needed
  - No need to keep the Epics super well organized in a hierarchy (no Epic
    hyper-graph!)

## Sprint

- 1 Sprint = 2 weeks of work
  - Sprint starts on Monday at 10:00 am ET = after the All-hands meeting
- Sprints are numbered (and can have a commodity name)
  - E.g., "Sprint1 - Gold", "Sprint2 - Natural gas"
- We have a single Sprint for the entire company, since the teams are only a
  convenience to split the work (but we win or lose together)

## Pipelines

- We have the following Pipelines on the ZH board:
  - `New Issues`
  - `Icebox (P2)`
  - `Backlog (P1)`
  - `Ready to Go (P0)`
  - `Sprint backlog`
  - `In Progress`
  - `Review/QA`
  - `Done`
  - `Epics`
  - `Open Research`
  - `Closed`
- Pipeline order is integral for the whole team, so make sure you are not
  changing the order of the pipelines on the board while working

### New Issues

- Any new GH Issue goes here

### Icebox (P2)

- Low priority, un-prioritized issues

### Backlog (P1)

- Issues of medium priority at the moment
  
### Ready to Go (P0)

- Issues of high priority at the moment

### Sprint backlog

- = Sprint backlog
  - All issues to be completed during the current Sprint

### In progress

- Issues that we are currently working on

### Review / QA

- Issues opened for review and testing
- Code is ready to be deployed pending feedback
- Issues stay in Review/QA pipeline while being reviewed

### Epic

- All Epic issues
  - Both Master Epics and Sub-epics

### Open Research

- Contains Issues with exploratory analysis that might be completed, but whose
  implications are still unknown
- We are moving these material to gdocs (e.g., DSE and CDSE) and closing these
  issues
- TODO(*): Remove this when all bugs are closed

### Done

- Definition of `Done` for an issue:
  - PR which is connected to the issue is merged
    - If there is more than one PR, all PRs should be merged
  - All tests are written
  - If an issue requires updating documentation, PR with documentation update is
    merged

### Closed

- Issues that are done and don't need a followup
  - Issues are moved from `Done` to `Closed` by RPs

## Workflow

### Moving tasks between pipelines workflow

- When an assignee starts to work on a Issue, he/she moves it to `In progress`
  pipeline
  - Ideally only Issues from the current Sprint milestone should be selected

- Once the PR is done, the assignee moves the Issue to `Review / QA for the
  duration of the entire review process
  - The Issue doesn't go back to "In progress"
  - We rely on GH emails / PR interface to know what needs to be reviewed

- The issue stays in Review/QA pipeline until all PRs are merged. It means that
  - All tests are written
    - If tests are in a separate PR than the PR with tests should be merged
  - The documentation is updated
    - If the issue requires a documentation update than the PR with
      documentation update should be merged
  - When all the PRs are merged, the assignee moves the Issue to `Done`
  - The assignee doesn't close the GH issue, but only moves it to the `Done`
    pipeline in ZH

- GP & P see if new Issues need to be filed as follow up (or maybe a touch up)
  - Once there is nothing else to do, GP & P move the Issue to "Close"
  - If an issue stays in `Done` for 2 sprints in a row, it is closed automatically

### Filing an issue

- When filing an issue:
  - Add a title for the issue
    - No need for a period at the end of the title
  - Add issue to a Sub-epic, or to a Master Epic, if it doesn't belong to any
    sub-epics
  - Add issue to a pipeline based on its priority
    - if an issue is of high (immediate) priority, add it to `Ready to Go (P0)` pipeline
    - if an issue is of medium priority, add it to the `Backlog (P1)` pipeline
    - if an issue is of low priority, add it to the `Icebox (P2)` pipeline
    - if you are not sure about the priority of an issue, leave it in New Issues pipeline
      - Paul & GP are to sort out the New Issues pipeline by priorities
- When working on an issue
  - Make sure the issue is assigned to you and / or other people who are working
    on it
    - All issues in `Ready to Go (P0)` and all Pipelines to the right should be
      assigned
    - Assign Issue to the one who actually does the work
    - Avoid adding / removing people to the bug just to do some part of the job
      - If you want someone to have a look at the issue and comment on it without 
        actually working on it, just tag them in a comment
        - `@saggese can you please ...`
    - If you don't know whom to assign the issue to or what to do, assign it to
      GP + Paul for rerouting
  - Make sure the issue is properly estimated
    - If the difficulty of the issue changes while you are working on it, update
      its estimate
  - Make sure the issue is situated in the correct Pipeline
- When an Issue is being reviewed
  - Make sure it is added to the current Sprint milestone
  - If an issue requires a PR, make sure the PR is connected to the issue

## Story Points aka Task Estimation

- Each Atomic Issue has log complexity 1, 2, 3, 4, 5 (2x more complex)
  - If some is more complex, needs to turned into an Epic and be broken down
    into atomic issues

- 1:
  - update a test
  - fix a simple break
  - lint a piece of code
- 2:
  - search & replace of a variable in the entire code
  - write a unit test
- 3
  - refactor a piece of code
  - convert functions into a class
- 4:
  - add a simple new feature
  - simple exploratory analysis
- 5:
  - add a complex new feature
  - complex exploratory notebook

- An Epic of course can have any complexity given by its components
- How to calibrate story points across the team members?
  - With each team we pick an "medium" difficult Issue and assign value 3
  - Then we score each Issue with respect to the reference Issue
- For tricky bugs to estimate we assign:
  - 1-5 for coding complexity
  - 1-5 for conceptual complexity
  - Then we sum the scores and potentially break the Issue in sub-Issues

### Open the Sprint workflow

- On Friday before the sprint is over, GP + Paul meet with the RPs to review the
  sprint

## Labels

- TODO(gp): Add definition of labels

- Remove Umbrella, Wontfix, Unclear, Permanent, Enhancement, Duplicate, Feature,
  Question

- We removed "in progress", "to close", "to review", "P0", "P1" and "P2" labels 
  since we want to use the ZH Board pipelines instead
- "Background" label
  - The label "Background" is added to the issues, which are to work on 
    when you are blocked on the core task (waiting for a review, S3 problem, etc.)
  - Requirements for background issues:
    - Issues you can work on independently
      - E.g., refactoring, adding unit tests, linting your code
    - Relevant to our recent work as a team
      - E.g., something related to the old Twitter pipeline is obsolete
    - Issues with little interaction with other people's work including your current work
      - E.g., renaming something in the entire codebase is going to interact with
        everybody
    - Technical debt: something that you keep hitting and that slows you down 
      in your daily work
    - Reading documentation
  - The issue may be converted to a background task either by Paul & GP, or by yourself
  - Background issues have the same issue properties and follow the same rules of moving 
    tasks between pipelines as all other issues

### Closing the Sprint workflow

To close the Sprint the PM team (GP, Paul, Olga) should follow the checklist below.

- On Monday before the All-hands sync-up
  - [ ] Olga: create an issue called "Checklist to close Sprint #N" and copy&paste the following checklist to it
  - [ ] Olga: make sure all completed issues are moved from the Review/QA pipeline to the `Done` pipeline
  - [ ] Olga: make sure all issues in the `Done` pipeline are ready to be closed
    - They all have estimates
    - They all are added to the current Sprint milestone
    - They all belong to the corresponding Epic
  - [ ] Olga: remind Paul & GP to come up with the name of the new Sprint
  - [ ] Olga: create a new tab in the Team assessment spreadsheet
  
- On Monday after All-hands sync-up:
  - [ ] Olga: create a new Milestone in ZH
  - [ ] Paul & GP: close issues in the `Done` pipeline in ZH
  - [ ] Paul & GP: assess team performance as a result of the sprint

- On Tuesday - the second day of the new Sprint
  - [ ] Olga: review the Sprint burndown report and send an email to Paul & GP to review the stats
    - We might want to start sending this info on Fri email 
  - [ ] Olga: make sure that all issues left from the previous Sprint are reassigned to the new Sprint

## Sprint Meetings

- Monday group sync-up
  - Every week at 9 am ET, independently of Day Light Savings (although we can
    try to adjust things to help the Russia team)
  - Usual meeting agenda
- Thursday group meetings
  - Usual group sync-ups
