# Abbreviations
- GH = GitHub
- ZH = ZenHub
- PR = Pull Request
- RP = Responsible Particle

# Task management with GitHub

## Everything we work on comes as a GitHub task

- We file tasks and then prioritize and distribute the workload
- We try to always work on high priority (e.g., `P0`) tasks

## Issues vs bugs vs tasks

- We call GitHub issues "issues", "bugs", and "tasks" interchangeably 
- "Bugs" is a bit improper since many times we use GitHub to track ideas,
  activities, and improvements, and not only defects in the code
- The best names are "tasks" and "issues"

## Life cycle of a bug / task

- We use ZenHub as project management layer on top of GitHub
- When you start working on an Issue, move it to the `In Progress` pipeline on
  ZH
  - Make sure the description, the assignees, and the Epic are up to date
  - Try to use `In Progress` only for Issues you are actively working on
  - A rule of thumb is that you should not have more than 2-3 `In Progress`
    Issues
  - Give priority to Issues that are close to being completed, rather than
    starting a new Issue

- Update an Issue on GH often, like at least once a day of work
  - Show the progress to the team with quick updates
  - Update your Issue with pointers to gdocs, PRs, notebooks
  - If you have questions, post them on the bug and tag people

- When, in your opinion, there is no more work to be done on your side on an
  Issue, please move it to the `Done` or `Review/QA` pipeline, but do not close
  it
- If we decide to stop the work, add a `Paused` label and move it back to the
  backlog, e.g., `Ready to Go (P0)`, `Backlog (P1)`, `Icebox (P2)`

- If there is something that needs to be done, please update the Issue
  summarizing status and next actions

- Connect the PR to the Issue
  - You should do this on the PR page of GH
  - If you do not see the option to connect the PR to the Issue on that page,
    then the likely cause is that you do not have the ZH plugin installed in
    your browser. Check this and install the plugin if you do not have it.
  - The Issue is then automatically moved to `Review/QA`

- We leave Issues in the `Done` state when we need to do some other work after
  it and we don't want to forget about this by closing it
  - The rule is that only the one who filed the bug or a RP should close the
    bug, but only after verifying that all work has done up to our standards

## Done means "DONE"

- A task is closed when the pull request has been reviewed and merged into
  `master`
- If you made specific assumptions, or if there are loose ends, etc., add a
  `TODO(user)`
- Done means that something is DONE, not 99% done
  - It means that the code is tested, readable and usable by other teammates
- Together we can decide that 99% done is good enough, but it should be a
  conscious decision and not come as a surprise
- If you know that more work needs to be done, file more Issues explaining what
  needs to be done

## Tend your tasks

- Periodically (ideally every single day) go over your tasks and update them,
  e.g.,
  - Add the branch you are working on, when you start
  - Add information about what problems you are facing
  - Describe what was done and what needs to be done
  - Point to gdocs
  - Add a pull request link if it's in review

## File descriptive GH tasks

- For any "serious" problem, file an Issue describing the problem and, ideally,
  giving a "repro case" (i.e., a clear description of how to reproduce the
  problem)
  - Put as much information about the Issue as possible, e.g.,
    - The command line you ran
    - The log of the run
      - Maybe the same run using `-v DEBUG` to get more info on the problem
    - What the problem is and why it is different from what you expected
  - You need to make life easy for the person who is going to have to fix the
    Issue

## Do not change the assignee for a task

- If you need someone to do something just `@mention` it

- The rationale is that we want one person to be responsible for the task from
  beginning to end and not ping-pong the responsibility
  - Collective ownership of a task means that nobody owns it

# Code review with GitHub

- TODO(gp): Move it `code_review.md`

## Avoid committing to `master`

- Exceptions are small commits that are not part of a feature
  - E.g., fixing a break, improving documentation

## Use branches and PRs

- All code should be reviewed before it is merged into `master`

## Optional PR review

- Sometimes we want to do an "optional" review in GitHub

- The process is:
  - Create a PR
  - Tag the reviewers, adding a description if needed, like in the normal PR
    flow
  - Merge the PR without waiting for the review

- Unfortunately merging the PR automatically closes the PR

- The problem is that once the reviewers get to that PR and add comments,
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

- Just like for pre-commit reviews, the author should:
  - Address the comments as soon as possible
  - Close the conversation on GH, marking them as resolved or engage in
    discussion
  - Tag commits as addressing reviewers' comments

## Reviewers don't follow a branch

- We don't expect code in a branch to be reviewed until a PR is filed
- It's ok to cut corners during the development of the code (e.g., running all
  tests or linting after every commit)
  - The code needs to be production quality when you propose to merge into
    `master`

## Reviewers vs assignees

- In a GitHub PR mark people as reviewers and leave the assignee field empty
- The difference between reviewers and assignees is explained 
  [here](https://stackoverflow.com/questions/41087206)
- In a few words assignees are people that are requested to merge the branch
  after the PR, when they are different from the reviewers

## Reviewers and authors interactions

- If the reviewer's comment is clear to the author and agreed upon
  - The author addresses the comment with a code change and _after_ changing the
    code (everywhere the comment it applies) marks it as `RESOLVED` on the GitHub
    interface
  - Here we trust the authors to do a good job and to not skip / lose comments
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
- Once you publish the review, then an email is sent and comments become visible

# ZenHub

## Refs

- [Help](https://help.zenhub.com/support/home)

## Agile concepts

### Agile development

- = iterative approach to software development that emphasizes flexibility,
  interactivity, and transparency
- It focus on:
  - Frequent releases of useable code
  - Continuous testing
  - Acceptance that reality is always changing and thus requirements are

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

- = the work that the team is committed to tackling in a given Milestone

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

- Track how much work has been done across dates

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

- Master Epics are long-running Epics (i.e., projects)

- If you don't know what the right Epic is, talk to an RP

- TODO: Remove Epics `CLEANUP`, `INVESTIGATE`
  - There is a top level project (MISC, TOP, ORG, NAN)
- TODO(gp): Make examples and leave it of sync with the board

- Each issue should belong to an Epic: either a sub-epic or a master Epic
  - There is no need to add an issue to a Master Epic if it is already added to
    a sub-epic

### Sub-epics

- Master Epics can be broken down into smaller Epics (=sub-epics)
  - Ex.: `NLP - RP skateboard`
  - Their title should follow the pattern: `XYZ - `, where XYZ is a master Epic
    title
- Sub-epics should have a short title and a smaller scope
- Sub-epics should belong to a Master Epic in ZenHub
  - A sub-epic can be moved to `Done` only if all issues nested in it are moved
    to `Done`

## Sprint

- 1 Sprint = 2 weeks of work
  - Sprint starts on Monday at 10:00 am ET = after the All-hands meeting
- Sprints are numbered (and can have a commodity name)
  - E.g., "Sprint1 - Gold", "Sprint2 - Natural gas"
- We have a single Sprint for the entire company, since the teams are only a
  convenience for splitting the work (but we win or lose together)

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
- We are moving these materials to gdocs (e.g., DSE and CDSE) and closing these
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

- Issues that are done and don't need a follow-up
  - Issues are moved from `Done` to `Closed` by RPs

## Labels

- Labels represent "qualifications" of an Issue that are not represented by Epics
  or Pipelines

- TODO(gp): Add definition of labels in GitHub
- TODO(gp): Remove Umbrella, Wontfix, Unclear, Permanent, Enhancement, Duplicate,
  Feature, Question

### Pipeline vs Epic vs Label
- A ZH Pipeline represents the "progress" status of an Issue
- A ZH Epic pulls together Issues that are somehow related by their topic
- A GH Label represents characteristics like "bug", "1 hr", "discussion"

- Note that certain attributes of an Issue are clearly in one of the
  classifications above
  - E.g., `P0` could be a label in the `Backlog` pipeline
  - We decided to use different pipelines for priority to make it simpler to
    separate Issues given their urgency

## Workflows

### Moving tasks between pipelines workflow

- When an assignee starts to work on a Issue, he/she moves it to the `In
  progress` pipeline
  - Ideally only Issues from the current Sprint milestone should be selected

- Once the PR is done, the assignee moves the Issue to `Review / QA for the
  duration of the entire review process
  - The Issue doesn't go back to "In progress"
  - We rely on GH emails / PR interface to know what needs to be reviewed

- The issue stays in Review/QA pipeline until all PRs are merged. It means that
  - All tests are written
    - If tests are in a separate PR, then the PR with tests should be merged
  - The documentation is updated
    - If the issue requires a documentation update then the PR with the
      documentation update should be merged
  - When all the PRs are merged, the assignee moves the Issue to `Done`
  - The assignee doesn't close the GH issue, but only moves it to the `Done`
    pipeline in ZH

- GP & P see if new Issues need to be filed as follow-up (or maybe a touch up)
  - Once there is nothing else to do, GP & P move the Issue to "Closed"
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

- Each Atomic Issue has a log complexity in terms of "story points" of `1`, `2`,
  `3`, `4`, `5`, where to go from one level to the successive the complexity /
  effort increases 2 times
  - E.g., an Issue with complexity `2` is 2x more complex than a task with
    complexity `1`
  - Complexity `3` is 2x more complex than a task with complexity
    `2` and thus 4x more complex than a task of complexity `1`
  - Thus a task with complexity `5` is 16x more complex than a task of complexity
    `1`
  - Alas life is exponentially complex
  - If some Issue is more complex than `5`, it needs to turned into an Epic or be
    broken down into atomic issues

- Some example of story point complexity:
  - `1`:
    - Update a unit test
    - Fix a simple break
    - Lint a piece of code
  - `2`:
    - Search and replace of a variable in the entire code base (including
     running tests!)
    - Write a unit test
  - `3`:
    - Refactor a piece of code
    - Collect functions into a nice class
  - `4`:
    - Add a simple new feature (with unit tests!)
    - Implement a simple exploratory analysis
  - `5`:
    - Add a complex new feature (with unit tests!)
    - Implement a complex exploratory notebook

- An Epic of course has a complexity given by its components
- For tricky bugs to estimate we assign:
  - 1-5 for coding complexity
  - 1-5 for conceptual complexity
  - Then we sum the scores and potentially break the Issue in sub-Issues

### How to make good estimates?
- It's hard to estimate what you know you don't know (known unknowns)
  - It's very hard to estimate things that you don't know you don't know (unknown
    unknowns)
  - The trick is not to shy away from the estimation saying "it's ready when it's
    ready"
    - This approach is not acceptable
    - We need to learn how to make estimates and we can achieve that only by
      trying hard to estimate and understand why our estimates are incorrect
- Check the Pragmatic Programmer chapter about it (e.g., see
  https://github.com/alphamatic/amp/blob/master/documentation/general/the_pragramatic_programmer.md#pp_tip-18-estimate-to-avoid-surprises)
- Some heuristics:
  - [Ninety-ninety rule](https://en.wikipedia.org/wiki/Software_development_effort_estimation#Humor):
    - The first 90 percent of the code accounts for the first 90 percent of the
      development time. The remaining 10 percent of the code accounts for the
      other 90 percent of the development time.
  - Hofstadter's law:
    - It always takes longer than you expect, even when you take into account
      Hofstadter's Law
  - Fred Brooks' law:
    - What one programmer can do in one month, two programmers can do in two months.
  - From https://en.wikipedia.org/wiki/Parkinson%27s_law
    - Work expands so as to fill the time available for its completion

## Sprint retrospective / planning

- By Thursday EOD (RPs)
  - [ ] Move all the completed issues from the `Review/QA` pipeline to the `Done`
    pipeline
  - [ ] Make sure all issues in the `Done` pipeline are ready to be closed
    - They all have estimates
    - They all are added to the current Sprint milestone
    - They all belong to an Epic

- By Friday EOD (GP + Paul)
  - [ ] Do the team assessment
  - [ ] Look at the statistics of the sprint (e.g., Sprint burndown report)
    - TODO(GP, Paul): Describe exactly what to do

- By Friday EOD (RPs + teams)
  - [ ] Prepare the Sprint backlog

- Saturday / Sunday
  - [ ] GP and Paul review the Sprint backlog

- Monday group meeting
  - Every week at 9 am ET, independently of Day Light Savings (although we can
    try to adjust things to help the Russia team)
  - Review the Sprint progress in terms of plots

- Thursday group meetings
  - Usual tech sync-ups
