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

# ZenHub

## Refs

- [Help](https://help.zenhub.com/support/home)

## GH

- = GitHub

## ZH

- = ZenHub

## Agile concepts

## Agile development

- = iterative approach to software development that emphasizes flexibility,
  interactivity, and transparency
- It focus on:
  - Frequent release of useable code
  - Continuous testing
  - Acceptance that reality is always changing, and thus requirements

## Sprints

- Sprints are a fixed length of time during which agreed-upon chunk of work is
  completed and shipped
- Once a Sprint begins, its scope remain fixed
  - The opposite is called "scope creep"

## User story

- = high level descriptions of features from customer's perspective

- A template of a user story is:
  - (Title): "as a <USER>, I want <GOAL> so that <BENEFIT>"
  - User story
  - Acceptance criteria
  - Definition of "Done"

## Epics

- = "big" user story of theme of work
- E.g.,
  - Epic: "Management feature"
  - User story: "As a customer, I want to be able to create an account"
- It corresponds to our "Umbrella Issue"

## Product backlog

- Aka "Master Story List"
- = include all the work, e.g.,
  - User stories
  - Half-baked feature ideas
  - Bug fixes
- The goal is to get stuff out of our head and into GH

## Icebox

- = items that are low priority in the product backlog

## Sprint backlog

- = the work that the team is committed to tackle in a given Milestone

## Mapping Agile concepts onto GH

## GH Issues

- = user stories

## GH milestones

- = Scrum sprints

## GH list of issues

- = product backlog

# ZenHub

## ZH vs GH

- GH Issues are used to provide a place to talk about bugs and features
- ZH builds on top of GH Issues, PRs, Milestones to implement project management
  layer

## Workspaces

- Allows you to bundle multiple GitHub repos into a single view
- Different teams (or team members) can create different pipeline structures for
  the same set of repos
  - Each team can have their own workflow

## Epics

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

# How to transition from our project management to ZH

- Our project "XYZ:" (e.g., RP, ETL2, ...) become Epics
  - We can split projects into multiple Epics if the work theme is complex
    - E.g., NLP Epic can be decomposed into multiple Epics, e.g., "NLP - RP
      analyses"
  - This is also equivalent to our purpose milestones (they become Epics)

- Remove the project tags

- Give priority to the project
  - E.g., AutoML doc -> Add doc

# Our conventions

## Epics

We distinguish Master Epics and non-master Epics ( =sub-epics)

### Master Epics

- Master Epics are long-running Epics ( = projects)
  - AUTOML: issues related to AutoML research
  - BUILD: issues related to Jenkins
  - CLEANUP: issues related to refactorings, reorganization of the code base
    (especially if there is not a more specific master Epic, e.g., a cleanup in
    AutoML will go in AutoML)
  - CONFIG: issues related to our configuration layer for experiments
  - DATA: issues related to data source exploration
  - DATAFLOW: issues related to dataflow design, structure, usage patterns, etc.
  - DOCUMENTATION: issues related to documenting workflow and so on (unless
    there is a more specific software component that can be applied, e.g.,
    AutoML)
  - ETL: issues related to ETL layer
  - INFRA: issues related to IT work, sysadmin, AWS
  - INVESTIGATE: investigation of packages, tools, papers
  - KG: issues related to Knowledge Graph design, schema, implementation, or
    population
  - NLP: issues related to NLP
  - ORG: anything related to organization, process, conventions
  - RESEARCH: general research topics
  - TOOLS: tools that make us more productive (e.g., linter, docker, git, etc.)
  - WIND: issues related to WIND terminal and data
- Master Epics can be broken down into smaller Epics ( =sub-epics)
  - Ex.: NLP - RP skateboard
  - Their title should follow the pattern: "XYZ - ", where XYZ is a master Epic
    title
- Each issue should belong to an Epic: either a sub-epic or a master Epic
  - There is no need to add an issue to a Master Epic if it is already added to
    a sub-epic

### Non-master Epics

- Sub-epics should have a short title and a smaller scope
- Sub-epics should belong to a Master Epic
- An epic can be moved to Done only if all issues nested in it are moved to Done
- Organically it's ok to have as many levels of the Epic as needed
  - No need to keep the Epics super well organized in a hierarchy (no Epic
    hypergraph!)

## Sprint

- 1 Sprint = 2 weeks of work
  - Sprint starts on Monday at 10:00 am ET = after the All-hands meeting
- Sprints are numbered and have a commodity name
  - E.g., "Sprint1 - Gold", "Sprint2 - Natural gas"
- We have a single Sprint for the entire company, since the teams (Dev, Tools,
  NLP, AutoML) are only a convenience to split the work (but we win or lose
  together)
  - The Sprint is planned for each team (Dev, Tools, NLP, AutoML)
  - Then we merge the Issues selected for the Sprint in a single Milestone /
    Sprint for the entire company

## Pipelines

- We have the following Pipelines on the ZH board:
  - New Issues
  - Junkyard
  - Icebox
  - Backlog
  - Background tasks
  - Sprint Candidates
  - Sprint Backlog
  - In Progress
  - Review/QA
  - Done
  - Epic
  - Open Research
  - Working Design
  - Reading groups
  - Closed
- Pipeline order is integral for the whole team, so make sure you are not
  changing the order of the pipelines on the board while working

### New Issues

- Any new GH Issue is here

### Junkyard

- Legacy Issues (crap we don't know what the heck do it with)
  - We will either promote them to Icebox, Backlog, or terminate them.

### Icebox

- Low priority, un-prioritized Issues

### Backlog

- = product backlog
  - All issues that should be done to create a product

### Background tasks

- Background issues to work on when you are blocked on the core task (waiting
  for a review, S3 problem, etc.)
- Background tasks can be selected from Backlog, Sprint Candidates, New Issues
  pipelines or filed
  - Backgound issues should be selected by each person independently
  - Each person should always have at least 3-4 background issues in the
    Pipeline
- Requirements for background issues:
  - Issues you can work on independently
    - E.g., refactoring, adding unit tests, linting your code
  - Relevant to our recent work as a team
    - E.g., something related to the old Twitter pipeline is obsolete
  - Issues with little interaction with other people's work including your
    current work
    - E.g., renaming something in the entire codebase is going to interact with
      everybody
  - Technical debt: something that you keep hitting and that slows you down in
    your daily work
  - Reading documentation
- To convert a task to a background issue / file a background issue
  - Assign it to yourself
  - Add it to a sub-epic or a Master Epic
  - Add the label "Background" to it
- When you start working on a background task
  - Estimate it
  - Move it to "In Progress" pipeline

### Sprint Candidates

- Issues that we want to include in the following Sprint
  - They are usually higher priority issues than issues we have in Backlog

### Sprint Backlog

- Issues that we plan to accomplish during the Sprint
- We try to keep the Sprint Backlog fixed during the Sprint
- However it's ok to create follow-up Issues and add them to the current Sprint
  Backlog
  - We want to be flexible even inside the Sprint
    - Especially since almost all issues include a great deal of research
  - This can happen for research when one Issue organically leads to a follow-up
    bug
  - For development we want to be a little stricter so as to avoid going off on
    a tangent, as long as we agree that's the right approach in the specific
    case
- All issues in Sprint Backlog and forward (all Pipelines to the right) should
  be assigned
  - Assign Issue to the one who actually does the work
    - Avoid adding / removing people to the bug just to do some part of the job
    - If you want someone to have a look at the issue and comment on it without
      actually working on it, just tag them in a comment
      - @OlgaVakhonina can you please ...
  - If you don't know whom to assign the issue to, assign it to yourself
  - If you don't know what to do, assign it to GP + Paul for rerouting

### In progress

- Issues that we are currently working on

### Review / QA

- Issues opened for review and testing
- Code is ready to be deployed pending feedback
- Issues stay in Review/QA pipeline while being reviewed

### Epic

- All Epic issues
  - Both Master Epics and sub-epics

### Open Research

- Contains Issues with exploratory analysis that might be completed, but whose
  implications are still unknown

### Working Design

- Design specs subject to change based on implementation iterations
- Not yet ready to go into markdown

### Reading groups

- Issues related to on-going reading groups

### Done

- Definition of Done for an issue:
  - PR which is connected to the issue is merged
    - If there is more than one PR, all PRs should be merged
  - All tests are written
  - If an issue requires updating documentation, PR with documentation update is
    merged

### Closed

- Issues that are done and don't need a followup
  - Issues are moved from Done to Closed by GP & Paul

## Workflow

### Sprint Planning Workflow

- The bugs that are candidates for the next Sprint go in Sprint Candidates
  pipeline
  - Each Issue needs to have a clear goal, a definition of "done", and "small",
    enough details to be clear to everybody
    - If we are not sure about this, we need to iterate on the Issue until it
      passes our threshold for being actionable and clear
  - Issues are ranked in terms of business value
    - In our case it corresponds to next product milestones, servicing
      customers, and so on
  - No need to estimate Issues in Sprint Candidates
- At the beginning of each Sprint we select issues that we want to complete
  during the Sprint
  - These issues go to Sprint Backlog pipeline
  - All issues that are added to Sprint Backlog should be estimated
  - No need to add these issues to current Sprint milestone at this point
- We plan Sprint backlog for each Team in terms of a number of story points that
  is doable in 2 weeks
  - Initially we assume 2 story points = 1 day, so for the sprint is 20 points
    per Team member
  - We will then refine the estimates using Velocity charts
- Production tasks which were originally not included to the Sprint, but are
  urgent (like "Particle website reanimation") should be added to the current
  Sprint (Milestone in ZH) and estimated
  - We want to have fair Burndown reports

### Moving Tasks Between Pipelines Workflow

- When an assignee starts to work on a Issue, he/she moves it to "In progress"
  pipeline
  - Still no need to add the Issue to a current Sprint milestone
- Once the PR process, the assignee moves the Issue to "Review / QA" for the
  duration of the entire review process
  - The Issue doesn't go back to "In progress"
  - We rely on GH emails / PR interface to know what needs to be reviewed
- As soon as the Issue is moved to Review/QA pipleline, it should be added to
  the current Sprint milestone
- The issue stays in Review/QA pipeline until all PRs are merged. It means that
  - All tests are written
    - If tests are in a separate PR than the PR with tests should be merged
  - The documentation is updated
    - If the issue requires a documentation update than the PR with
      documentation update should be merged
  - When all the PRs are merged, the assignee moves the Issue to "Done"
  - The assignee doesn't close the GH issue, but only moves it to the "Done"
    pipeline in ZH
- GP & P see if new Issues need to be filed as follow up (or maybe a touch up)
  - Once there is nothing else to do, GP & P move the Issue to "Close"
  - If an issue stays in Done for 2 sprints in a row, it is closed automatically
    (by Olga)
    
### Closing the Sprint workflow

To close the Sprint the PM team (GP, Paul, Olga) should follow the checklist below.

- On Monday before the All-hands sync-up
  - [ ] Olga: create an issue called "Checklist to close Sprint #N" and copy&paste the following checklist to it
  - [ ] Olga: make sure all completed issues are moved from the Review/QA pipeline to the Done pipeline
  - [ ] Olga: make sure all issues in the Done pipeline are ready to be closed
    - They all have estimates
    - They all are added to the current Sprint milestone
    - They all belong to the corresponding Epic
  - [ ] Olga: remind Paul & GP to come up with the name of the new Sprint
  - [ ] Olga: create a new tab in the Team assessment spreadsheet
  
- On Monday after All-hands sync-up:
  - [ ] Olga: create a new Milestone in ZH
  - [ ] Paul & GP: close issues in the Done pipeline in ZH
  - [ ] Paul & GP: assess team performance as a result of the sprint

- On Tuesday - the second day of the new Sprint
  - [ ] Olga: review the Sprint burndown report and send an email to Paul & GP to review the stats
    - We might want to start sending this info on Fri email 
  - [ ] Olga: make sure that all issues left from the previous Sprint are reassigned to the new Sprint

## Issue Properties

- When filing an issue
  - Add title for the issue
    - No need to for a period at the end of the title
  - Add issue to a sub-epic (or a Master Epic, if it doesn't belong to any
    sub-epics)
- When working on an issue
  - Make sure the issue is assigned to you / you and other people who are
    working on it
  - Make sure the issue is properly estimated
    - If the difficulty of the issue changes while you are working on it, update
      its estimate
  - Make sure the issue is situated in the correct Pipeline
- When an Issue is being reviewed
  - Make sure it is added to the current Sprint milestone
  - If an issue requires a PR, make sure the PR is connected to the issue

## Story Points aka Task Estimation

- Each Atomic Issue <= 5
  - If some is more complex, needs to turned into an Epic and be broken down
    into atomic issues
- An Epic of course can have any complexity given by its components
- How to calibrate story points across the team members?
  - With each team we pick an "medium" difficult Issue and assign value 3
  - Then we score each Issue with respect to the reference Issue
- For tricky bugs to estimate we assign:
  - 1-5 for coding complexity
  - 1-5 for conceptual complexity
  - Then we sum the scores and potentially break the Issue in sub-Issues

## Labels

- We removed "in progress", "to close", "to review" labels since we want to use
  the ZH Board pipelines instead
- We want to keep using P0, P1, and P2 labels even in ZH
  - The reason is that we have lots of Issues and it's going to difficult to
    keep the pipeline ordered in ZH
  - It's probably a good idea to have priorities set in the sprint backlog, at
    least to mark P0 issues
  - Of course, priorities may change as we go, even inside a Sprint

## Sprint Meetings

- Monday group sync-up
  - Every week at 9 am ET, independently of Day Light Savings (although we can
    try to adjust things to help the Russia team)
  - Usual meeting agenda
- Monday Sprint retrospective
  - Probably 10-15 mins
  - Once every 2 weeks at the end of the Sprint, we do a Sprint retrospective
    with the following agenda:
    - We review the work done in the Sprint
    - The story points we got during current Sprint vs previous Sprint
    - Check if there is anything we need to learn / improve from the last Sprint
- Monday Sprint planning
  - Once every 2 weeks
  - Start on 10 am EST
    - ~30 mins each per team
    - Need to schedule for the team based on their personal preferences
  - Sprint planning in groups: Dev, Tools, AutoML, Max, NLP
- Thursday group meetings
  - Usual group sync-ups
- Friday meeting with Olga
  - 30 mins max
  - Review intermediate results of the sprint
  - Q&A from the team
  - We can delete, if not needed
