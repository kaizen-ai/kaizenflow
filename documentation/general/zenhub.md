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
- We have a single Sprint for the entire company, since the teams (ETL, Infra / Tools,
  NLP, KG, AutoML) are only a convenience to split the work (but we win or lose
  together)

## Pipelines

- We have the following Pipelines on the ZH board:
  - New Issues
  - Icebox (P2)
  - Backlog (P1)
  - Ready to Go (P0)
  - In Progress
  - Review/QA
  - Done
  - Epics
  - Open Research
  - Closed
- Pipeline order is integral for the whole team, so make sure you are not
  changing the order of the pipelines on the board while working

### New Issues

- Any new GH Issue is here

### Icebox (P2)

- Low priority, un-prioritized issues that are not immediate priorities

### Backlog (P1)

- = product backlog
  - All issues that should be done to create a product
- issues of meduim priority at the moment
  
### Ready to Go (P0)

- = Sprint backlog
  - All issues to be completed during the current Sprint
- issues of high priority at the moment

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

### Moving tasks between pipelines workflow

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
  - Add issue to the pipeline it should belong to based on its priority
    - if an issue is of high (immediate) priority, add it to Ready to Go (P0) pipeline
    - if an issue is of medium priority, add it to the Backlog (P1) pipeline
    - if an issue is of low priority, add it to the Icebox (P2) pipeline
    - if you are not sure about the priority of an issue, leave it in New Issues pipeline
      - Paul & GP are to sort out the New Issues pipeline by priorities
- When working on an issue
  - Make sure the issue is assigned to you / you and other people who are
    working on it
    - All issues in Ready to Go (P0) and forward (all Pipelines to the right) 
      should be assigned
    - Assign Issue to the one who actually does the work
    - Avoid adding / removing people to the bug just to do some part of the job
      - If you want someone to have a look at the issue and comment on it without 
        actually working on it, just tag them in a comment
        - @OlgaVakhonina can you please ...
    - If you don't know whom to assign the issue to, assign it to yourself
    - If you don't know what to do, assign it to GP + Paul for rerouting
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

## Sprint Meetings

- Monday group sync-up
  - Every week at 9 am ET, independently of Day Light Savings (although we can
    try to adjust things to help the Russia team)
  - Usual meeting agenda
- Thursday group meetings
  - Usual group sync-ups
- Friday meeting with Olga
  - 30 mins max
  - Review intermediate results of the sprint
  - Q&A from the team
  - We can delete, if not needed
