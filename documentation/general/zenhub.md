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

## Mapping Agile concept onto GH

## GH Issues

- = user stories

## GH milestones

- = Scrum sprints

## GH list of issues

- = product backlog

## ZenHub

## ZH vs GH

- GH Issues are used to provide a place talk about bugs and features
- ZH builds on top of GH Issues, PRs, Milestones to implement project management
  layer

## Workspaces

- Allows you to bundle multiple GitHub repos into a single view
- Different teams (or team members) can create different pipeline structure for
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

# Pipelines

## Pipelines

- Implement multiple workflows representing how Issues are selected,
  implemented, and completed

## New issues

- = any new GH Issue is here

## Icebox

- = low priority, un-prioritized Issues

## Backlog

- Issues that are immediate priorities
- Issues should be prioritized top to bottom in the pipeline

## In progress

- What the team is currently working on, ordered by priority

## Review / QA

- Issues opened for review and testing
- Code is ready to be deployed pending feedback

## Done

- Issues that are tested and ready to be deployed

## Closed

- Closed issues

# How to transition from our project management to ZH

- Our project "XYZ:" (e.g., RP, ETL2, ...) become Epics
  - We can split projects into multiple Epics if the work theme is complex
    - E.g., RP can be decompose into multiple Epics, e.g.,
      - "Approximation of RP ESS"
  - This is also equivalent to our purpose milestones (they become Epics)

- Remove the project tags (at least moving forward)
  - A quick tool to clean up the tags from the bugs

- Give priority to the project
  - E.g., AutoML doc -> Add doc

# Our conventions

## Sprint

- Sprints are numbered and have a commodity name
  - E.g., "Sprint1 - Gold", "Sprint2 - Natural gas"
- We have a single Sprint for the entire company, since the teams (Dev, Tools,
  NLP, AutoML) are only a convenience to split the work (but we win or lose
  together)
  - The Sprint is planned for each team (Dev, Tools, NLP, AutoML)
  - Then we merge the Issues selected for the Sprint in a single Milestone /
    Sprint for the entire company

## Sprint Backlog

- Sprint backlog remains fixed during the sprint
  - Since we just started to use the system, we may overplan sprints (add more
    issues than we can complete)
    - It's better to overplan than underplan
  - All tasks that we didn't compete during the current sprint (e.g., there was
    some work done on the task, but it wasn't completed) are automatically
    transferred to the next sprint, unless they need to be de-prioritize for
    some reason
  - We will improve in sprint planning over time
- Sometimes it's ok to create followup Issues and add them to the current Sprint
  Backlog
  - This can happen for research when one Issue organically leads to a follow up
    bug
  - For development we want to be a little more strict to avoid to go for a
    tangent, as long as we agree that's the right approach in the specific case

**UPD**
 - All issues in Sprint Backlog and forward (all Pipelins to the right) should be assigned
   - If you don’t know whom to assign the issue to, assign it to yourself 
   - If you don’t know what to do, you assign it to GP / Paul for re-route

## Workflow

### Sprint Planning workflow

- The bugs that are candidates for the next Sprint go in SprintCandidate
  pipeline
  - Each Issue needs to have a clear goal, a definition of "done", and "small",
    enough details to be clear to everybody
    - If we are not sure about this, we need to iterate on the Issue until it
      passes our threshold for being actionable and clear
  - Issues are ranked in terms of business value
    - In our case it corresponds to next product milestones, servicing
      customers, and so on
- Each Team estimates each of their Issues in SprintCandidate in terms of story
  points
  - If an issue you want to put in SprintCandidates is a potential Epic:
    - Convert the issue into an Epic
    - Create a single task inside an Epic for breaking down the Epic into
      smaller tasks -- the planning task
    - Put the planning task into SprintBacklog
      - Max 2 points for planning how to break down an Epic
- Then we select for each Team the Sprint in terms of a number of story points
  that is doable in 2 weeks
  - Initially we assume 2 story points = 1 day, so for the sprint is 20 points
    per Team member
  - We will then refine the estimates using Velocity charts
  

  **UPD**
- Production tasks which were originally not included to the Sprint, but are urgent (like "Particle website reanimation") should be added to the current Sprint (Milestone in ZH) and estimated
  - We want to have fair Burndown reports

### Moving Tasks Between Pipelines Workflow

- When an assignee starts to work on a Issue, he/she moves it to "In progress"
  pipeline
- Once the PR process, the assignee moves the Issue to "Review / QA" for the
  duration of the entire review process
  - The Issue doesn't go back to "In progress"
  - We rely on GH emails / PR interface to know what needs to be reviewed
- When the PR is merged, the assignee moves the Issue to "Done"
  - The assignee doesn't close the GH issue, but only moves it to the "Done"
    pipeline in ZH
- GP & P see if new Issues need to be filed as follow up (or maybe a touch up)
  - Once there is nothing else to do, GP & P move the Issue to "Close"

## Epics

- We keep epics on the board
  - It is easy to filter them
- Epic can be moved to "Done" only if all issues nested in it are moved to
  "Done"
- Organically it's ok to have as many levels of the Epic as needed
  - No need to keep the Epics super well organized in a hierarchy (no Epic
    hypergraph!)

## Story Points aka Task Estimation

- Each Atomic Issue &lt;= 5
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

- We want to remove "in progress", "to close", "to review" since we want to use
  the ZH Board instead
- We want to keep using P0, P1, and P2 labels even in ZH
  - The reason is that we have lots of Issues and it's going to difficult to
    keep the pipeline ordered in ZH
  - It's probably a good idea to have priorities set in the sprint backlog, at
    least to mark P0 issues
  - Of course, priorities may change as we go, even inside a Sprint
- TODO(\*): What do we do with the "Paused" label?
  - Maybe change it into a pipeline? Or leave it as Label?

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
    - The points we got vs what we estimated
    - Check if there is anything we need to learn / improve from the last Sprint
- Monday Sprint planning
  - Once every 2 weeks
  - Start on 10 am EST
    - ~30 mins each per team
    - Need to schedule for the team based on their personal preferences
  - Sprint planning in groups: Dev, Tools, AutoML, Max, NLP
- Wed group meetings
  - Usual group sync-ups
- Friday meeting with Olga
  - 30 mins max
  - Review intermediate results of the sprint
  - Q&A from the team
  - We can delete, if not needed
