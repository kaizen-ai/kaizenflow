# ZenHub

* Refs
- [Help](https://help.zenhub.com/support/home)

* GH
- = GitHub

* ZH
- = ZenHub

## Agile concepts

* Agile development
- = iterative approach to software development that emphasizes flexibility,
  interactivity, and transparency
- It focus on:
  - Frequent release of useable code
  - Continuous testing
  - Acceptance that reality is always changing, and thus requirements

* Sprints
- Sprints are a fixed length of time during which agreed-upon chunk of work is
  completed and shipped
- Once a Sprint begins, its scope remain fixed
  - The opposite is called "scope creep"

* User story
- = high level descriptions of features from customer's perspective

- A template of a user story is:
  - (Title): "as a <USER>, I want <GOAL> so that <BENEFIT>"
  - User story
  - Acceptance criteria
  - Definition of "Done"

* Epics
- = "big" user story of theme of work
- E.g.,
  - Epic: "Management feature"
  - User story: "As a customer, I want to be able to create an account"
- It corresponds to our "Umbrella Issue"

* Product backlog
- Aka "Master Story List"
- = include all the work, e.g.,
  - user stories
  - half-baked feature ideas
  - bug fixes
- The goal is to get stuff out of our head and into GH

* Icebox
- = items that are low priority in the product backlog

* Sprint backlog
- = the work that the team is committed to tackle in a given Milestone
  - estimates
  - requirements

## Mapping Agile concept onto GH

* GH Issues
- = user stories

* GH milestones
- = Scrum sprints

* GH list of issues
- = product backlog

## ZenHub

* ZH vs GH
- GH Issues are used to provide a place talk about bugs and features
- ZH builds on top of GH Issues, PRs, Milestones to implement project management
  layer

* Workspaces
- Allows you to bundle multiple GitHub repos into a single view
- Different teams (or team members) can create different pipeline structure for
  the same set of repos
  - Each team can have their own workflow

* Epics
- = theme of work containing several sub-tasks required to complete a larger goal
- Tasks are broken down into small, manageable chunks
- An Epic is a "big user story"

* Epics vs GH Issues
- GH issues have no hierarchy: they are a list
  - Which issues are related, which are blocked, or dependent?
- Epics add a layer of hierarchy on GH issues
- Epics are like "themes of work"

* Roadmaps
- Organize Projects and Epics into a Gantt-style timeline view
- This shows what is the critical part of the software project

* Sprint planning
- How much work can we actually tackle?
- Can we ship in the next two weeks?
- What issues should be de-scoped?

* Burndown
- = indicator of how projects are processing
- Each time an issue is closed the burndown chart is updated

* Velocity charts
- Reporting on how the amount of work completed fluctuates over time (i.e.,
  sprint over sprint)

* Issue cycle and control chart
- Understand how long Issues take from start to finish

* Cumulative flow diagram
- Track how much work is been done across dates

* Release reports
- Releases are used for tracking long-term and dynamic projects
- Features span multiple sprints

* Milestone vs Epics
- Epics are larger initiatives
  - Contain issues related to the same subject
  - Issues are added and removed

- Milestones are GH sprints
  - Contain issues related in terms of time
  - Issues are fixed once a sprint begins

# Pipelines

* Pipelines
- Implement multiple workflows representing how Issues are selected, implemented,
  and completed

* New issues
- = any new GH Issue is here

* Icebox
- = low priority, un-prioritized Issues

* Backlog
- Issues that are immediate priorities
- Issues should be prioritized top to bottom in the pipeline

* In progress
- What the team is currently working on, ordered by priority

* Review / QA
- Issues opened for review and testing
- Code is ready to be deployed pending feedback

* Done
- Issues that are tested and ready to be deployed

* Closed
- Closed issues

# How to transition from our project management to ZH

- Our project "XYZ:" (e.g., RP, ETL2, ...) become Epics
  - We can split projects into multiple Epics if the work theme is complex
    - E.g., RP can be decompose into multiple Epics, e.g.,
        - "Approximation of RP ESS"
  - This is also equivalent to our purpose milestones (they become Epics)
