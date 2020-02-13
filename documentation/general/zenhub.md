<!--ts-->
   * [Abbreviations](#abbreviations)
      * [References](#references)
      * [GH](#gh)
      * [ZH](#zh)
   * [Agile concepts](#agile-concepts)
      * [Agile development](#agile-development)
      * [Sprints](#sprints)
      * [User story](#user-story)
      * [Epics](#epics)
      * [Product backlog](#product-backlog)
      * [Icebox](#icebox)
      * [Sprint backlog](#sprint-backlog)
   * [Mapping Agile concept onto GH](#mapping-agile-concept-onto-gh)
      * [GH Issues](#gh-issues)
      * [GH milestones](#gh-milestones)
      * [GH list of issues](#gh-list-of-issues)
   * [ZenHub concepts](#zenhub-concepts)
      * [ZH vs. GH](#zh-vs-gh)
      * [Workspaces](#workspaces)
      * [Epics](#epics-1)
      * [Epics vs. GH Issues](#epics-vs-gh-issues)
      * [Roadmaps](#roadmaps)
      * [Sprint planning](#sprint-planning)
      * [Burndown](#burndown)
      * [Velocity charts](#velocity-charts)
      * [Issue cycle and control chart](#issue-cycle-and-control-chart)
      * [Cumulative flow diagram](#cumulative-flow-diagram)
      * [Release reports](#release-reports)
      * [Milestone vs. Epics](#milestone-vs-epics)
   * [Pipelines](#pipelines)
      * [Pipelines](#pipelines-1)
      * [New issues](#new-issues)
      * [Icebox](#icebox-1)
      * [Backlog](#backlog)
      * [In progress](#in-progress)
      * [Review / QA](#review--qa)
      * [Done](#done)
      * [Closed](#closed)
   * [How to transition from our project management to ZH](#how-to-transition-from-our-project-management-to-zh)



<!--te-->

# Abbreviations

## References

- [Help](https://help.zenhub.com/support/home)

## GH

- = GitHub

## ZH

- = ZenHub

# Agile concepts

## Agile development

- = iterative approach to software development that emphasizes flexibility,
  interactivity, and transparency
- It focuses on:
  - Frequent release of useable code
  - Continuous testing
  - Acceptance that reality is always changing, and thus requirements

## Sprints

- Sprints are a fixed length of time during which agreed-upon chunk of work is
  completed and shipped
- Once a Sprint begins, its scope remains fixed
  - The opposite phenomenon is called "scope creep"

## User story

- = high-level descriptions of features from the customers' perspective

- A template of a user story is:
  - (Title): "as a <USER>, I want <GOAL> so that <BENEFIT>"
  - User story
  - Acceptance criteria
  - Definition of "Done"

## Epics

- = "big" user story or theme of work
- E.g.,
  - Epic: "Management feature"
  - User story: "As a customer, I want to be able to create an account"
- It corresponds to our "Umbrella issue"

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

- = the work that the team is committed to tackling in a given Milestone
  - Estimates
  - Requirements

# Mapping Agile concept onto GH

## GH Issues

- = user stories

## GH milestones

- = Scrum sprints

## GH list of issues

- = product backlog

# ZenHub concepts

## ZH vs. GH

- GH Issues are used to provide a place talk about bugs and features
- ZH builds on top of GH Issues, PRs, Milestones to implement project management
  layer

## Workspaces

- Allows you to bundle multiple GitHub repos into a single view
- Different teams (or team members) can create different pipeline structure for
  the same set of repos
  - Each team can have its workflow

## Epics

- = theme of work containing several sub-tasks required to complete a larger
  goal
- Tasks are broken down into small, manageable chunks
- An Epic is a "big user story"

## Epics vs. GH Issues

- GH issues have no hierarchy: they are a list
  - Which issues are related, which are blocked, or dependent?
- Epics add a layer of hierarchy on GH issues
- Epics are like "themes of work"

## Roadmaps

- Organize Projects and Epics into a Gantt-style timeline view
- Roadmaps show what the critical part of the software project is

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

## Milestone vs. Epics

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
    - E.g., we can decompose RP Issues into multiple Epics, e.g.,
      - "Approximation of RP ESS"
  - This is also equivalent to our purpose milestones (they become Epics)
