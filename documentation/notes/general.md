<!--ts-->
   * [Ask somebody if you have any doubts](#ask-somebody-if-you-have-any-doubts)
   * [Collaboration](#collaboration)
      * [Why do we need to follow this handbook?](#why-do-we-need-to-follow-this-handbook)
         * [Learning from each other](#learning-from-each-other)
         * [Consistency and process](#consistency-and-process)
      * [Sync-ups](#sync-ups)
         * [All-hands meetings](#all-hands-meetings)
         * [Technical sync-ups](#technical-sync-ups)
         * [Ad-hoc meetings](#ad-hoc-meetings)
         * [Org emails](#org-emails)
         * [The need for synchronization points](#the-need-for-synchronization-points)
         * [Synchronization point](#synchronization-point)
         * [Morning Moscow sync ups](#morning-moscow-sync-ups)
         * [Morning email](#morning-email)
         * [Morning email: example](#morning-email-example)
      * [Communication](#communication)
         * [Use the right form of communication](#use-the-right-form-of-communication)
         * [DRY applies also to documentation](#dry-applies-also-to-documentation)
         * [Avoid write-once code / research](#avoid-write-once-code--research)
         * [Consistency](#consistency)
         * [Training period](#training-period)
         * [Go slowly to go faster](#go-slowly-to-go-faster)
      * [Improve your English!](#improve-your-english)
         * [Study an English grammar book](#study-an-english-grammar-book)
   * [Vacation policy](#vacation-policy)
      * [Vacation](#vacation)
      * [Taking days off](#taking-days-off)
      * [Mark the calendar](#mark-the-calendar)
      * [Long vacations](#long-vacations)
      * [When you are off, you are off](#when-you-are-off-you-are-off)
   * [Mix (to reorg)](#mix-to-reorg)
      * [Use the tools we have](#use-the-tools-we-have)
      * [File a bug when you have a problem](#file-a-bug-when-you-have-a-problem)
      * [Review](#review)



<!--te-->

# Ask somebody if you have any doubts
- If you have doubts on how to do something you want to do:
    - Ask your team-members
        - The problem is that sometimes somebody tells you his / hers
          interpretation or their workaround for a problem
        - So, be careful
    - Ask Sergey if he is around
    - Don't hesitate to ask GP & Paul

# Collaboration

## Why do we need to follow this handbook?

### Learning from each other
- Good research and software engineer practices allow us to:
    - learn from each other
    - accumulate and distill the wisdom of experts
    - share lessons that we have learned from our mistakes along the way

### Consistency and process
- Consistency is a key enabler to make teams faster
- Productivity increases when team members "work in the same way", i.e.,
  there is a single official way of performing a task, so that it's possible
  and easy to:
    - re-use research and software components
    - help each other in debugging issues
    - add / transfer new people to teams
    - work on multiple projects simultaneously
    - learn from each other's experience and mistakes
    - review each other's work looking for mistakes and improvements
    - ...

- We are not going to discuss this, but rather assume the above as self-evident
  truth

## Sync-ups
- We meet regularly every week and with different audiences to check on the
  progress of the many projects we work on

### All-hands meetings
- All-hands meeting on Mondays has the following goals:
    - summarize projects going on and their status
        - E.g. what are blocking tasks across projects
    - discuss topics of general interest
        - E.g., organization, process
    - talk about team, hiring, ...

### Technical sync-ups
- We meet one time per week for each of the projects (e.g., NLP, INFRA, SCORING)

- Please check your calendar to make sure the times work and the invited
  participants are correct
- The people running the day-to-day project should update the meeting agenda in
  the Gdoc
    - Try to do it one day before so that everybody knows ahead of time what we
      need to talk about and can come prepared

- Typically 2-3 issues are more than enough to fill one hour of discussion
    - Give priority to tasks that are controversial, blocking, or finished
    - No reason to linger on the successes or on the easy stuff

- Send an email or tag a comment to Gdocs to broadcast the agenda

- It's ok to skip a meeting when the agenda is empty, or keep it short when there
  is not much to discuss
    - We don't have to fill one hour every time

### Ad-hoc meetings
- If somebody is unsure about
    - what exactly needs to be done
    - the specs of a task
  please don't hesitate to ask for a quick meeting to discuss the issue

- Better safe than sorry

### Org emails
- GP & Paul will send emails with subject starting with "ORG:" pointing to
  interesting docs that are of general interest and relevance

- Please make sure to read carefully the docs and internalize what we suggest to
  do and, especially, the rationale of the proposed solutions

- If it's ok acknowledge the email replying to all

### The need for synchronization points
- We understand that most of the time everybody is head-down making progress on
  their tasks
- This is great

- Sometimes we need coordination:
    - We need to stop the progress for a bit
    - Do a task
    - Acknowledge that the task is done
    - Go back to pushing

### Synchronization point
- The procedure is:
    - one of us (e.g., Paul / Sergey / me) creates a GitHub task, with:
        - detailed instructions
        - the list of all of the persons affected by the task
    - send a ping with the link on Telegram if the task is urgent
    - everybody does what's asked
    - mark on the GitHub task your name

### Morning Moscow sync ups
- Particle team discusses various organizational topics in the morning, e.g.,
    - best Git practices
    - ORG emails from previous days

### Morning email
- First thing in the morning, send an email to all@particle.one to broadcast what you are working on.

- The goal is:
    - think about what you are going to work on for the day, so you have a clear
      plan
    - make sure people blocked on your tasks know that / whether you are working
      on those tasks
    - broadcast if you are blocked and / or if you don't have tasks
    - communicate realistic ETAs (no reason to be optimistic: complex things
      take time to be done properly)

    - Use a date in the usual format (e.g., 2018-10-10) and add "today",
      "yesterday", "tomorrow", "end of week" so that it's easier to parse
    - please list tasks in priority order
    - please stick to using "TODO" (all caps) as subject, and the suggested
      format, so that it's easy to filter emails and see what everybody is doing
      today and what was doing in the previous days.
    - If your original ETA needs to be updated (e.g., you thought that you would
      have finished a task by yesterday, but it's taking longer) keep the older
      ETA and add the new one, e.g.,
      ```
      #240 edg: Metadata
      - ETA: today (2018-10-10)
      - Original ETA: yesterday (2018-10-09)
      - Reason: it was more complex than what we thought
              or
      - Reason: I had to context switch to ...
      ```

### Morning email: example
- Here is an example of an email
    ```
    Subject: TODO
    - #240 edg: Metadata
        - ETA: today (2018-10-10)
        - Original ETA: yesterday (2018-10-09)
        - Reason: the download didn't finish
    -   #166 edg: Generate h5 file from forms4
        - reorganize document
        - ETA: tomorrow (2018-10-11)
    - Background tasks
        - working with Dima on #203 RP: Market cap mapping
        - #233 RP: Create convenience pickles
        - working with Vitaly #238 INFRA: Total reorg of AWS
        - scoring sync up
    - Blocked on #XYZ Fix the world before it explodes
    - I'm running out of tasks in a few days!
    ```

## Communication

### Use the right form of communication
- Guidelines on how we use the many communication tools we have

- Github issues
    - are concerned with technical details, the process of debugging, discussion
      about a solution, ...
- Jupyter notebooks
    - describe the research in detail
- Markdown files
    - document instructions on how to run the code
    - notes that need to be close to the code itself
    - documents that need to be authoritative and long-term (e.g., reviewed,
      tracked carefully)
- Google docs
    - document research in a descriptive way
    - explain what are the results independently on how they were reached
- IM (Slack, Telegram)
    - for discussions that need tight interaction (like a debug session) or
      immediacy (e.g., "are you ready for the sync up?")
- Emails
    - are rarely used: everything should be in some other form
    - exceptions are to send non-urgent information to everybody

- There should be little replication among these forms of documentation

### DRY applies also to documentation
- DRY! Do not Repeat Yourself
- E.g., it's not a good idea to cut & paste pieces of gdocs in a github bug,
  rather just point to the relevant session on gdocs from the github bug

### Avoid write-once code / research
- Code / research is:
    - written once by a few people
    - read many times by many people
- Therefore it is important to heavily invest in the process of writing it

### Consistency
- Coding / research across our group is done with consistent procedures, code
  layout, and naming conventions

### Training period
- When you start working with us, you need to go through a period of training in
  following the procedures and conventions described in this handbook

- We understand that this is a painful process for you:
    - you need to change your old habits for new habits that you might disagree
      with, or not comprehend
    - you need to rework your code / notebooks that is already perfectly working
      until it adheres to the new conventions

- Understand that this is also a painful process for the reviewers:
    - on top of their usual workload they need to:
         - invest time to explain you how we do things
         - answer your questions
         - try to convey the sense of why these procedures are important

- In a few words, nobody _enjoys_ this process, and yet it is necessary,
  mandatory, and even beneficial
- Acquaintance can take a few days if you are open and patient, but months if you
  resist or treat it as an afterthought
    - Our suggestion is to accept these rules as the existence of gravity

### Go slowly to go faster
- Once you reach proficiency you will be moving much faster and make up for the
  invested time
    - In fact, everyone will be much faster, because everyone will be able to
      look at any part of the codebase or any notebook and get oriented quickly

## Improve your English!

- Makes sure you have English checker in all your tools:
    - pycharm: you can use
      [this](https://plugins.jetbrains.com/plugin/1574-spell-checker-english-dictionary)
      plugin
    - Vim: `set spell`
    - Emacs: TBD (something bloated and probably broken)
    - Google docs: Grammarly
    - Github and web: Grammarly
    - Email client: TBD

- These tools are going to help you improve since you can see the mistake as you
  go

- Feel free to use [Google Translate](https://translate.google.com/) when you are
  not sure about a word or a phrase

- What's the point of doing a really good job if you can't communicate it?

### Study an English grammar book
- I used
  [this](https://www.amazon.com/English-Grammar-Use-Self-study-Intermediate/dp/0521189063/ref=sr_1_3?ie=UTF8&qid=1536765989&sr=8-3&keywords=English+Grammar+in+Use)
  when I learned English (late in life at 25 starting from no-English-whatsoever,
  so you can do it too)

# Vacation policy

## Vacation
- We don't count the vacation days
    - We expect people to take vacations on a as-needed basis in a responsible
      way
    - E.g., if you know that in January there is going to be a product release,
      try to schedule vacation after the deadline

## Taking days off
- If you need to take some day off here and there, please send an email to the
  ORG folks (Tanya, Ilya, Paul, GP) as soon as you know the dates you need to be
  off
    - Please indicate the days you need to be off
    - Indicate if you are going to be completely unreachable ("I am going to go
      for a hike in Antarctica, that is known to have spotty WiFi") vs you still
      have your phone / computer in case of emergency

## Mark the calendar
- Once it's confirmed that it's ok to take the days off
    - Mark the calendar, creating an event spanning multiple days with a title
      "XYZ is OOO" (OOO = Out Of the Office)
    - Send an email to the entire team with an heads up
        - "I am going on vacation from ... to .... I won't be reachable unless
          for emergencies"
    - Send a reminder email to the team, like 1 week before the day that your
      time off is coming up, so that people can plan around you
    - Try to mitigate your absence, e.g.,
        - Think about what you need to finish
        - Think about what work you can pass to someone else
        - Think of who can cover for you

## Long vacations
- E.g., for Christmas, New Year, summer break
- We try to take vacation all at the same time, whenever possible
    - Small startups often implement a "shutdown" where everybody goes in
      vacation at the same time
- The reasons for this (counter-intuitive) approach are:
    1) We don't have redundancy for all roles
        - If one team member for which we don't have extra coverage is on vacation,
          it might not be possible for the rest of the team to make progress
        - So the people that are not on vacation, work with low efficiency or are
          stuck
    2) It's difficult and stressful for who goes on vacation to get back and
       rebuild the state of what happened while on vacation
        - There are thousands of emails sent by others that one needs to catch up
          with
        - Part of the state is lost
        - Fixes were not reviewed, new bugs entered the system, ...
- For all these reasons, in practice, the vacation period becomes the union of
  the vacation of all the team members
    - Therefore we want to maximize the overlap of each team member vacation

## When you are off, you are off
- Completely disconnect and recharge
    - Don't think about work
    - Vacation means "vacate your mind"
- If you want to catch up on reading (e.g., "The pragmatic programmer", technical
  papers on machine learning), that's fine

# Mix (to reorg)

## Use the tools we have
> find . -name "ssh_tunnels.py"
./amp/dev_scripts/infra/ssh_tunnels.py

## File a bug when you have a problem
- Report the command
- The log
- The more information the better

## Review
- We try to review all the changes that go directly to `master` as post-commit
    - Code authors are invited to make changes right away before we all forget
    - Please refrain from committing to `master` and use feature branches unless
      it's a small change or emergency

- We don't review changes that go in feature branches unless the author is ready
  for merging back to `master`
