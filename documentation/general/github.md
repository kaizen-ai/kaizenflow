<!--ts-->
   * [Task management with GitHub](#task-management-with-github)
      * [GH stands for GitHub](#gh-stands-for-github)
      * [Everything we work on comes as a GitHub task](#everything-we-work-on-comes-as-a-github-task)
      * [Issues vs bugs vs tasks](#issues-vs-bugs-vs-tasks)
      * [Use the proper labels](#use-the-proper-labels)
      * [Life cycle of a bug / task](#life-cycle-of-a-bug--task)
      * [Done means "DONE"](#done-means-done)
      * [Tend your tasks](#tend-your-tasks)
      * [File descriptive GH tasks](#file-descriptive-gh-tasks)
      * [Do not change the assignee for a task](#do-not-change-the-assignee-for-a-task)
   * [Code review with GitHub](#code-review-with-github)
      * [Avoid committing to master](#avoid-committing-to-master)
      * [Post-commit comments](#post-commit-comments)
      * [Addressing post-commit comments](#addressing-post-commit-comments)
      * [Use branches and PR](#use-branches-and-pr)
      * [Optional PR review](#optional-pr-review)
      * [Apply reviewers' comments for post-commit / optional review](#apply-reviewers-comments-for-post-commit--optional-review)
      * [Reviewers don't follow a branch](#reviewers-dont-follow-a-branch)
      * [Reviewers vs assignees](#reviewers-vs-assignees)
      * [Reviewers and authors interactions](#reviewers-and-authors-interactions)
      * ["Pending" comments](#pending-comments)



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

## Use the proper labels

- `IN_PROGRESS`: once you start working on it
- `PAUSED`: if you started working on it, but we decided to context switch to
  something else
- `TO_CLOSE`: when it's done
  - Whoever opened the task is in charge of verifying that it is done properly
    and closing
- `TO_REVIEW`: when a review is required
- `P0`, `P1`. `P2`: we use priorities to represent what work is more urgent
  - No priority means that we haven't assigned a priority yet
  - Tasks should be bumped to `P0` before being executed
- `UMBRELLA`: when a task is tracking multiple sub-tasks
- `BLOCKING`: there is an action that needs to be taken in order to make
  progress unblocking the task

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
