<!--ts-->
   * [Task management with GitHub](#task-management-with-github)
      * [Everything we work on comes as a GitHub task](#everything-we-work-on-comes-as-a-github-task)
      * [Use the proper labels](#use-the-proper-labels)
      * [Life cycle of a bug](#life-cycle-of-a-bug)
      * [Done means "DONE"](#done-means-done)
      * [Tend your tasks](#tend-your-tasks)
      * [File descriptive GH tasks](#file-descriptive-gh-tasks)
      * [Do not change the assignee for a task](#do-not-change-the-assignee-for-a-task)
   * [Code review with GitHub](#code-review-with-github)
      * [Try not to commit to master](#try-not-to-commit-to-master)
      * [Use branches and PR](#use-branches-and-pr)
      * [Optional review](#optional-review)
      * [Apply reviewers' comments for post-commit / optional review](#apply-reviewers-comments-for-post-commit--optional-review)
      * [Post-commit comments](#post-commit-comments)
      * [Reviewers don't follow a branch](#reviewers-dont-follow-a-branch)
      * [Reviewers vs assignees](#reviewers-vs-assignees)
      * [Reviewers and authors interactions](#reviewers-and-authors-interactions)
      * ["Pending" comments](#pending-comments)



<!--te-->

# Task management with GitHub

## Everything we work on comes as a GitHub task

- We file tasks and then prioritize and distribute the workload
- We try to work always on P0 tasks

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
- `BLOCKING`: there is an action that needs to be taken in order to make progress
  unblocking the task

## Life cycle of a bug

- When you start working on a bug, mark it as in `PROGRESS`
- Make sure the label, the description, and the assignees are up to date
- Try to have `IN_PROGRESS` only for the bugs on which you are actually doing work
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

- Periodically (ideally every single day) go over your tasks and update them, e.g.,
    - add the branch you are working on, when you start
    - add info on what issues you are facing
    - point to gdocs
    - add a pull request link if it's in review
    - add relevant commits that implemented the work

## File descriptive GH tasks

- For any "serious" problem, file a task describing the problem and, ideally, giving a
  "repro case" (i.e., a clear description of how to reproduce the problem)
    - Put as many information about the data as possible, e.g.,
        - the command line you ran
        - the log of the run, maybe the same run using `-v DEBUG` to get more
          info
        - what is the problem and why it is different from what you expected
    - Try to make life easy for the person who is going to have to fix the bug

## Do not change the assignee for a task

- If you need someone to do something just @mention it

- The rationale is that we want one person to be responsible for the task from
  beginning to end

# Code review with GitHub

## Try not to commit to `master`

- Exceptions are small commits that are not part of a feature

## Use branches and PR

- We try to get all the code to be reviewed before it's merged into `master`

## Optional review

- Sometimes we want to track an "optional" PR in GitHub
- Doing a PR and then merging (even through command line) closes the PR
- The approach is just post the link to the PR in the task and use a @mention
  to get attention
    - We can also ping via email / IM to notify the person

## Apply reviewers' comments for post-commit / optional review

- You can check in directly in `master` using the right task number (e.g., 
  `PartTask351: ...`)

## Post-commit comments

- We follow commits to `master` and might ask to perform some fixes

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
   - The author addresses the comment with a code change and *after* changing
     the code (everywhere the comment it applies) marks it as RESOLVED
   - Here we trust the authors to do a good job and to not skip comments,
     otherwise a review will take forever to check on the fixes and so on
   - This mechanism only works if the author is diligent

- If the comment needs further discussion, the author adds a note explaining why
  he/she disagrees and the discussion continues until consensus is reached

- We don't want to leave comments unaddressed since otherwise we don't know if it
  was agreed upon and done or forgotten

- We are ok with doing multiple commits in the branch or a single commit for all
  the comments
   - The goal is for the author to keep the CL clear and minimize his / her
     overhead

## "Pending" comments

- Comments that are marked as "pending" in GitHub are not published yet and
  visible only to the author
- Once you publish the review then an email is sent and comments become visible
