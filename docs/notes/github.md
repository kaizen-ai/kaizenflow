# Github flow

## Everything we work on comes is a GitHub task
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
    - No priority means that we didn't assign a priority yet
    - Tasks should be bumped to `P0` before being executed
- `UMBRELLA`: when a task is tracking multiple sub-tasks
- `BLOCKING`: there is an action that needs to be taken in order to make progress
  unblocking the task

## Done means "DONE"
- A task is closed when the pull request has been reviewed and merged into
  `master`
- If you made specific assumptions, there are loose ends and so on, add a
  `TODO(user)`
- Done means that something is DONE, not 99% done
    - It means that the code is tested, readable and usable by other team mates
- Together we can decide that 99% done is good enough but it should be a
  conscious decision and not a comes as a surprise

## Tend your tasks
- Periodically (ideally every single day) go over your tasks and update them, e.g.,
    - add the branch you are working on, when you start
    - add info on what are the issues you are facing
    - point to gdocs
    - add a pull request link if it's in review
    - add relevant commits that did the work

## File descriptive GH tasks
- For any "serious" problem file a task, describing the problem and, ideally, giving a
  "repro case" (i.e., a clear description of how to reproduce the problem)
    - Put as many information about the data as possible, e.g.,
        - the command line you ran
        - the log of the run, maybe the same run using `-v DEBUG` to get more
          info
        - what is the problem and why it is different from what you expected
    - Try to make the life easy to who is going to have to fix the bug

## Use branches and PR
- We try to get all the code to be reviewed before it's merged into `master`

## Optional review
- Sometimes we want to track an "optional" PR in GitHub
- Doing a PR and then merging (even through command line) closes the PR
- The approach is just post the link to the PR in the task and use a @mention
  to get attention
    - We can also ping via email / IM to notify the person

## Try not to commit to `master`
- Exceptions are small commits that are not part of a feature

## Post-commit comments
- We follow commits to `master` and might ask to perform some fixes

## Don't follow a branch
- We don't expect code in a branch to be reviewed until a PR
- It's ok to cut corner during the development of the code, although the code
  needs to be production quality when it's merged into `master`
