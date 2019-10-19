<!--ts-->
   * [Github flow](github.md#github-flow)
      * [Everything we work on comes as a GitHub task](github.md#everything-we-work-on-comes-as-a-github-task)
      * [Use the proper labels](github.md#use-the-proper-labels)
      * [Done means "DONE"](github.md#done-means-done)
      * [Tend your tasks](github.md#tend-your-tasks)
      * [File descriptive GH tasks](github.md#file-descriptive-gh-tasks)
      * [Do not change the assignee for a task](github.md#do-not-change-the-assignee-for-a-task)
      * [Try not to commit to master](github.md#try-not-to-commit-to-master)
      * [Use branches and PR](github.md#use-branches-and-pr)
      * [Optional review](github.md#optional-review)
      * [Apply reviewers' comments for post-commit / optional review](github.md#apply-reviewers-comments-for-post-commit--optional-review)
      * [Post-commit comments](github.md#post-commit-comments)
      * [Reviewers don't follow a branch](github.md#reviewers-dont-follow-a-branch)
      * [Reviewers vs assignees](github.md#reviewers-vs-assignees)
      * [Reviewers and authors interactions](github.md#reviewers-and-authors-interactions)
      * ["Pending" comments](github.md#pending-comments)

<!-- Added by: saggese, at: Sat Oct 19 19:46:49 EDT 2019 -->

<!--te-->

# Github flow

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

- TODO(gp): The rest of this should maybe go into `code_review.md` in a section
  like "Reviewing with GitHub" ?

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

- We don’t want to leave comments unaddressed since otherwise we don’t know if
  it was agreed upon and done or forgotten

- We are ok with doing multiple commits in the branch or a single commit for all
  the comments
   - The goal is for the author to keep the CL clear and minimize his / her
     overhead

## "Pending" comments

- Comments that are marked as "pending" in GitHub are not published yet and
  visible only to the author
- Once you publish the review then an email is sent and comments become visible
