<!--ts-->
   * [General rules about code review](#general-rules-about-code-review)
      * [Read the Google code review best practices](#read-the-google-code-review-best-practices)
   * [Code review workflows](#code-review-workflows)
      * [Pull request](#pull-request)
      * [Post-commit review](#post-commit-review)
   * [PR checklist](#pr-checklist)
   * [Some other remarks based on our experience](#some-other-remarks-based-on-our-experience)
      * [Give priority to code review](#give-priority-to-code-review)
      * [Why do we review code](#why-do-we-review-code)
      * [Reviewing other people's code is usually not fun](#reviewing-other-peoples-code-is-usually-not-fun)
      * [The first reviews are painful](#the-first-reviews-are-painful)
      * [Apply review comments everywhere](#apply-review-comments-everywhere)
      * [Look at the code top-to-bottom](#look-at-the-code-top-to-bottom)
      * [Close the PR and delete the branch](#close-the-pr-and-delete-the-branch)



<!--te-->

# General rules about code review

## Read the Google code review best practices
- From the [developer's perspective](https://google.github.io/eng-practices/review/developer)
- From the [reviewer's perspective](https://google.github.io/eng-practices/review/reviewer)
- Where the Google guide says "CL", think "PR" 
- Read it (several times, if you need to)
- Think about it
- Understand it

# Code review workflows 

## Pull request
- Our usual review process is to work in a branch and create a pull request
- See the `git.md` notes for details

## Post-commit review
- It is best to create a branch with the files you want to review and to add
  TODOs in the code (so that the PR will pick up those sections)
- If you want a review on a single commit you don't have to create a branch,
  although creating a review branch and pull request is still best
- The alternative is to comment on the relevant lines in an individual commit 

## Close the PR and delete the branch
- When code is merged into `master` by one of the reviewers through the UI one
  can select the delete branch option
- Otherwise you can delete the branch using the procedure in `git.md`

# PR checklist
- From [Google reviewer
  checklist](https://google.github.io/eng-practices/review/reviewer/looking-for.html):

- In asking (and doing) a code review, you should make sure that:
    - [ ] The code is well-designed.
    - [ ] The functionality is good for the users of the code.
    - [ ] The code isn't more complex than it needs to be.
    - [ ] The developer isn't implementing things they might need in the future
      but don't know they need now.
    - [ ] Code has appropriate unit tests.
    - [ ] Tests are well-designed.
    - [ ] The developer used clear names for everything.
    - [ ] Comments are clear and useful, and mostly explain why instead of what.
    - [ ] Code is appropriately documented.
    - [ ] The code conforms to our style guides.

# Some other remarks based on our experience

## Give priority to code review
- We target to give feedback on a PR in 24hr so that the author is not blocked
  for too long
- Advanced user: to keep working on a related changes and make progress, one can:
    - merge the branch under review with another branch; or
    - branch from a branch

## Why we review code
- We spend time reviewing each other code so that we can:
    - Build a better product, by letting other people look for bugs
    - Propagate knowledge of the code base through the team
    - Learn from each other

## Reviewing other people's code is usually not fun
- Reviewing code is time-consuming and tedious
    - So do everything you can to make the reviewer's job easier
    - Don't cut corners
- If a reviewer is confused about something, others likely would be too
    - What is obvious to you as the author may not be obvious to readers
    - Readability is paramount

## The first reviews are painful
- One needs to work on the same code over and over
    - Just think about the fact that the reviewer is also reading (still crappy)
      code over and over
- Unfortunately it is needed pain to get to the quality of code we need to make
  progress

## Apply review comments everywhere
- Apply a review comment everywhere, not just where the reviewer pointed out
  the issue
- E.g., if the reviewer says:
    - Please replace:
	```python
	_LOG.warning("Hello %s".format(name))
	```
  with
	```python
    _LOG.warning("Hello %s", name")
	```
  you are expected to do this replacement everywhere
    1) in the current review
    2) in all future code you write
    3) in old code, as you come across it in the course of your work 
        - of course don't start modifying the old code in this review, but open
          a clean-up bug, if you need a reminder

## Look at the code top-to-bottom
- E.g., if you do search & replace, make sure everything is fine
