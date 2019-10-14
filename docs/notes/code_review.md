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

# General rules about code review

## Read the Google code review best practices
- From the [developer's perspective](https://google.github.io/eng-practices/review/developer)
- From the [reviewer's perspective](https://google.github.io/eng-practices/review/reviewer)
- Where the Google guide says "CL", think "PR" 
- Read it (several times, if you need to)
- Think about it
- Understand it

# Some other remarks based on our experience

## Give priority to code review
- We target to give feedback on a PR in 24hr so that the author is not blocked
  for too long
- Advanced user: to keep working on a related changes and make progress, one can:
    - merge the branch under review with another branch; or
    - branch from a branch

## Why do we review code
- We spend time reviewing each other code so that we can:
    - build a better product
    - learn from each other

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
