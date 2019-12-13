<!--ts-->
   * [General rules about code review](#general-rules-about-code-review)
      * [Read the Google code review best practices](#read-the-google-code-review-best-practices)
   * [Code review workflows](#code-review-workflows)
      * [Pull request](#pull-request)
      * [Post-commit review](#post-commit-review)
      * [Close the PR and delete the branch](#close-the-pr-and-delete-the-branch)
   * [Some other remarks about getting your code reviewed](#some-other-remarks-about-getting-your-code-reviewed)
      * [PR checklist](#pr-checklist)
      * [The golden rule of code review](#the-golden-rule-of-code-review)
      * [Be clear in the PR request about what you want](#be-clear-in-the-pr-request-about-what-you-want)
      * [Do not mix changes and refactoring / shuffling code](#do-not-mix-changes-and-refactoring--shuffling-code)
      * [Double check before sending a PR](#double-check-before-sending-a-pr)
      * [Give priority to code review](#give-priority-to-code-review)
      * [Why we review code](#why-we-review-code)
      * [Reviewing other people's code is usually not fun](#reviewing-other-peoples-code-is-usually-not-fun)
      * [The first reviews are painful](#the-first-reviews-are-painful)
      * [Apply review comments everywhere](#apply-review-comments-everywhere)
      * [Look at the code top-to-bottom](#look-at-the-code-top-to-bottom)
      * [Answering comments after a review](#answering-comments-after-a-review)
      * [Apply changes to a review quickly](#apply-changes-to-a-review-quickly)
   * [Some other remarks about reviewing other people code](#some-other-remarks-about-reviewing-other-people-code)
      * [Final comment](#final-comment)



<!--te-->

# General rules about code review

## Read the Google code review best practices

-   From the
    [developer's perspective](https://google.github.io/eng-practices/review/developer)
-   From the
    [reviewer's perspective](https://google.github.io/eng-practices/review/reviewer)
-   Where the Google guide says "CL", think "PR"
-   Read it (several times, if you need to)
-   Think about it
-   Understand it

# Code review workflows

## Pull request

-   Our usual review process is to work in a branch and create a pull request
-   See the `git.md` notes for details

## Post-commit review

-   It is best to create a branch with the files you want to review and to add
    TODOs in the code (so that the PR will pick up those sections)
-   If you want a review on a single commit you don't have to create a branch,
    although creating a review branch and pull request is still best
-   The alternative is to comment on the relevant lines in an individual commit

## Close the PR and delete the branch

-   When code is merged into `master` by one of the reviewers through the UI one
    can select the delete branch option
-   Otherwise you can delete the branch using the procedure in `git.md`

# Some other remarks about getting your code reviewed

## PR checklist

-   From
    [Google reviewer checklist](https://google.github.io/eng-practices/review/reviewer/looking-for.html):

-   In asking (and doing) a code review, you should make sure that:
    -   [ ] The code is well-designed.
    -   [ ] The functionality is good for the users of the code.
    -   [ ] The code isn't more complex than it needs to be.
    -   [ ] The developer isn't implementing things they might need in the
            future but don't know they need now.
    -   [ ] Code has appropriate unit tests.
    -   [ ] Tests are well-designed.
    -   [ ] The developer used clear names for everything.
    -   [ ] Comments are clear and useful, and mostly explain why instead of
            what.
    -   [ ] Code is appropriately documented.
    -   [ ] The code conforms to our style guides.

## The golden rule of code review

-   Make life easy for the reviewers
    -   Aka "Do not upset the reviewers"
-   Reviewing other people code is hard and unrewarding work
    -   Do your best for not frustrating the reviewers
-   If you are in doubt "it's probably clear, although I am not 100% sure", err
    on giving more information and answer potential questions

## Be clear in the PR request about what you want

-   Summarize what was done in the PR
    -   Refer to the GH task, but the task alone might not be sufficient
    -   A PR can implement only part of a complex task
        -   Which part is it implementing?
        -   Why is it doing it in a certain way?
-   Do you want just an architectural review?
-   Is it blocking?
    -   Do not abuse asking for a quick review
    -   All code is important and we do our best to review code quickly and
        carefully

## Do not mix changes and refactoring / shuffling code

-   The job of the reviewers become frustrating when the author mixes:

    -   refactoring / moving code; and
    -   changes

-   It is time consuming or impossible for a reviewer to understand what
    happened:

    -   What is exactly changed?
    -   What was moved where?

-   In those cases reviewers have the right to ask the PR to be broken in pieces

-   One approach for the PR author is to:

    -   do a quick PR to move code around (e.g., refactoring) or purely cosmetic
        -   You can ask the reviewer to take a quick look
    -   do the next PRs with the actual changes

-   Another approach is to develop in a branch and break the code into PRs as
    the code firms up
    -   In this case you need to be very organized and be fluent in using Git
    -   E.g., develop in a branch (e.g., `gp_scratch`)
    -   Create a branch (e.g., `TaskXYZ_do_this_and_that`)
    -   Copy / edit the files from `gp_scratch` to `TaskXYZ_do_this_and_that`
    -   Do a PR for `TaskXYZ_do_this_and_that`
    -   Keep working in `gp_scratch` while the review is moving forward
    -   Make changes to the `TaskXYZ_do_this_and_that` as request
    -   Merge `TaskXYZ_do_this_and_that` back into `gp_scratch` and keep moving

## Double check before sending a PR

-   After creating a PR take a look at it to make sure things look good, e.g.,
    -   E.g., are there merge problems?
    -   Did you forget some file?
    -   Skim through the PR to make sure that people can understand what you
        changed

## Give priority to code review

-   We target to give feedback on a PR in 24hr so that the author is not blocked
    for too long
-   Advanced user: to keep working on a related changes and make progress, one
    can:
    -   merge the branch under review with another branch; or
    -   branch from a branch

## Why we review code

-   We spend time reviewing each other code so that we can:
    -   Build a better product, by letting other people look for bugs
    -   Propagate knowledge of the code base through the team
    -   Learn from each other

## Reviewing other people's code is usually not fun

-   Reviewing code is time-consuming and tedious
    -   So do everything you can to make the reviewer's job easier
    -   Don't cut corners
-   If a reviewer is confused about something, others likely would be too
    -   What is obvious to you as the author may not be obvious to readers
    -   Readability is paramount

## The first reviews are painful

-   One needs to work on the same code over and over
    -   Just think about the fact that the reviewer is also reading (still
        crappy) code over and over
-   Unfortunately it is needed pain to get to the quality of code we need to
    make progress

## Apply review comments everywhere

-   Apply a review comment everywhere, not just where the reviewer pointed out
    the issue
-   E.g., if the reviewer says:
    -   Please replace:
    ```python
    _LOG.warning("Hello %s".format(name))
    ```
    with
    ```python
    _LOG.warning("Hello %s", name)
    ```
    you are expected to do this replacement everywhere
    1. In the current review
    2. In all future code you write
    3. In old code, as you come across it in the course of your work
        - Of course don't start modifying the old code in this review, but open
          a clean-up bug, if you need a reminder

## Look at the code top-to-bottom

-   E.g., if you do a search & replace, make sure everything is fine

## Answering comments after a review

-   It's better to answer comments in chunks so we don't get an email per
    comment
    -   Use "start a review" (not in conversation)
-   If one of the comment is urgent (e.g., other comments depend on this) you
    can send it as single comment

## Apply changes to a review quickly

-   In the same way the reviewers are expected to review PRs within 24 hours,
    the author of a PR is expected to apply the requested changes quickly,
    ideally 1 or 2 days

    -   If it takes longer, then either the PR was too big or the quality of the
        PR was too low

-   If it takes too long to apply the changes:
    -   the reviewers (and the authors) might forget what is the context of the
        requested changes
    -   It becomes more difficult (or even impossible) to merge, since the code
        base is continuously changing
    -   It creates dependencies among your PRs
    -   remember that you should not be adding more code to the same PR, but
        only fix the problems and then open a PR with new code
    -   Other people that rely on your code are blocked

# Some other remarks about reviewing other people code

## Final comment

-   Once you are done with the detailed review of the code, you need to

    -   write a short comment
    -   decide what is the next step for the PR, e.g.,
        1. Comment
            - Submit general feedback without explicit approval
        2. Approve
            - Submit feedback and approve merging these changes
        3. Request changes
            - Submit feedback that must be addressed before merging

-   We use an integrator / developer manager, initially with Paul and GP testing
    and merging most of the PRs

-   We use the 3 possible options in the following way:
    1. Comment
        - when reviewers want the changes to be applies and then look at the
          resulting changes to decide the next steps
        - in practice this means "make the changes and then we'll discuss more"
        - E.g., this is of course the right choice for a pre-PR
    2. Approve
        - no more changes: time to merge!
        - Often it is accompanied with the comment "LGMT" (Looks Good To Me)
    3. Request changes
        - This typically means "if you address the comments we can merge"
        - In practice this is more or less equivalent to "Comment"
