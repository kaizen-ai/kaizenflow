- `all@particle.one` is the mailing list with everybody at the company

- We send notifications for commits and other services (e.g., Jenkins) to
  `git@particle.one`
    - TODO(gp): Maybe it should be a more general `dev@particle.one` ?

- A GitHub user `infraparticleone` is used to check out code for services (e.g.,
  Jenkins, ReviewBoard)

# Anatomy of email messages from infra

- The goal is to classify emails so that we can filter email effectively

# Email flow

https://help.github.com/en/categories/receiving-notifications-about-activity-on-github

- I prefer to use filters on the gmail (server) side
    - pros
        - I don't get emails on my cellphone continuously
        - the emails are organized as they arrive
        - folders are on the server side, so my client can simply sync
    - cons
        - gmail interface for filtering emails is horrible

https://mail.google.com/mail/u/0/#inbox -> saggese@gmail.com
    /#settings/filters
https://mail.google.com/mail/u/2/#inbox -> gp@particle.one
https://mail.google.com/mail/u/5/#inbox -> gp@alphamatic.llc

## Pull requests

- `review_requested@noreply.github.com` in "to" field
    ```
    Paul <notifications@github.com>
    Fri, Oct 11, 8:49 PM (22 hours ago)
    to
        alphamatic/amp (amp@noreply.github.com)
        Subscribed (subscribed@noreply.github.com)

    You can view, comment on, or merge this pull request online at:
      https://github.com/alphamatic/amp/pull/31

    Commit Summary
    PartTask403: Add docstrings and type annotations
    Make comments self-consistent
    Add more docstrings, annotations
    ```
- Pattern
    - has the words: "You can view, comment on, or merge this pull request online at:"

## Issue activity

- The emails are coming from:
    - `subscribed@noreply.github.com` -> General GH bug activity (when the issue is opened)
    - `your_activity@noreply.github.com` -> Your GH bug activity
    - `comment@noreply.github.com` -> Comment on GH bug not from you
- The email looks like:
    ```
    GP Saggese <notifications@github.com>
    to
        ParticleDev/commodity_research (commodity_research@noreply.github.com),
        me (saggese@gmail)
        Your (your_activity@noreply.github.com)

    A TOC (table of contents) for our md documentation might help navigating it, since it's not easy to have a view of the high level structure.

    A solution that seems pretty simple is here

    https://github.com/ekalinin/github-markdown-toc

    —
    You are receiving this because you are subscribed to this thread.
    Reply to this email directly, view it on GitHub, or unsubscribe.
    ```
- Pattern:
    - has the words "You are receiving this because you are subscribed to this thread."

## Commits

- The emails are coming from:
    - `notifications@github.com`
- The email looks like:
    ```
    Sergey Malanin <noreply@github.com>
    to git

      Branch: refs/heads/master
      Home:   https://github.com/ParticleDev/commodity_research
      Commit: b0431274fdf619cdb831e1274cd01841fe810b62
          https://github.com/ParticleDev/commodity_research/commit/b0431274fdf619cdb831e1274cd01841fe810b62
      Author: gad26032 <malanin@particle.one>
      Date:   2019-10-12 (Sat, 12 Oct 2019)

      Changed paths:
        M Data_encyclopedia.ipynb
        M Data_encyclopedia.py

      Log Message:
      -----------
      PartTask302 added reader in DE
    ```
- Pattern:
    - has the words "Changed paths:"

## Gdocs

- From `comments-noreply@docs.google.com` or `(Google Docs)` in Subject

# TODO(gp): update amp email to @particle.one
