# #############################################################################
# Post a review
# #############################################################################

* Post-commit review
- If you want a review on a single commit (or a few commits) you don't have to
  create a branch, although still creating a review branch is best

- Find the official name of the GitHub issue with:
	```bash
	> ghi_show.py 274 --only_github
	> ghi_show.py 274 -r ParticleDev/commodity_research --only_github
	# Github:
	#274: PRICE: Download equity data
	https://github.com/alphamatic/lemonade/issues/274

	# Tag:
	LemTask274_PRICE_Download_equity_data
	```

* Post a review from a git branch

- Look at all the available branches:
	```bash
	> git branch -r
	  origin/HEAD -> origin/master
	  origin/ParTask356_RESEARCH_Improve_dataflow_framework1
	...
	```

- Go to the branch you want to ask to be reviewed:
	```bash
	> git checkout ParTask356_RESEARCH_Improve_dataflow_framework1
	```

- Check what are the commits that are on the current branch but not on `master`
    ```bash
    > gll master..
    * fe37b57 paul      ParTask356: bug fix and move more result_bundle-style stats into dataflow nodes. (  16 hours ago) Sat Sep 28 00:30:55 2019  (HEAD -> ParTask356_RESEARCH_Improve_dataflow_framework1, origin/ParTask356_RESEARCH_Improve_dataflow_framework1)
    * ca0f6b0 paul      ParTask356: lint and remove outdated comment                      (  17 hours ago) Fri Sep 27 23:39:05 2019
    ...
    * 03a7537 paul      Lint                                                    (  25 hours ago) Fri Sep 27 15:42:30 2019
    * 9ca6f17 paul      ParTask356: add tests                                   (  25 hours ago) Fri Sep 27 15:40:24 2019
    ```

- Post the review with the commit range `first..last`:
	```bash
	> rbt post --summary 'Review for branch ParTask356_RESEARCH_Improve_dataflow_framework1' 9ca6f17..fe37b57
	```

- TODO(gp): Check `--submit-as`

# #############################################################################
# Close a review
# #############################################################################

* Ship it!
- When a review is completed

# #############################################################################
# General rules about code review
# #############################################################################

* Read Google code review
- From the developer perspective
    - `https://google.github.io/eng-practices/review/developer`
- From the reviewer perspective
    - `https://google.github.io/eng-practices/review/reviewer`
- Read it (several times, if you need to)
- Think about it
- Understand it

## ############################################################################
## Some other remark based on our experience
## ############################################################################

* Why do we review code
- We spend time reviewing each other code so that we can:
    - build a better product
    - learn from each other

* Reviewing other people code is not fun
- As a general reminder, reviewing other people code is not fun
    - So don't cut corners when reviewing the code

* The first reviews are painful
- One needs to work on the same code over and over
    - Just think about the fact that the reviewer is also reading (still crappy)
      code over and over
- Unfortunately it is needed pain to get to the quality of code we need to make
  progress

* Apply review comment everywhere
- Apply a review comment everywhere, not just where the review pointed out
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
    1) in all the current review
    2) in all the future code you are going to write
    3) in the old code, as you see these instances of the problem
        - of course don't start modifying the code in this review, but open a
          clean up bug, if you need a reminder

* Look at the code top-to-bottom
- E.g., if you do search & replace make sure everything is fine
