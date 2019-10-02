# #############################################################################
# Analyzing commits
# #############################################################################

* Show files modified in a commit
- You can see the files modified in a given commit hash with:
    ```bash
    > git show --name-only $HASH
    ```

- E.g.,

    ```bash
    > git show --name-only 39a9e335298a3fe604896fa19296d20829801cf2

    commit 39a9e335298a3fe604896fa19296d20829801cf2
    Author: Julia <julia@particle.one>
    Date:   Fri Sep 27 11:43:41 2019

        PartTask274 lint

    vendors/cme/utils.py
    vendors/first_rate/utils.py
    ```

# #############################################################################
# Conflicts
# #############################################################################

* Accepting "theirs"
```bash
> git reset HEAD $FILES
> git checkout --theirs $FILES
> git add $FILES
```

# #############################################################################
# Revert
# #############################################################################

* Revert the last local commit
```bash
> git reset --soft HEAD~
```

# #############################################################################
# Branch
# #############################################################################

* Naming a branch

- You can get automatically the name of a branch corresponding to a GitHub issue
  with:
    ```bash
    > ghi_show.py 274 --only_github
    > ghi_show.py 274 -r ParticleDev/commodity_research --only_github
    # Github:
    #274: PRICE: Download equity data
    https://github.com/alphamatic/lemonade/issues/274

    # Tag:
    LemTask274_PRICE_Download_equity_data
    ```

* Checking what work was done in a branch

- Look at all the branches available:
    ```bash
    > git fetch
    > git branch -r
      origin/HEAD -> origin/master
      origin/PartTask274
      ...
    ```

- Go to the branch:
    ```bash
    > git checkout PartTask274
    ```
- Check what are the commits that are in the current branch but not in master:
    ```bash
    > gll master..HEAD
    > git log master..HEAD
    * eb12233 Julia     PartTask274 verify dataset integrity                    (  13 hours ago) Sat Sep 28 18:55:12 2019  (HEAD -> PartTask274, origin/PartTask274)
    ...
    * a637594 saggese   PartTask274: Add tag for review                         (    3 days ago) Thu Sep 26 17:13:33 2019
    ```

- To see the actual changes in a branch you can't do:
    **Bad**
    ```bash
    > git diff master..HEAD
    ```
  since `git diff` compares two commits and not a range of commits like `git log`
  (yes, Git has a horrible API)

- What you need to do is to get the first commit in the branch and the last from
  `git log` and compare them:
    ```bash
    > git difftool a637594..eb12233
    > gd a637594..eb12233
    ```

* Compare a directory among branches

- This is useful if we want to focus on changes on a single dir
    ```bash
    > git ll master..PartTask274 vendors/cme

    * 39a9e33 Julia     PartTask274 lint                                                  (    2 days ago) Fri Sep 27 11:43:41 2019
    * c8e7e1a Julia     PartTask268 modify according to review16                          (    2 days ago) Fri Sep 27 11:41:47 2019
    * a637594 saggese   PartTask274: Add tag for review                                   (    3 days ago) Thu Sep 26 17:13:33 2019
    ```

    ```bash
    > git diff --name-only a637594..33a46b2 -- vendors helpers
    helpers/csv.py
    vendors/cme/utils.py
    vendors/first_rate/Task274_verify_datasets.ipynb
    vendors/first_rate/Task274_verify_datasets.py
    vendors/first_rate/reader.py
    vendors/first_rate/utils.py
    vendors/test/test_vendors.py
    ```

# #############################################################################
# Branch workflow
# #############################################################################

* Branches are cheap
- One of the advantages of working with Git is that branches are cheap

* `master` is sacred
- In an ideal world `master` branch is sacred (see Platinum rule of Git)
    - Development should never be done directly on master;
    - Changes to master should only happen by pull-request or merge;
    - One should avoid working in master except in rare cases, e.g., a simple
      urgent bug-fix needed to unblock people.

## ############################################################################
## Branch workflow best practices
## ############################################################################

* Always work in a branch
- Call your branch `PartTaskXYZ_descriptive_name`
    - Ideally from the bug report, e.g., `PartTask274_PRICE_Download_equity_data`

- Generally it is best to be the sole contributor to your branch
    - If you need to collaborate with somebody on a branch, remember that the
      golden rule of rebase still applies to this "public" branch: "do not rebase
      pushed commits"
- It is ok to open multiple branches for a given task if needed
    - In this case try to give a meaningful name to the branch: e.g.,
      `TaskXYZ_v2` is definitively not ok
- All the rules that apply to `master` apply also to a branch.
    - E.g., commit often, use meaningful commit messages.
    - We are ok with a little looser attitude
    - E.g., it might be ok to not run unit tests before each commit, but be
      careful!

* Commit to your branch **early** and **often**
- `git commit`
- Push your local work to the remote branch regularly (with `git push`) at least
  once a day

* Rebase your branch onto master
- Rebase onto master **at least once a day**, if the branch stays around that
  long:
- `git rebase master`

* Keep different changes in separate branches

- It is easier for you to keep work sane and separated

- Cons of multiple conceptual changes in the same branches

- You are testing / debugging all at once which might make your life more
  difficult
- Reviewing unrelated changes slows down the review process
- Packaging unrelated changes together means no change gets merged until **all**
  the changes are accepted

* Use ReviewBoard review
- This is a replacement to pull request
- Use ReviewBoard **early** and **often**

- **Make sure your CL is coherent**
    - It may not need to do everything the Task requires, but the CL should be
      self-contained and not break anything

- If you need changes under review to keep going, create the new branch from
  the old branch rather than from master (less ideal)

- Frequent small CLs are easier to review
    - you will also experience faster review turnaround
    - reviewers like working on smaller changes more than working on larger ones

- Merging changes frequently means other people can more easily see how the code
  is progressing earlier on in the process, and give you feedback
    - E.g., "here it is a much simpler way of doing this", or even better "you
      don’t need to write any code, just do <this_and_that>"

- Merged changes are tested in the Jenkins build

### ###########################################################################
### Branch workflow best practices
### ###########################################################################

* Pull changes from master
```bash
> git checkout master
> git pull
```

* Create and checkout branch
```bash
> git branch my_feature
> git checkout my_feature
```

- You can also create and branch in one command with:


```
> git checkout -b my_feature
```

- From this point on you commit only in the branch and changes to master will not
  affect your branch

* Commit your code

- When you commit, commits are local and stay on your computer
```
> git status
On branch my-feature
...

> git add ...

> git commit
[my-feature 820b296] My feature is awesome!
```

- Commits stay local until you tell explicitly git to "upload" the commits through push

* Push your commits upstream

- When you want your code to be pushed to the server (e.g., to back up or to
  share the changes with someone else), you need to push the branch
```bash
> git push -u origin my_feature
...
30194fc..820b296  my-feature -> my-feature
Branch 'my-feature' set up to track remote branch 'my-feature' from 'origin'.
```

- Note that `-u` tells git to set the upstream of this branch to origin

- It is needed only the first time you do it

* Update your branch with changes from master
- If your branch lives long, you want to apply changes made on master to show on
  your branch
- E.g., you can integrate daily by using merge or rebase

1) Assume your branch is clean
- E.g., everything is committed, or stashed

2) Pull changes from master on the remote repo
```bash
> git checkout master
> git pull
```

3) Checkout your feature branch
```bash
> git checkout my_feature
```

4) Merge stuff from master to `my_feature`
```
> git merge master --no-ff
```

... editor will open a message for the merge commit ...

- You can also rebase into master, i.e., you re-apply your changes to master
    - Not the other way around: that would be a disaster
```bash
> git rebase master

> git ll ..origin/master
// Now you see that there is nothing in master you don't have

> > git ll origin/master..
// You can see that you are ahead of master
```

* Create a pull request / ReviewBoard review
- Create an Upsource review
- You can raise a pull-request to merge your code into master
    - Go to the branch on the web interface and push "Compare & pull request"

- The procedure for manual merges is as follows. Do not merge yourself unless
  explicitly requested by a reviewer.

1) Pull changes from remote master branch
```bash
> git checkout master
> git pull
```

2) Merge your branch into master without fast-forward
```bash
> git merge --no-ff my-feature
```

3) Push the newly merged master
```bash
> git push
```

4) Delete the branch, if you are done with it
```bash
> git branch -d my-feature
```