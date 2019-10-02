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

# Branch workflow

## Git branch workflow

- One of the advantages of working with Git is that branches are cheap
- In an ideal world `master` branch is sacred (see Platinum rule of Git):
    - Development should never be done directly on master;
    - Changes to master should only happen by pull-request or merge;
    - One should avoid working in master except in rare cases, e.g., a simple
      urgent bug-fix needed to unblock people.

### Branch workflow best practices

- The next several sections describe the details of our git / review workflow.
  Here we omit the technical details and describe what our flow should look like
  at a higher level.

1. **Always work in a branch**.
    - Call your branch `*TaskXYZ_descriptive_name`, ideally from the bug report
        - E.g., `PartTask274_PRICE_Download_equity_data`
    - Generally it is best to be the sole contributor to your branch
        - If you need to collaborate with somebody on a branch, remember that the
          golden rule of rebase still applies: "do not rebase pushed commits, but
          only your local repo"
    - It is ok to open multiple branches for a given task if needed.
        - In this case try to give a meaningful name to the branch: e.g.,
          `TaskXYZ_v2` is definitively not ok
    - All the rules that apply to `master` apply also to a branch.
        - E.g., commit often, use meaningful commit messages.
        - We are ok with a little looser attitude (e.g., it might be ok to not
          run unit tests before each commit, but be careful!)

2. Commit to your branch **early** and **often** (with `git commit`)
    - Push your local work to the remote branch regularly (with `git push`) at
      least once a day).

3. Rebase your branch onto master **at least once a day**, if the branch stays
   around that long (with git `rebase master`).
4. Keep orthogonal changes in separate branches.
    - Reviewing unrelated changes slows down the review process.
    - Packaging unrelated changes together means no change gets merged until
      **all** the changes are accepted.
    - Easier for you to keep work sane and separated
        - E.g., if you have many conceptual changes in the same branch, you are
          testing / debugging all at once which might make your life (and the
          reviewer’s life) more difficult
5. ReviewBoard review (which we use as replacement to pull request) **early** and
   **often**.
    - **Make sure your PR is coherent**: e.g., it may not need to do everything
      the Task requires, but the PR should be self-contained and not break
      anything
    - If you need to make additional progress on the Task while waiting for a
      review, update the review (or create another branch)
    - If you need changes under review to keep going, create the new branch from
      the old branch rather than from master
    - Frequent small CLs are easier to review, so you will also experience
      faster review turnaround; plus, reviewers like working on smaller changes
      more than working on larger ones
    - Merging changes frequently means other people can more easily see how the
      code is progressing earlier on in the process, and give you feedback
        - e.g., “here it is a much simpler way of doing this”, or even better
          “you don’t need to write any code, just do <this_and_that>)
    - Merged changes are tested in the Jenkins build


### 9.10.2. Create a new branch {#9-10-2-create-a-new-branch}

Pull changes from master


```
> git checkout master
> git pull
```



### 9.10.3. Create and checkout branch {#9-10-3-create-and-checkout-branch}


```
> git branch my_feature
> git checkout my_feature
```


You can also create and branch in one command with:


```
> git checkout -b my_feature
```


From this point on you commit only in the branch and changes to master will not affect your branch


### 9.10.4. Commit your code {#9-10-4-commit-your-code}

When you commit, commits are local and stay on your computer


```
> git status
On branch my-feature
...

> git add ...

> git commit
[my-feature 820b296] My feature is awesome!
```


Commits stay local until you tell explicitly git to "upload" the commits through push


### 9.10.5. Pushing your commits upstream {#9-10-5-pushing-your-commits-upstream}

When you want your code to be pushed to the server (e.g., to back up or to share the changes with someone else), you need to push the branch


```
> git push -u origin my_feature
...
30194fc..820b296  my-feature -> my-feature
Branch 'my-feature' set up to track remote branch 'my-feature' from 'origin'.
```


Note that `-u` tells git to set the upstream of this branch to origin

It is needed only the first time you do it


### 9.10.6. Update your branch with changes from master {#9-10-6-update-your-branch-with-changes-from-master}

If your branch lives long, you want to apply changes made on master to show on your branch

E.g., you can integrate daily by using merge or rebase

1) Assume your branch is clean

E.g., everything is committed, or stashed

2) Pull changes from master on the remote repo


```
> git checkout master
> git pull
```


3) Checkout your feature branch


```
> git checkout my_feature
```


4) Merge stuff from master to `my_feature`


```
> git merge master --no-ff
```


... editor will open a message for the merge commit ...

You can also rebase into master (i.e., you re-apply your changes to master, not the other way around: that would be a disaster)


```
> git rebase master

> git ll ..origin/master
// Now you see that there is nothing in master you don't have

> > git ll origin/master..
// You can see that you are ahead of master
```



### 9.10.7. Create a pull request and an Upsource review {#9-10-7-create-a-pull-request-and-an-upsource-review}

You can raise a pull-request to merge your code into master

Go to the branch on the web interface and push "Compare & pull request".

Additionally, create an Upsource review. To make the life of the reviewer easier, put a link to the Upsource review in the Pull Request.

The procedure for manual merges is as follows. Do not merge yourself unless explicitly requested by a reviewer.

1) Pull changes from remote master branch


```
> git checkout master
> git pull
```


2) Merge your branch into master without fast-forward


```
> git merge --no-ff my-feature
```


3) Push the newly merged master


```
> git push
```


4) Delete the branch, if you are done with it


```
> git branch -d my-feature
