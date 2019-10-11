# Analyzing commits

## Show files modified in a commit
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

# Conflicts

## Getting the conflicting files
- To see the files in conflicts
    ```bash
    git diff --name-only --diff-filter=U
    ```
- This is what the script `git_conflict_files.sh` does

## Accepting "theirs"
```bash
> git checkout --theirs $FILES
> git add $FILES
```

ours and theirs. The first option represents the current branch from which you
executed the command before getting the conflicts, and the second option refers
to the branch where the changes are coming from.

```
> git show :1:README

> git show :2:README

> git show :3:README
```

Stage #1 is the common ancestor of the files,
stage #2 is the target-branch version,
and stage #3 is the version you are merging from.

## How to get out of a messy/un-mergeable branch

- If one screws up a branch

1) rebase to master
2) resolve the conflicts
    - E.g., pick the `master` version when needed:
        `git checkout --theirs ...; git add ...`
3) diff the changes in the branch vs another client at `master`
    ```bash
    > diff_to_vimdiff.py --dir1 $DIR1/amp --dir2 $DIR2/amp --skip_vim
    Saving log to file '/Users/saggese/src/commodity_research2/amp/dev_scripts/diff_to_vimdiff.py.log'
    10-06_15:22 INFO : _parse_diff_output:36  : Reading '/tmp/tmp.diff_to_vimdiff.txt'
    #       DIFF: README.md
    #       DIFF: core/dataflow.py
    #       DIFF: core/dataflow_core.py
    #       DIFF: core/test/test_core.py
    #       DIFF: dev_scripts/diff_to_vimdiff.py
    #       ONLY: diff_to_vimdiff.py.log in $DIR1/dev_scripts
    #       DIFF: dev_scripts/grc
    #       ONLY: code_style.txt in $DIR2/docs/notes
    ...
    #       DIFF: vendors/test/test_vendors.py
    ```
3) diff / merge manually the files that are different
    ```bash
    > diff_to_vimdiff.py --dir1 $DIR1/commodity_research2/amp --dir2 $DIR2/commodity_research3/amp --skip_vim --only_diff_content
    #       DIFF: README.md
    #       DIFF: core/dataflow.py
    #       DIFF: core/dataflow_core.py
    #       DIFF: core/test/test_core.py
    ...
    ```

# Revert

## Revert the last local commit
```bash
> git reset --soft HEAD~
```

# Branch

## Naming a branch

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
- The name is `LemTask274_PRICE_Download_equity_data`

- To use multiple branches for a given task, append a numeral to the name,
  e.g., `LemTask274_PRICE_Download_equity_data01`

## Checking what work was done in a branch

- Look at all the branches available:
    ```bash
    # Fetch all the data from origin.
    > git fetch
    # List all the branches.
    > git branch -r
      origin/HEAD -> origin/master
      origin/PartTask274
      ...
    ```

- Go to the branch:
    ```bash
    > git checkout PartTask274
    ```

- Check what are the commits that are in the current branch `HEAD` but not in
  `master`:
    ```bash
    > gll master..HEAD
    > git log master..HEAD
    * eb12233 Julia     PartTask274 verify dataset integrity                    (  13 hours ago) Sat Sep 28 18:55:12 2019  (HEAD -> PartTask274, origin/PartTask274)
    ...
    * a637594 saggese   PartTask274: Add tag for review                         (    3 days ago) Thu Sep 26 17:13:33 2019
    ```

- To see the actual changes in a branch you can't do (**Bad**)
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

## Check if you need to merge `master` into your feature branch

- You can see what commits are in `master` but missing in your branch with:
    ```bash
    > gll ..master
    * de51a7c saggese   Improve script to help diffing trees in case of difficult merges. Add notes from reviews (   5 hours ago) Sat Oct 5 11:24:11 2019  (origin/master, origin/HEAD, master)
    * 8acd60c saggese   Add a basic end-to-end unit test for the linter                   (  19 hours ago) Fri Oct 4 21:28:09 2019
    ...
    ```
- You want to `rebase` your feature branch onto `master`

## Compare the difference of a  directory among branches

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

# Branch workflow best practices

## Branches are cheap
- One of the advantages of working with Git is that branches are cheap

## `master` is sacred
- In an ideal world `master` branch is sacred (see Platinum rule of Git)
    - Development should never be done directly on master
    - Changes to master should only happen by pull-request or merge
    - One should avoid working in master except in rare cases, e.g., a simple
      urgent bug-fix needed to unblock people
    - `master` should be always never broken (all tests are passing and it is
      deployable)

## Always work in a branch
- Call your branch `PartTaskXYZ_descriptive_name`
    - Ideally from the bug report using `git_show.py`, e.g.,
      `PartTask274_PRICE_Download_equity_data`

- Generally it is best to be the sole contributor to your branch
    - If you need to collaborate with somebody on a branch, remember that the
      golden rule of rebase still applies to this "public" branch: "do not rebase
      pushed commits"
- It is ok to open multiple branches for a given task if needed
    - E.g., if you have multiple chunks of work or multiple people are working on
      orthogonal changes
    - It might be that the task is too big and needs to be broken in smaller bugs
    - In this case try to give a meaningful name to the branch: e.g.,
      `TaskXYZ_meaningful_name_01` is ok

- All the rules that apply to `master` apply also to a branch
    - E.g., commit often, use meaningful commit messages.
    - We are ok with a little looser attitude in your branch
    - E.g., it might be ok to not run unit tests before each commit, but be
      careful!

## Commit to your branch **early** and **often**
- `git commit`
- Push your local work to the remote branch regularly (with `git push`) at least
  once a day

## Merge master into your branch regularly 
- Merge `master` into your feature branch **at least once a day**, if the branch
  stays around that long:
    ```bash
    // Get the .git from the server
    > git fetch
    // Make your master up to origin/master.
    > git checkout master
    > git pull
    // Merge master into my_feature branch.
    > git checkout my_feature
    > git merge master
    ```
- A simpler flow which should be equivalent
    - TODO(gp): Verify that
    ```bash
    // Get the .git from the server
    > git fetch
    // Merge master into my_feature branch.
    > git checkout my_feature
    > git merge origin/master
    ```

## Keep different changes in separate branches

- It is easier for you to keep work sane and separated

- Cons of multiple conceptual changes in the same branches

- You are testing / debugging all at once which might make your life more
  difficult
- Reviewing unrelated changes slows down the review process
- Packaging unrelated changes together that means no change gets merged until
  **all** of the changes are accepted

## Use Pull-requests (PRs) for reviews 

- We are moving towards a flow where everything is reviewed and goes into master
  through PRs

- Make sure your PR is coherent
    - It may not need to do everything the Task requires, but the PR should be
      self-contained and not break anything

- If you **absolutely** need changes under review to keep going, create the new
  branch from the old branch rather than from master (less ideal)
    - Try to avoid branching from branches
        - This creates also dependencies on the order of committing branches
        - You end up with a spiderweb of branches

- Frequent small PRs are easier to review
    - you will also experience faster review turnaround
    - reviewers like working on smaller changes more than working on larger ones
    - PR review time does not scale linearly with lines changed (may be more
      like exponential) 

- Merging changes frequently means other people can more easily see how the code
  is progressing earlier on in the process, and give you feedback
    - E.g., "here it is a much simpler way of doing this", or even better "you
      don’t need to write any code, just do <this_and_that>"

- Add Paul and GP as reviewers, plus anybody else that might be interested

- After the review process Paul or GP will merge the code

- Merged changes are tested in the Jenkins build

# Branch workflow best practices

1) Pull changes from `master`
    - You want to branch from the latest version of master to avoid a merge
    ```bash
    > git checkout master
    > git pull
    ```

2) Go to the branch
    - Create and checkout branch
        ```bash
        > git branch my_feature
        > git checkout my_feature
        ```
    - You can also create and branch in one command with:
        ```bash
        > git checkout -b my_feature
        ```
    - From this point on you commit only in the branch and changes to master
      will not affect your branch
  
3) Push your commits upstream
    - When you want your code to be pushed to the server (e.g., to back up or to
      share the changes with someone else), you need to push the branch upstream
        ```bash
        > git push -u origin my_feature
        ...
        30194fc..820b296  my-feature -> my-feature
        Branch 'my-feature' set up to track remote branch 'my-feature' from 'origin'.
        ```
    - Note that `-u` tells git to set the upstream of this branch to origin
    
    - This operation is needed only the first time you create the branch, but
      not for each `git push`

4) Commit your code
    - When you commit, commits are local and stay on your computer
        ```bash
        > git status
        On branch my-feature
        ...

        > git add ...

        > git commit
        [my-feature 820b296] My feature is awesome!
        ```
    - Commits stay local until you tell explicitly git to "upload" the commits
      through `git push`

5) Update your branch with changes from `master`
    - If your branch lives long, you want to apply changes made on master to
      show on your branch
        
    - Merge flow
    
    - Assume your branch is clean
        - E.g., everything is committed, or stashed

    - Pull changes from `master` on the remote repo
        ```bash
        > git checkout master
        > git pull
        ```

    - Checkout your feature branch
        ```bash
        > git checkout my_feature
        ```

    - Merge stuff from `master` to `my_feature`
        ```bash
        > git merge master --no-ff
        ```

    ... editor will open a message for the merge commit ...

    - In few informal words, the `--no-ff` option means that commits are not
      "inlined" (similar to rebase) even if possible, but a merge commit is
      always used
        - The problem is that if the commits are "inlined" then you can't revert
          the change in one shot like we would do for a merge commit, but you
          need to revert all the inlined changes

- **For now we are suggesting to avoid rebase flow**
    - Rebase flow is advanced; please avoid this flow

    - The reason is that rebase makes thing cleaner when used properly, but can
      get you in trouble if not used properly

    - You can rebase into `master`, i.e., you re-apply your changes to
       `master`
    - Not the other way around: that would be a disaster!
        ```bash
        > git checkout my-feature
        
        // See that you have that master doesn't have.
        > > git ll origin/master..

        // See that master has that you don't have.
        > git ll ..origin/master
        
        > git rebase master

        > git ll ..origin/master
        // Now you see that there is nothing in master you don't have

        > > git ll origin/master..
        // You can see that you are ahead of master
        ```

## Create a pull request
- You can create a Pull Request to merge your code into master
    - Go to the branch on the web interface and push "Compare & pull request"

- The procedure for manual merges is as follows
- Do not merge yourself unless explicitly requested by a reviewer

1) Pull changes from remote `master` branch
    ```bash
    > git checkout master
    > git pull
    ```

2) Merge your branch into `master` without fast-forward
    ```bash
    > git merge --no-ff my-feature
    ```

3) Push the newly merged `master`
    ```bash
    > git push
    ```

4) Delete the branch, if you are done with it:
    ```bash
    > git branch -d my-feature
    ```

# TODO(gp):
- How to sync both git repos?
- How to move forward the amp / infra markers?
