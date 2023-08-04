# Codebase clean-up scripts

<!-- toc -->

- [Using the script approach](#using-the-script-approach)
- [Rationale for the script approach](#rationale-for-the-script-approach)
- [Notes on `replace_text.py`](#notes-on-replace_textpy)
- [Example usage of `replace_text.py`](#example-usage-of-replace_textpy)
- [Instructions for the PR author](#instructions-for-the-pr-author)
  * [Example](#example)
- [Instructions for the subrepo integrator](#instructions-for-the-subrepo-integrator)

<!-- tocstop -->

## Using the script approach

- We want to apply clean-up changes to the code base with a script

  - Ideally we would like to apply all the changes automatically through the
    script
  - E.g.: in [SorrTask258](https://github.com/sorrentum/sorrentum/issues/258) a
    script that replaces `pytest.raises` with `self.assertRaises` everywhere in
    the code

- We are ok to make the vast majority (like 95%) of the changes automatically,
  and the rest manually

- We want to keep together in a single PR

  - the script performing automatically the changes and
  - the manual changes from the outcome of the script

- We don't want to check in the PR, the outcome of running the script

  - We can create a separate PR to communicate with the reviewers the output of
    running the script

- The reviewer should run the script on all the repos, run the unit tests, and
  merge (through a PR as usual)

- Typically we use
  [replace_text.py](https://github.com/sorrentum/sorrentum/blob/master/dev_scripts/replace_text.py)
  and wrap it up in a `sh` script

- For more complex problems we write a custom regex-based script

## Rationale for the script approach

- Since we have multiple repos, we can't always easily replace code in one repo
  (e.g., with PyCharm) and have all the other repos work properly
  - So we create a script that applies the correct changes to all the repos

- When developing a change for the entire repo, we want to be able to "reset"
  the work and apply the change from scratch

- Often during the review of the PR:
  - the code in master might be changing creating conflicts
  - the reviewers might ask some changes
  - Using the non-script approach, this creates a lot of manual changes. Instead
    with the script approach we can check out master, run the script to apply
    the changes automatically, regress and merge

## Notes on `replace_text.py`

- Replace an instance of text in all:

  - `.py` file contents

  - `.ipynb` file contents

  - `.txt` file contents

  - filenames

- `--old`: regular expression or string that should be replaced with `--new`

- `--new`: regular expression or string that should replace `--old`

- `--preview`: see script result without making actual changes

- `--only_dirs`: provide space-separated list of directories to process only

- `--only_files`: provide space-separated list of files to process only

- `--exclude_files`: provide space-separated list of files to exclude from
  replacements

- `--exclude_dirs`: provide space-separated list of dir to exclude from
  replacements

- `--ext`: process files with specified extensions
  - defaults are `py, ipynb, txt, sh`
  - use `_all_` for all files

## Example usage of `replace_text.py`

- See [SorrIssue259](https://github.com/sorrentum/sorrentum/issues/259) and the
  related [PR](https://github.com/sorrentum/sorrentum/pull/336) for reference

  - We wanted to make `_to_multiline_cmd()` from `helpers/lib_tasks_utils.py` a
    public function

  - This would require to rename `_to_multiline_cmd()` to `to_multiline_cmd()`
    with the script

  - This
    [script](https://github.com/cryptokaizen/cmamp/blob/master/dev_scripts/cleanup_scripts/SorrTask259_Make_to_multi_line_cmd_public.sh)
    will make the replacement smoothly everywhere in the code except for the
    dirs specified `--exclude_dirs` flag.

- See [SorrIssue258](https://github.com/sorrentum/sorrentum/issues/258) and the
  related [PR](https://github.com/sorrentum/sorrentum/pull/350) for reference

  - We wanted to replace `pytest.raises` with `self.assertRaises`

  - This
    [script](https://github.com/sorrentum/sorrentum/blob/master/dev_scripts/cleanup_scripts/SorrTask258_Replace_pytest_raises_with_self_assertraises.sh)
    will replace it everywhere in the code

  - Note the use of `--ext` flag to specify the file extentions the script
    should work on

- Of course the changes need to be applied in one repo and then propagated to
  all the other repos if the tests are successful

## Instructions for the PR author

- Create a local branch called `...TaskXYZ_..._script` containing:

  - the code that needs to be changed manually
    - E.g.: replacing `pytest.raises` with `self.assertRaises`
  
  - more contextual changes
    - E.g.: adding unit tests to the new functions
  
  - the script for the replacement of the caller named after the GH issue
    - The script should:
      - prepare the target Git client
      - merge this `script` branch with the manual changes
      - make the automated changes
        - E.g., rename a function or replace certain word in comments /
          docstring
  
  - notes in the files that need to be changed manually after the automatic
    script

- Run from scratch the script getting the regression to pass

  - Any time there is a change needed by hand, the change should be added to the
    script branch
  - The goal is to be able to run the script

- File a PR of the `...TaskXYZ_..._script`

- (Optional) Create a PR with the result of the script

  - The author can request a review on this PR, but still the goal is to
    automate as much as possible

- Finally the PR author merges the PR with the results of the script

### Example

- The name of script should be related to the task. E.g:
  `SorrTask259_Make_to_multi_line_cmd_public.sh`

- The script should have a system call to `replace_text.py` to execute the
  required functionality as provided in the above examples

- Create a PR only with the script and the changes

## Instructions for the subrepo integrator

- The integrator:
  - does a `git checkout` of the `...TaskXYZ_..._script`
  - runs the script
  - reviews carefully the changes to make sure we are not screwing things up
  - runs the regressions
  - merges the resulting `...TaskXYZ...` PR
  - ensures `...TaskXYZ_..._script` is merged in `master`
  - deletes `...TaskXYZ_..._script`
