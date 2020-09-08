<!--ts-->
   * [pre-commit documentation](#pre-commit-documentation)
   * [Enable pre-commit in your repo](#enable-pre-commit-in-your-repo)
   * [Workflows](#workflows)
      * [Pre-commit on staged files](#pre-commit-on-staged-files)
      * [Pre-commit on all the files](#pre-commit-on-all-the-files)
      * [Pre-commit on specific files](#pre-commit-on-specific-files)
      * [Pre-commit only one hook](#pre-commit-only-one-hook)
      * [Pre-commit all the files modified in the current branch](#pre-commit-all-the-files-modified-in-the-current-branch)
      * [Run hooks before of committing](#run-hooks-before-of-committing)
      * [Skip running hooks](#skip-running-hooks)
      * [Generate all the lints to be fixed with vim](#generate-all-the-lints-to-be-fixed-with-vim)
      * [Commit and lint workflow](#commit-and-lint-workflow)
      * [How to skip one hook](#how-to-skip-one-hook)
      * [How to skip multiple hooks](#how-to-skip-multiple-hooks)
   * [TODO](#todo)



<!--te-->

# pre-commit documentation

- The documentation is https://pre-commit.com/

- Git hooks are scripts that are run at certain points of a Git commit
  - One can use Git hooks to check for property of files (e.g., files are not
    too big, no trailing white spaces) and abort a commit in certain cases
- `pre-commit` is a tool for managing git hooks

- We use `pre-commit` to run several tests on each commit to ensure code quality

# Enable pre-commit in your repo

```bash
> pre-commit install --install-hooks
```

- This might take a bit of time, but it's an operation performed only one time
  per repo

# Workflows

## Pre-commit on staged files

- This will run all hooks against currently staged files:

  ```bash
  > pre-commit run
  ```

- This is what pre-commit runs by default when committing

## Pre-commit on all the files

- Run all the hooks against all the files
  ```bash
  > pre-commit run --all-files
  ```
- This is a useful invocation if you are using pre-commit in CI

## Pre-commit on specific files

```bash
> pre-commit run --files FILE1 FILE2
```

## Pre-commit only one hook

- Run the flake8 hook against all staged files
  ```bash
  > pre-commit run flake8
  ```

## Pre-commit all the files modified in the current branch

```bash
> pre-commit run --files $(git diff --name-only master...)
```

## Run hooks before of committing

- If you want to see a preview of running the hooks you can run

- More information are at https://pre-commit.com/#pre-commit-run

## Skip running hooks

- You

```bash
> git commit --no-verify -m "..."
```

## Generate all the lints to be fixed with vim

## Commit and lint workflow

- You can commit skipping hooks and then do a commit fixing the lints
  - This is our typical flow of running the linter after committing the original
    changes in order to avoid mixing changes and lints

```bash
> git commit --no-verify -m "..."
> FILES=$(git diff --author=$(git config user.name) --name-only HEAD HEAD~1); pre-commit run --files $FILES
> git commit --no-verify -m "..."
```

## How to skip one hook

```bash
> SKIP=p1_specific_lints git commit -am "Fix some minor issues"
```

## How to skip multiple hooks

```bash
> SKIP=CoffeeLint,RuboCop git commit -m "my commit message"
```

# TODO

- Enable pre-commit when cloning a repo
  - See
    https://pre-commit.com/#automatically-enabling-pre-commit-on-repositories
