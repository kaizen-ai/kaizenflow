<!--ts-->
   * [pre-commit documentation](#pre-commit-documentation)
   * [Enable pre-commit in your repo](#enable-pre-commit-in-your-repo)
   * [Run hooks before of committing](#run-hooks-before-of-committing)
   * [Skip running hooks](#skip-running-hooks)
   * [Generate all the lints to be fixed with vim](#generate-all-the-lints-to-be-fixed-with-vim)
   * [Workflows](#workflows)
      * [How to skip one hook](#how-to-skip-one-hook)
      * [How to skip multiple hooks](#how-to-skip-multiple-hooks)
   * [TODO](#todo)



<!--te-->


# pre-commit documentation

- The documentation is https://pre-commit.com/

- Git hooks are script that are run at certain point of a Git commit
  - One can use Git hooks to check for certain property of files (e.g., files
    are not too big, no trailing white spaces) and abort a commit in certain
    cases
- `pre-commit` is a tool for managing git hooks

- We use `pre-commit` to run several tests on each commit to ensure quality
- The current list of hooks are:
  - ...

# Enable pre-commit in your repo

```bash
> pre-commit install --install-hooks
```

- This might take a bit of time, but it's a one time per repo operation

# Run hooks before of committing

- If you want to see a preview of running the hooks you can run

- Https://pre-commit.com/#pre-commit-run

# Skip running hooks

- You

```bash
> git commit --no-verify -m "..."
```

# Generate all the lints to be fixed with vim

# Workflows

- You can commit skipping hooks and then do a commit fixing the lints

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
