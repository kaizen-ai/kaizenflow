# `dev` build

## Goal

- This is useful to test dangerous code as Jenkins (e.g., that might have
  dependencies from our env) before committing to `master`

## Setup

- Create a `dev` branch in `amp` / `p1` repo

## Running a dev build

- Merge `master` to `dev`
```bash
> git checkout dev
> git merge master
```

- `dev` should have no difference with `master`
```bash
> git ll master..dev
> git ll dev..master
```

- Merge your code from the branch into `dev`
```
> git checkout dev
> git merge PartTask354_INFRA_Populate_S3_bucket
```

- Trigger a Jenkins build in dev to see if it passes

- Optional review

- Merge `dev` into `master`
