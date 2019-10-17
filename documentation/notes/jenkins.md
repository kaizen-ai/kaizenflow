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

# Installing / setting up Jenkins

1) Map python3 as python
    ```bash
    sudo ln -s /usr/bin/python3 /usr/bin/python
    ```

2) Create a `.bashrc`
    ```bash
    export PATH=/anaconda3/bin:$PATH
    export PYTHONPATH=""
    ```

3) Initialize conda with `conda init bash` which adds to `.bashrc` something like:
    ```bash
    # >>> conda initialize >>>
    # !! Contents within this block are managed by 'conda init' !!
    __conda_setup="$('/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
    if [ $? -eq 0 ]; then
        eval "$__conda_setup"
    else
        if [ -f "/anaconda3/etc/profile.d/conda.sh" ]; then
            . "/anaconda3/etc/profile.d/conda.sh"
        else
            export PATH="/anaconda3/bin:$PATH"
        fi
    fi
    unset __conda_setup
    # <<< conda initialize <<<
    ```

4) The build script for Jenkins is like
    ```bash
    #!/bin/bash -xe

    amp/dev_scripts/jenkins/amp.pytest.sh
    ```

# Expose webpages

- From `https://wiki.jenkins.io/display/JENKINS/User+Content`
- https://plugins.jenkins.io/htmlpublisher
