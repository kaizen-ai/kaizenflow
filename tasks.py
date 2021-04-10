from invoke import task

def _execute(ctx, cmds):
    for cmd in cmds:
        ctx.run(cmd)


@task
def git_pull(ctx):
    """
    Pull all the repos.
    """
    cmd = "git pull --autostash"
    ctx.run(cmd)
    cmd = "git submodule foreach 'git pull --autostash'"
    ctx.run(cmd)


@task
def git_pull_master(ctx):
    """
    Pull master without changing branch.
    """
    cmd = "git fetch origin master:master"
    ctx.run(cmd)


@task
def git_clean(ctx):
    """
    Clean all the repos.
    """
    # TODO(*): Add "are you sure?" or a `--force switch` to avoid to cancel by
    #  mistake.
    cmds = []
    cmd = "git clean -fd"
    cmd.append(cmd)
    cmd = "git submodule foreach 'git clean -fd'"
    cmds.append(cmd)
    cmd = """find . | \
    grep -E "(tmp.joblib.unittest.cache|.pytest_cache|.mypy_cache|.ipynb_checkpoints|__pycache__|\.pyc|\.pyo$$)" | \
    xargs rm -rf"""
    cmds.append(cmd)
    _execute(ctx, cmds)