<!--ts-->
   * [dev_scripts](#dev_scripts)
   * [helpers](#helpers)
   * [dev_scripts/aws](#dev_scriptsaws)
   * [dev_scripts/git](#dev_scriptsgit)
   * [dev_scripts/infra](#dev_scriptsinfra)
   * [dev_scripts/install](#dev_scriptsinstall)
   * [dev_scripts/jenkins](#dev_scriptsjenkins)
   * [dev_scripts/notebooks](#dev_scriptsnotebooks)
   * [dev_scripts/testing](#dev_scriptstesting)
   * [documentation/scripts](#documentationscripts)
   * [dev_scripts/git/git_hooks](#dev_scriptsgitgit_hooks)



<!--te-->
# ``

`tmp_diff.sh`

# `dev_scripts`

`dev_scripts/_setenv_amp.py`

```
Generate and print a bash script that is used to configure the environment for
//amp client.

This script:
- is used to configure the environment
- should have no dependency other than basic python library
```

`dev_scripts/ack`

`dev_scripts/cie`

```
> conda info --envs
```

`dev_scripts/cmd_done.py`

`dev_scripts/compress_files.sh`

`dev_scripts/diff_to_vimdiff.py`

```
Transform the output of `diff -r --brief src_dir dst_dir` into a script using vimdiff.

To clean up the crap in the dirs:
> git status --ignored
> git clean -fdx --dry-run

Diff dirs:
> diff_to_vimdiff.py --src_dir /Users/saggese/src/commodity_research2/amp --dst_dir /Users/saggese/src/commodity_research3/amp
```

`dev_scripts/ffind.py`

```
Find all files / dirs whose name contains Task243, i.e., the regex "*Task243*"
> ffind.py Task243

Look for files / dirs with name containing "stocktwits" in "this_dir"
> ffind.py stocktwits this_dir

Look only for files.
> ffind.py stocktwits --only_files
```

`dev_scripts/generate_script_catalog.py`

```
Generate a markdown file with the docstring for any script in the repo.

> generate_script_catalog.py
```

`dev_scripts/ghi`

`dev_scripts/ghi_my`

`dev_scripts/ghi_review`

`dev_scripts/ghi_show.py`

```
Simple wrapper around ghi (GitHub Interface) to implement some typical
workflows.

Get all the data relative to issue #257:
> ghi_show.py 257
Github:
#257: ST: sources analysis
https://github.com/ParticleDev/commodity_research/issues/257

Files in the repo:
./oil/ST/Task257_Sources_analysis.py
./oil/ST/Task257_Sources_analysis.ipynb

Files in the gdrive '/Users/saggese/GoogleDriveParticle':
'/Users/saggese/GoogleDriveParticle/Tech/Task 257 - ST - Sources Analysis.gdoc'
  https://docs.google.com/open?id=1B70mA0m5UovKmuzAq05XESlKNvToflBR1uqtcYGXhhM


Get all the data relative to issue #13 for a different GitHub repo:
> ghi_show.py 13 --repo Amp
```

`dev_scripts/grsync.py`

```
- Rsync a git dir against a pycharm deploy dir
> grsync.py --src_dir $HOME/src/particle/commodity_research --config P1 --action rsync -v DEBUG --preview

- Diff
> grsync.py --src_dir $HOME/src/particle/commodity_research --config P1 --action diff
> grsync.py --src_dir $HOME/src/particle/commodity_research --config P1 --action diff_verb
> grsync.py --src_dir $HOME/src/particle/commodity_research/tr --config P1 --action diff_verb
```

`dev_scripts/jack`

```
Search in all files.
```

`dev_scripts/jackipynb`

```
Search in .ipynb files.
```

`dev_scripts/jackppy`

```
Search in .py and .ipynb files.
```

`dev_scripts/jackpy`

```
Search in .py python (not .ipynb) files.
```

`dev_scripts/jacktxt`

```
Search in txt and md files.
```

`dev_scripts/linter.py`

```
Reformat and lint python and ipynb files.

This script uses the version of the files present on the disk and not what is
staged for commit by git, thus you need to stage again after running it.

E.g.,
Lint all modified files in git client.
> linter.py

Lint current files.
> linter.py -c --collect_only
> linter.py -c --all

Lint previous commit files.
> linter.py -p --collect_only

Lint a certain number of previous commits
> linter.py -p 3 --collect_only
> linter.py --files event_study/*.py linter_v2.py --yapf --isort -v DEBUG

Lint the changes in the branch:
> linter.py -f $(git diff --name-only master...)

Lint all python files, but not the notebooks.
> linter.py -d . --only_py --collect

To jump to all the warnings to fix:
> vim -c "cfile linter.log"

Check all jupytext files.
> linter.py -d . --action sync_jupytext
```

`dev_scripts/mkbak`

`dev_scripts/path`

`dev_scripts/print_paths.sh`

`dev_scripts/remove_redundant_paths.sh`

`dev_scripts/replace_text.py`

```
Replace an instance of text in all py, ipynb, and txt files.

> replace_text.py --old "import core.finance" --new "import core.finance" --preview
> replace_text.py --old "alphamatic/kibot/All_Futures" --new "alphamatic/kibot/All_Futures" --preview

Custom flow:
> replace_text.py --custom_flow _custom1

Replace in scripts
> replace_text.py --old "exec " --new "execute " --preview --dirs dev_scripts --exts None

To revert all files but this one
> gs -s | grep -v dev_scripts/replace_text.py | grep -v "\?" | awk '{print $2}' | xargs git checkout --
```

`dev_scripts/timestamp`

`dev_scripts/tmux_amp.sh`

`dev_scripts/url.py`

```
Convert a url / path into different formats: jupyter url, github, git path.

> url.py https://github.com/ParticleDev/commodity_research/blob/master/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb
file_name=
/Users/saggese/src/particle/commodity_research/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb

github_url=
https://github.com/ParticleDev/commodity_research/blob/master/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb

jupyter_url=
http://localhost:10001/tree/oil/ST/Task229_Exploratory_analysis_of_ST_data.ipynb
```

`dev_scripts/viack`

```
Run a command, capture its output, and call vic on it.
You need to surround the command with '...'
> viack 'jackpy "config = cfg.Config.from_env()"'
```

`dev_scripts/vic`

`dev_scripts/vigit`

```
Open with vim all the files modified under git.
```

`dev_scripts/vigitp`

```
Open with vim all the files modified in previous commits.
```

`dev_scripts/vil`

`dev_scripts/viw`

```
Open with vi the result of `which command`.
```

# `helpers`

`helpers/cache.py`

```
Import as:

import helpers.cache as cache

Use as:
import functools

import helpers.cache as cache


def _read_data(*args, **kwargs):
    _LOG.info("Reading ...")
    ...
    return ...


MEMORY = cache.get_disk_cache()

@MEMORY.cache
def _read_data_from_disk(*args, **kwargs):
    _LOG.info("Reading from disk cache: %s %s", *args, **kwargs)
    data = _read_data(*args, **kwargs)
    return data


@functools.lru_cache(maxsize=None)
def read_data(*args, **kwargs):
    _LOG.info("Reading from mem cache: %s %s", *args, **kwargs)
    data = _read_data_from_disk(*args, **kwargs)
    return data
```

`helpers/user_credentials.py`

```
Import as:

import helpers.user_credentials as usc

Test that all the credentials are properly defined.
> helpers/user_credentials.py
```

# `dev_scripts/aws`

`dev_scripts/aws/am_aws.py`

```
Start / stop / check AWS instance.
```

`dev_scripts/aws/get_inst_ip.sh`

`dev_scripts/aws/get_inst_status.sh`

# `dev_scripts/git`

`dev_scripts/git/gb`

```
Print name of the branch. We don't use any wrapper so that we can use this
result.
```

`dev_scripts/git/gcl`

```
Clean the client making a backup
> gsp.py
> git clean -fd
```

`dev_scripts/git/gco`

`dev_scripts/git/gcours`

```
Accept our changes in case of a conflict.
```

`dev_scripts/git/gctheirs`

```
Accept their changes in case of a conflict.
```

`dev_scripts/git/gd`

```
> git difftool $*
```

`dev_scripts/git/gdc`

```
> git difftool --cached $*
```

`dev_scripts/git/gdmaster.sh`

```
Diff current branch with master.
```

`dev_scripts/git/gdpy`

```
Git diff all the python files.
> gd $(git_files.sh)
```

`dev_scripts/git/git_backup.sh`

```
Create a tarball of all the files added / modified according to git_files.sh
```

`dev_scripts/git/git_branch.sh`

```
Show information about the branches.
```

`dev_scripts/git/git_branching_point.sh`

```
Found the branching point.
```

`dev_scripts/git/git_checkout_branch.sh`

```
Create a branch, check it out, and push it remotely.
```

`dev_scripts/git/git_commit.py`

```
- This script is equivalent to git commit -am "..."
- Perform various checks on the git client.
```

`dev_scripts/git/git_conflict_files.sh`

```
Find files with git conflicts.
```

`dev_scripts/git/git_create_patch.sh`

```
Create a patch file for the entire repo client from the base revision.
This script accepts a list of files to package, if specified.
```

`dev_scripts/git/git_delete_merged_branches.sh`

`dev_scripts/git/git_diff_notebook.py`

```
Diff a notebook against the HEAD version in git, removing notebook artifacts
to make the differences easier to spot using vimdiff.
```

`dev_scripts/git/git_files.sh`

```
Report git cached and modified files.
```

`dev_scripts/git/git_graph.sh`

```
Plot a graphical view of the branches, using different level of details
```

`dev_scripts/git/git_hash_head.sh`

```
Show the commit hash that HEAD is at.
```

`dev_scripts/git/git_merge.py`

`dev_scripts/git/git_merge_master.sh`

```
Merge master in the current Git client.
```

`dev_scripts/git/git_previous_commit_files.sh`

```
Retrieve the files checked in by the current user in the n last commits.
```

`dev_scripts/git/git_revert.sh`

```
Force a revert of files to HEAD.
```

`dev_scripts/git/git_root.sh`

```
Report path of the git client, e.g., /Users/saggese/src/lemonade/amp
```

`dev_scripts/git/git_show_conflicting_files.sh`

```
Generate the files involved in a merge conflict.
```

`dev_scripts/git/git_submodules_are_updated.sh`

```
Print the relevant Git hash pointers to amp.
```

`dev_scripts/git/git_submodules_clean.sh`

```
Clean all the submodules with `git clean -fd`.
```

`dev_scripts/git/git_submodules_commit.sh`

```
Commit in all the repos.
It assumes that everything has been pulled.
```

`dev_scripts/git/git_submodules_pull.sh`

```
Force a git pull of all the repos.
```

`dev_scripts/git/git_submodules_roll_fwd.sh`

```
Roll fwd the submodules.
```

`dev_scripts/git/git_unstage.sh`

```
Unstage with `git reset HEAD`
```

`dev_scripts/git/git_untracked_files.sh`

`dev_scripts/git/gll`

```
List commits in a fancy full-screen format like:
#
   * 299ad8e saggese   Improve scripts for tunnelling and handling notebooks             (   4 hours ago) Sat Sep 14 11:28:29 2019  (HEAD -> master, origin/master, origin/HEAD)
   * f08f8a0 saggese   Fix bug in ssh_tunnel.py and lint                                 (   5 hours ago) Sat Sep 14 10:30:41 2019
   * 296e6ad saggese   Consolidate all the user info in helpers/user_credentials. Clean up (   6 hours ago) Sat Sep 14 10:17:03 2019
   * 39b779d saggese   Add script to start jupyter server using the right port (to tunnel through if remote) (   6 hours ago) Sat Sep 14 10:16:49 2019
   * 3df71ff paul      Lint
#
One can pass other options, e.g., -n
#
Show the last 5 commits:
> gll -5
```

`dev_scripts/git/gllmy`

```
Like gll / git ll but only referring to your commits.
#
Show my last 5 commits.
> gllmy -5
```

`dev_scripts/git/gp`

```
Sync client with gup.py and then push local commits.
```

`dev_scripts/git/grc`

```
> git rebase --continue
```

`dev_scripts/git/grs`

```
git rebase --skip
```

`dev_scripts/git/gs`

```
Print the status of the clients and of the submodules with `git status`.
```

`dev_scripts/git/gs_to_files.sh`

```
Filter output of `git status --short`
```

`dev_scripts/git/gsl`

```
Print the stash list with:
> git stash list
```

`dev_scripts/git/gsp.py`

```
Stash the changes in a Git client without changing the client, besides a reset
of the index.
```

`dev_scripts/git/gss`

```
Print the status of the clients and of the submodules with
git status --short
```

`dev_scripts/git/gup.py`

```
Update a git client by:
- stashing
- rebasing
- reapplying the stashed changes
```

# `dev_scripts/infra`

`dev_scripts/infra/gdrive.py`

```
Handle backups / export / import of Google Drive directory.

List content of a Google drive dir.
> infra/gdrive.py --action ls --src_dir gp_drive:alphamatic -v DEBUG

Backup.
> infra/gdrive.py --action backup --src_dir gp_drive:alphamatic --dst_dir gdrive_backup -v DEBUG

Test of moving data.
> infra/gdrive.py --action backup --src_dir gp_drive:alphamatic/LLC --dst_dir gdrive_backup
> infra/gdrive.py --action export --src_dir gp_drive:alphamatic/LLC --dst_dir tmp.LLC
> infra/gdrive.py --action import --src_dir tmp.LLC --dst_dir alphamatic_drive:test/LLC

Moving data.
> infra/gdrive.py --action export --src_dir gp_drive:alphamatic --dst_dir tmp.alphamatic
> infra/gdrive.py --action import --src_dir tmp.alphamatic --dst_dir alphamatic_drive:alphamatic
```

`dev_scripts/infra/ssh_tunnels.py`

```
Start all tunnels
> ssh_tunnels.py start

Stop all service tunnels
> ssh_tunnels.py stop

Report the status of each service tunnel
> ssh_tunnels.py check

Kill all the ssh tunnels on the machine, for a known service or not.
> ssh_tunnels.py kill

Starting a tunnel is equivalent to:
> ssh -i {ssh_key_path} -f -nNT -L {local_port}:localhost:{remote_port} {user_name}@{server}
> ssh -f -nNT -L 10003:localhost:10003 saggese@$P1_DEV_SERVER
```

# `dev_scripts/install`

`dev_scripts/install/check_develop_packages.py`

`dev_scripts/install/create_conda.amp_develop.sh`

`dev_scripts/install/create_conda.py`

```
This script should have *no* dependencies from anything: it should be able to run
before setenv.sh, on a new system with nothing installed. It can use //amp
libraries.

Install the `amp` default environment:
> create_conda.py --env_name amp_develop --req_file dev_scripts/install/requirements/amp_develop.yaml --delete_env_if_exists

Install the `p1_develop` default environment:
> create_conda.py --env_name p1_develop --req_file amp/dev_scripts/install/requirements/amp_develop.yaml --req_file dev_scripts_p1/install/requirements/p1_develop.yaml --delete_env_if_exists

Quick install to test the script:
> create_conda.py --test_install -v DEBUG

Test the `develop` environment with a different name before switching the old
develop env:
> create_conda.py --env_name develop_test --req_file dev_scripts/install/requirements/amp_develop.yaml --delete_env_if_exists
```

`dev_scripts/install/install_jupyter_extensions.sh`

`dev_scripts/install/print_conda_packages.py`

`dev_scripts/install/show_jupyter_extensions.sh`

`dev_scripts/install/test_bootstrap.sh`

```
Test all the executables that need to bootstrap.
```

# `dev_scripts/jenkins`

`dev_scripts/jenkins/amp.build_clean_env.amp_develop.sh`

```
- Build "amp_develop" conda env from scratch.
```

`dev_scripts/jenkins/amp.build_clean_env.run_fast_coverage_tests.sh`

```
- Build conda env
- Run the fast tests with coverage
```

`dev_scripts/jenkins/amp.build_clean_env.run_fast_tests.sh`

```
- Build conda env
- Run the fast tests
```

`dev_scripts/jenkins/amp.build_clean_env.run_slow_coverage_tests.sh`

```
- Build conda env
- Run the slow tests with coverage
```

`dev_scripts/jenkins/amp.run_fast_tests.sh`

```
- No conda env is built, but we rely on `develop` being already build
- Run the fast tests
```

`dev_scripts/jenkins/amp.run_parallel_fast_tests.sh`

```
- (No conda env build)
- Run tests
  - fast
  - parallel
```

`dev_scripts/jenkins/amp.run_pytest_collect.run_linter.sh`

```
- Run linter on the entire tree.
```

`dev_scripts/jenkins/amp.smoke_test.sh`

```
- Run all the Jenkins builds locally to debug.
- To run
  > (cd $HOME/src/commodity_research1/amp; dev_scripts/jenkins/amp.smoke_test.sh 2>&1 | tee log.txt)
```

`dev_scripts/jenkins/bisect.sh`

# `dev_scripts/notebooks`

`dev_scripts/notebooks/ipynb_format.py`

`dev_scripts/notebooks/process_all_jupytext.sh`

```
Apply process_jupytext.py to all the ipynb files.
```

`dev_scripts/notebooks/process_jupytext.py`

```
Automate some common workflows with jupytext.

> find . -name "*.ipynb" | grep -v ipynb_checkpoints | head -3 | xargs -t -L 1 process_jupytext.py --action sync --file

Pair
> process_jupytext.py -f vendors/kibot/data_exploratory_analysis.ipynb --action pair

Test
> process_jupytext.py -f vendors/kibot/data_exploratory_analysis.ipynb --action test
> process_jupytext.py -f vendors/kibot/data_exploratory_analysis.ipynb --action test_strict

Sync
> process_jupytext.py -f vendors/kibot/data_exploratory_analysis.ipynb --action sync
```

`dev_scripts/notebooks/publish_notebook.py`

```
Given a notebook specified as:
- a ipynb file, e.g.,
    data/web_path_two/Minute_distribution_20180802_182656.ipynb
- a jupyter url, e.g.,
    https://github.com/...ipynb
- a github url

- Backup a notebook and publish notebook on shared space;
> publish_notebook.py --file xyz.ipynb --action publish

- Open a notebook in Chrome
> publish_notebook.py --file xyz.ipynb --action open
```

`dev_scripts/notebooks/run_jupyter_server.py`

```
Start a jupyter server.

Start a jupyter server killing the existing one:
> run_jupyter_server.py force_start

This is equivalent to:
> jupyter notebook '--ip=*' --browser chrome . --port 10001
```

`dev_scripts/notebooks/run_notebook.py`

```
> run_notebook.py --notebook research/Task11_Model_for_1min_futures_data/Task11_Simple_model_for_1min_futures_data.ipynb --function build_configs --no_incremental --dst_dir tmp.run_notebooks/ --num_threads -1
```

`dev_scripts/notebooks/strip_ipython_magic.py`

`dev_scripts/notebooks/strip_ipython_magic.sh`

`dev_scripts/notebooks/test_notebook.sh`

```
Run a notebook end-to-end and save the results into an html file, measuring
the elapsed time.
```

# `dev_scripts/testing`

`dev_scripts/testing/count_tests.sh`

`dev_scripts/testing/run_tests.py`

```
- To run the tests
> run_tests.py

- To dry run
> run_tests.py --dry_run -v DEBUG

- To run coverage
> run_tests.py --test datetime_utils_test.py --coverage -v DEBUG
```

# `documentation/scripts`

`documentation/scripts/generate_latex_sty.py`

```

    data += r"""
\newcommand{\AAA}{\mat{A}}
\newcommand{\BB}{\mat{B}}
\newcommand{\CC}{\mat{C}}
\newcommand{\II}{\mat{I}}
\newcommand{\FF}{\mat{F}}
\newcommand{\LL}{\mat{L}}
\newcommand{\MM}{\mat{M}}
\newcommand{\NN}{\mat{N}}
\newcommand{\PP}{\mat{P}}
\newcommand{\QQ}{\mat{Q}}
\newcommand{\RR}{\mat{R}}
\newcommand{\SSS}{\mat{S}}
\newcommand{\SSigma}{\mat{\Sigma}}
\newcommand{\UU}{\mat{U}}
\newcommand{\VVV}{\mat{V}}
\newcommand{\XX}{\mat{X}}
\newcommand{\ZZ}{\mat{Z}}
\newcommand{\WW}{\mat{W}}
```

`documentation/scripts/gh-md-toc`

`documentation/scripts/pandoc.py`

```
Convert a txt file into a PDF / HTML using pandoc.

From scratch with TOC:
> pandoc.py -a pdf --input ...

For interactive mode:
> pandoc.py -a pdf --no_cleanup_before --no_cleanup --input ...

Check that can be compiled:
> pandoc.py -a pdf --no_toc --no_open_pdf --input ...

> pandoc.py --input notes/IN_PROGRESS/math.The_hundred_page_ML_book.Burkov.2019.txt -a pdf --no_cleanup --no_cleanup_before --no_run_latex_again --no_open
```

`documentation/scripts/preprocess_md_for_pandoc.py`

```
Convert a txt file into markdown suitable for pandoc.py

E.g.,
- convert the text in some nice pandoc / latex format
- handle banners around chapters
- handle comments
```

`documentation/scripts/process_md.py`

```
Perform several kind of transformation on a txt file

- The input or output can be filename or stdin (represented by '-')
- If output file is not specified then the same input is assumed

1) Create table of context from the current file, with 1 level
> process_md.py -a toc -i % -l 1

2) Format the current file with 3 levels
:!process_md.py -a format -i --max_lev 3
> process_md.py -a format -i notes/ABC.txt --max_lev 3

- In vim
:!process_md.py -a format -i % --max_lev 3
:%!process_md.py -a format -i - --max_lev 3

3) Increase level
:!process_md.py -a increase -i %
:%!process_md.py -a increase -i -
```

`documentation/scripts/publish_notes.py`

```
Publish all notes:
> docs/scripts/publish_all_notes.py publish

Publish all the notes from scratch:
> docs/scripts/publish_notes.py ls rm publish
```

`documentation/scripts/replace_latex.py`

```
Replace only:
> scripts/replace_latex.py -a replace --file notes/IN_PROGRESS/finance.portfolio_theory.txt

Replace and check:
> scripts/replace_latex.py -a pandoc_before -a replace -a pandoc_after --file notes/IN_PROGRESS/finance.portfolio_theory.txt
```

`documentation/scripts/replace_latex.sh`

# `dev_scripts/git/git_hooks`

`dev_scripts/git/git_hooks/commit-msg.py`

`dev_scripts/git/git_hooks/install_hooks.py`

```
Install and remove git pre-commit hooks.

Install hooks.
> install_hooks.py --action install

Remove hooks.
> install_hooks.py --action remove

Check hook status.
> install_hooks.py --action status
```

`dev_scripts/git/git_hooks/pre-commit.py`

```
This is a git commit-hook which can be used to check if huge files
where accidentally added to the staging area and are about to be
committed.
If there is a file which is bigger then the given "max_file_size"-
variable, the script will exit non-zero and abort the commit.
```
