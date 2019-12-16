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

# `dev_scripts`

**_dev_scripts/\_setenv_amp.py_**

```
Generate and print a bash script that is used to configure the environment for
//amp client.

This script:
- is used to configure the environment
- should have no dependency other than basic python library
```

**_dev_scripts/ack_**

**_dev_scripts/cie_**

```
> conda info --envs
```

**_dev_scripts/cmd_done.py_**

**_dev_scripts/compress_files.sh_**

**_dev_scripts/diff_to_vimdiff.py_**

```
Transform the output of `diff -r --brief src_dir dst_dir` into a script using vimdiff.

To clean up the crap in the dirs:
> git status --ignored
> git clean -fdx --dry-run

Diff dirs:
> diff_to_vimdiff.py --src_dir /Users/saggese/src/commodity_research2/amp --dst_dir /Users/saggese/src/commodity_research3/amp
```

**_dev_scripts/ffind.py_**

```
Find all files / dirs whose name contains Task243, i.e., the regex "*Task243*"
> ffind.py Task243

Look for files / dirs with name containing "stocktwits" in "this_dir"
> ffind.py stocktwits this_dir

Look only for files.
> ffind.py stocktwits --only_files
```

**_dev_scripts/ghi_**

**_dev_scripts/ghi_my_**

**_dev_scripts/ghi_review_**

**_dev_scripts/ghi_show.py_**

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

**_dev_scripts/grsync.py_**

```
- Rsync a git dir against a pycharm deploy dir
> grsync.py --src_dir $HOME/src/particle/commodity_research --config P1 --action rsync -v DEBUG --preview

- Diff
> grsync.py --src_dir $HOME/src/particle/commodity_research --config P1 --action diff
> grsync.py --src_dir $HOME/src/particle/commodity_research --config P1 --action diff_verb
> grsync.py --src_dir $HOME/src/particle/commodity_research/tr --config P1 --action diff_verb
```

**_dev_scripts/jack_**

```
Search in all files.
```

**_dev_scripts/jackipynb_**

```
Search in .ipynb files.
```

**_dev_scripts/jackppy_**

```
Search in .py and .ipynb files.
```

**_dev_scripts/jackpy_**

```
Search in .py python (not .ipynb) files.
```

**_dev_scripts/jacktxt_**

```
Search in txt and md files.
```

**_dev_scripts/linter.py_**

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

**_dev_scripts/mkbak_**

**_dev_scripts/path_**

**_dev_scripts/print_paths.sh_**

**_dev_scripts/remove_redundant_paths.sh_**

**_dev_scripts/replace_text.py_**

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

**_dev_scripts/timestamp_**

**_dev_scripts/tmux_amp.sh_**

**_dev_scripts/url.py_**

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

**_dev_scripts/viack_**

```
Run a command, capture its output, and call vic on it.
You need to surround the command with '...'
> viack 'jackpy "config = cfg.Config.from_env()"'
```

**_dev_scripts/vic_**

**_dev_scripts/vigit_**

```
Open with vim all the files modified under git.
```

**_dev_scripts/vigitp_**

```
Open with vim all the files modified in previous commits.
```

**_dev_scripts/vil_**

**_dev_scripts/viw_**

```
Open with vi the result of `which command`.
```

# `helpers`

**_helpers/cache.py_**

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

**_helpers/user_credentials.py_**

```
Import as:

import helpers.user_credentials as usc

Test that all the credentials are properly defined.
> helpers/user_credentials.py
```

# `dev_scripts/aws`

**_dev_scripts/aws/am_aws.py_**

```
Start / stop / check AWS instance.
```

**_dev_scripts/aws/get_inst_ip.sh_**

**_dev_scripts/aws/get_inst_status.sh_**

# `dev_scripts/git`

**_dev_scripts/git/gcl_**

```
Clean the client making a backup:
> gsp.py
> git clean -fd
```

**_dev_scripts/git/gco_**

```
> git checkout $1
```

**_dev_scripts/git/gcours_**

```
Accept our changes in case of a conflict.
```

**_dev_scripts/git/gctheirs_**

```
Accept their changes in case of a conflict.
```

**_dev_scripts/git/gd_**

```
> git difftool $*
```

**_dev_scripts/git/gd_master.sh_**

```
Diff current branch against master.
```

**_dev_scripts/git/gd_notebook.py_**

```
Diff a notebook against the HEAD version in git, removing notebook artifacts
to make the differences easier to spot using vimdiff.
```

**_dev_scripts/git/gdc_**

```
> git difftool --cached $*
```

**_dev_scripts/git/gdpy_**

```
Git diff all the python files.
```

**_dev_scripts/git/git_backup.sh_**

```
Create a tarball of all the files added / modified according to git_files.sh
```

**_dev_scripts/git/git_branch.sh_**

```
Show information about the branches.
```

**_dev_scripts/git/git_branch_checkout.sh_**

```
Create a branch, check it out, and push it remotely.
```

**_dev_scripts/git/git_branch_delete_merged.sh_**

**_dev_scripts/git/git_branch_name.sh_**

```
Print name of the branch.
```

**_dev_scripts/git/git_branch_point.sh_**

```
Find the branching point.
```

**_dev_scripts/git/git_commit.py_**

```
- This script is equivalent to git commit -am "..."
- Perform various checks on the git client.
```

**_dev_scripts/git/git_conflict_files.sh_**

```
Find files with git conflicts.
```

**_dev_scripts/git/git_conflict_merge.py_**

**_dev_scripts/git/git_conflict_show.sh_**

```
Generate the files involved in a merge conflict.
```

**_dev_scripts/git/git_create_patch.sh_**

```
Create a patch file for the entire repo client from the base revision.
This script accepts a list of files to package, if specified.
```

**_dev_scripts/git/git_files.sh_**

```
Report git cached and modified files.
```

**_dev_scripts/git/git_graph.sh_**

```
Plot a graphical view of the branches, using different level of details
```

**_dev_scripts/git/git_hash_head.sh_**

```
Show the commit hash that HEAD is at.
```

**_dev_scripts/git/git_merge_branch.sh_**

```
Qualify a branch in amp and then merge it into master.
```

**_dev_scripts/git/git_merge_master.sh_**

```
Merge master in the current Git client.
```

**_dev_scripts/git/git_previous_commit_files.sh_**

```
Retrieve the files checked in by the current user in the n last commits.
```

**_dev_scripts/git/git_revert.sh_**

```
Force a revert of files to HEAD.
```

**_dev_scripts/git/git_root.sh_**

```
Report the path of the git client, e.g., /Users/saggese/src/amp
```

**_dev_scripts/git/git_submodules_are_updated.sh_**

```
Print the relevant Git hash pointers to amp.
```

**_dev_scripts/git/git_submodules_clean.sh_**

```
Clean all the submodules with `git clean -fd`.
```

**_dev_scripts/git/git_submodules_commit.sh_**

```
Commit in all the repos.
It assumes that everything has been pulled.
```

**_dev_scripts/git/git_submodules_merge_branch.sh_**

```
Qualify a branch with multiple Git repos and then merge it into master.
```

**_dev_scripts/git/git_submodules_pull.sh_**

```
Force a git pull of all the repos.
```

**_dev_scripts/git/git_submodules_roll_fwd.sh_**

```
Roll fwd the submodules.
```

**_dev_scripts/git/git_untracked_files.sh_**

**_dev_scripts/git/gll_**

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

**_dev_scripts/git/gllmy_**

```
Like gll / git ll but only referring to your commits.
#
Show my last 5 commits.
> gllmy -5
```

**_dev_scripts/git/gp_**

```
Sync client and then push local commits.
```

**_dev_scripts/git/grc_**

```
> git rebase --continue
```

**_dev_scripts/git/grs_**

```
git rebase --skip
```

**_dev_scripts/git/gs_**

```
Print the status of the clients and of the submodules with `git status`.
```

**_dev_scripts/git/gs_to_files.sh_**

```
Filter output of `git status --short`
```

**_dev_scripts/git/gsl_**

```
Print the stash list with:
> git stash list
```

**_dev_scripts/git/gsp.py_**

```
Stash the changes in a Git client without changing the client, besides a reset
of the index.
```

**_dev_scripts/git/gss_**

```
Print the status of the clients and of the submodules with
git status --short
```

**_dev_scripts/git/gup.py_**

```
Update a git client by:
- stashing
- rebasing
- reapplying the stashed changes
```

# `dev_scripts/infra`

**_dev_scripts/infra/gdrive.py_**

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

**_dev_scripts/infra/ssh_tunnels.py_**

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

**_dev_scripts/install/check_develop_packages.py_**

**_dev_scripts/install/create_conda.amp_develop.sh_**

**_dev_scripts/install/create_conda.py_**

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

**_dev_scripts/install/install_jupyter_extensions.sh_**

**_dev_scripts/install/print_conda_packages.py_**

**_dev_scripts/install/show_jupyter_extensions.sh_**

**_dev_scripts/install/test_bootstrap.sh_**

```
Test all the executables that need to bootstrap.
```

# `dev_scripts/jenkins`

**_dev_scripts/jenkins/amp.build_clean_env.amp_develop.sh_**

```
- Build "amp_develop" conda env from scratch.
```

**_dev_scripts/jenkins/amp.build_clean_env.run_fast_coverage_tests.sh_**

```
- Build conda env
- Run the fast tests with coverage
```

**_dev_scripts/jenkins/amp.build_clean_env.run_fast_tests.sh_**

```
- Build conda env
- Run the fast tests
```

**_dev_scripts/jenkins/amp.build_clean_env.run_slow_coverage_tests.sh_**

```
- Build conda env
- Run the slow tests with coverage
```

**_dev_scripts/jenkins/amp.run_fast_tests.sh_**

```
- No conda env is built, but we rely on `develop` being already build
- Run the fast tests
```

**_dev_scripts/jenkins/amp.run_parallel_fast_tests.sh_**

```
- (No conda env build)
- Run tests
  - fast
  - parallel
```

**_dev_scripts/jenkins/amp.run_pytest_collect.run_linter.sh_**

```
- Run linter on the entire tree.
```

**_dev_scripts/jenkins/amp.smoke_test.sh_**

```
- Run all the Jenkins builds locally to debug.
- To run
  > (cd $HOME/src/commodity_research1/amp; dev_scripts/jenkins/amp.smoke_test.sh 2>&1 | tee log.txt)
```

**_dev_scripts/jenkins/bisect.sh_**

# `dev_scripts/notebooks`

**_dev_scripts/notebooks/ipynb_format.py_**

**_dev_scripts/notebooks/process_all_jupytext.sh_**

```
Apply process_jupytext.py to all the ipynb files.
```

**_dev_scripts/notebooks/process_jupytext.py_**

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

**_dev_scripts/notebooks/publish_notebook.py_**

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

**_dev_scripts/notebooks/run_jupyter_server.py_**

```
Start a jupyter server.

Start a jupyter server killing the existing one:
> run_jupyter_server.py force_start

This is equivalent to:
> jupyter notebook '--ip=*' --browser chrome . --port 10001
```

**_dev_scripts/notebooks/run_notebook.py_**

```
> run_notebook.py --notebook research/Task11_Model_for_1min_futures_data/Task11_Simple_model_for_1min_futures_data.ipynb --function build_configs --no_incremental --dst_dir tmp.run_notebooks/ --num_threads -1
```

**_dev_scripts/notebooks/strip_ipython_magic.py_**

**_dev_scripts/notebooks/strip_ipython_magic.sh_**

**_dev_scripts/notebooks/test_notebook.sh_**

```
Run a notebook end-to-end and save the results into an html file, measuring
the elapsed time.
```

# `dev_scripts/testing`

**_dev_scripts/testing/count_tests.sh_**

**_dev_scripts/testing/run_tests.py_**

```
- To run the tests
> run_tests.py

- To dry run
> run_tests.py --dry_run -v DEBUG

- To run coverage
> run_tests.py --test datetime_utils_test.py --coverage -v DEBUG
```

# `documentation/scripts`

**_documentation/scripts/convert_txt_to_pandoc.py_**

```
Convert a txt file into markdown suitable for pandoc.py

E.g.,
- convert the text in pandoc / latex format
- handle banners around chapters
- handle comments
```

**_documentation/scripts/generate_latex_sty.py_**

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

**_documentation/scripts/generate_script_catalog.py_**

```
Generate a markdown file with the docstring for any script in the repo.

> generate_script_catalog.py
```

**_documentation/scripts/gh-md-toc_**

**_documentation/scripts/lint_txt.py_**

```
Used in vim to prettify a part of the text.
```

**_documentation/scripts/pandoc.py_**

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

**_documentation/scripts/publish_notes.py_**

```
Publish all notes:
> docs/scripts/publish_all_notes.py publish

Publish all the notes from scratch:
> docs/scripts/publish_notes.py ls rm publish
```

**_documentation/scripts/replace_latex.py_**

```
Replace only:
> scripts/replace_latex.py -a replace --file notes/IN_PROGRESS/finance.portfolio_theory.txt

Replace and check:
> scripts/replace_latex.py -a pandoc_before -a replace -a pandoc_after --file notes/IN_PROGRESS/finance.portfolio_theory.txt
```

**_documentation/scripts/replace_latex.sh_**

**_documentation/scripts/transform_txt.py_**

```
Perform one of several transformations on a txt file.

- The input or output can be filename or stdin (represented by '-')
- If output file is not specified then we assume that the output file is the
  same as the input

- The possible transformations are:
    1) Create table of context from the current file, with 1 level
        > transform_txt.py -a toc -i % -l 1

    2) Format the current file with 3 levels
        :!transform_txt.py -a format -i % --max_lev 3
        > transform_txt.py -a format -i notes/ABC.txt --max_lev 3
        - In vim
        :!transform_txt.py -a format -i % --max_lev 3
        :%!transform_txt.py -a format -i - --max_lev 3

    3) Increase level
        :!transform_txt.py -a increase -i %
        :%!transform_txt.py -a increase -i -
```

# `dev_scripts/git/git_hooks`

**_dev_scripts/git/git_hooks/commit-msg.py_**

**_dev_scripts/git/git_hooks/install_hooks.py_**

```
Install and remove git pre-commit hooks.

Install hooks.
> install_hooks.py --action install

Remove hooks.
> install_hooks.py --action remove

Check hook status.
> install_hooks.py --action status
```

**_dev_scripts/git/git_hooks/pre-commit.py_**

```
This is a git commit-hook which can be used to check if huge files
where accidentally added to the staging area and are about to be
committed.
If there is a file which is bigger then the given "max_file_size"-
variable, the script will exit non-zero and abort the commit.
```
