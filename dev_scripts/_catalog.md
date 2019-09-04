- To regenerate the list
```
> (cd dev_scripts; unset CLICOLOR; \ls -1 | sort | perl -pe 's/^/\n* /') >_catalog.md
> %s/^/* /g
```

* _script_catalog.txt

* ack
- Command for fancy search, much better than `find` + `grep`
- TODO(gp): Maybe we should install with conda

* cie

* cmd_done.py

* diff_to_vimdiff.py

* ffind.py

* gcl
> git clean -fd

* gcours
> git checkout --ours $*
> git add $*

* gctheirs
> git checkout --theirs $*
> git add $*

* gdc

* gdmaster.sh

* ghi_review

* ghimy

* git_commit.py

* git_conflict_files.sh

* git_create_patch.sh

* git_diff_notebook.py

* git_files.sh
- Current git files both modified and cached

* git_hash_head.sh

* git_hooks

* git_merge.py

* git_previous_commit_files.sh
- Files modified by my previous commit

* git_revert.sh

* git_root.sh

* git_untracked_files.sh

* git_up.sh

* git_yapf.sh

* github_to_jupyter.py

* gllmy.sh
> git ll --author gp -15

* grc
> git rebase --continue

* grs
> git rebase --skip

* grsync.old

* grsync.py

* gsl
> git stash list

* gsp.py

* gup.py

* ipynb_format.py

* jackipynb
- Search in jupyter notebooks

* jackppy
- Search in both python and jupyter notebooks

* jackpy
- Search in python files 

* jacktxt
- Search in md and txt files 

* jupyter_install_extensions.sh

* jupyter_server.sh

* jupyter_show_extensions.sh

* linter.py

* mkbak

* path

* process_prof.py

* replace_text.py
- Script to do complex search and replacement in code base

* run_jupyter.sh

* script_skeleton.py

* setenv.sh

* setenv.sh.bak

* strip_ipython_magic.py

* strip_ipython_magic.sh

* svn_wrapper_vimdiff.sh

* timestamp

* tmux.sh

* unpack_path.sh

* vic

* vil

* vimgit.sh

* viw
