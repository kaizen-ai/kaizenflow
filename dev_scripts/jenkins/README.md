1) Map python3 as python

```
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
