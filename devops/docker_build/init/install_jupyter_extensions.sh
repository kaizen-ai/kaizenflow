#!/usr/bin/env bash

set -e

echo "Install jupyter extensions"

conda activate venv

conda info -e

DIR_NAME=$(jupyter --data-dir)
echo "Jupyter data dir: $DIR_NAME"
if [[ ! -d $DIR_NAME ]]; then
  mkdir -p $DIR_NAME
fi;

if [[ 0 == 1 ]]; then
  VIM_DIR=$DIR_NAME/nbextensions/vim_binding
  if [[ -d $VIM_DIR ]]; then
      rm -rf $VIM_DIR
  fi;
  mkdir -p $VIM_DIR
  cd $VIM_DIR
  # vim bindings.
  git clone https://github.com/lambdalisue/jupyter-vim-binding vim_binding
  # Activate the extension.
  jupyter nbextension enable vim_binding/vim_binding
  exit
fi;

extensions="
vim_binding/vim_binding
autosavetime/main
code_prettify/code_prettify
collapsible_headings/main
comment-uncomment/main
contrib_nbextensions_help_item/main
execute_time/ExecuteTime
highlighter/highlighter
jupyter-js-widgets/extension
notify/notify
runtools/main
toc2/main
spellchecker/main"

for v in $extensions; do
  cmd="jupyter nbextension enable $v"
  echo "> $cmd"
  if [[ 1 == 1 ]]; then
    eval $cmd
  fi;
done;
eval "jupyter nbextension disable vim_binding/vim_binding"

# Disable configuration for nbextensions without explicit compatibility
echo "{\"nbext_hide_incompat\": false}" > /root/.jupyter/nbconfig/common.json
