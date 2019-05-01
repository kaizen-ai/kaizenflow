#!/bin/bash -e

conda info -e

dir_name=$(jupyter --data-dir)
echo "Jupyter data dir: $dir_name"
ls $dir_name

if [[ 0 == 1 ]]; then
  # vim bindings.
  git clone https://github.com/lambdalisue/jupyter-vim-binding vim_binding
  # Activate the extension
  jupyter nbextension enable vim_binding/vim_binding
fi;

extensions="
autosavetime/main
code_prettify/code_prettify
collapsible_headings/main
comment-uncomment/main
contrib_nbextensions_help_item/main
execute_time/ExecuteTime
highlighter/highlighter
jupyter-js-widgets/extension
limit_output/main
notify/notify
runtools/main
toc2/main
vim_binding/vim_binding"
#nb_anacondacloud/main
#nb_conda/main
#jnbextensions_configurator/config_menu/main
#nbextensions_configurator/tree_tab/main
#nbpresent/js/nbpresent.min

for v in $extensions; do
  cmd="jupyter nbextension enable $v"
  echo "> $cmd"
  if [[ 0 == 1 ]]; then
    eval $cmd
  fi;
done;
