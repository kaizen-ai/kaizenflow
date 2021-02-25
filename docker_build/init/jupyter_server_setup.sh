#!/usr/bin/env bash

echo "Setup jupyter server"

conda activate venv

conda info -e

jupyter notebook --generate-config -y
jupyter nbextension enable jupytext --py
cat << EOT >> ~/.jupyter/jupyter_notebook_config.py
#------------------------------------------------------------------------------
# Jupytext
#------------------------------------------------------------------------------
c.NotebookApp.contents_manager_class = "jupytext.TextFileContentsManager"
# Always pair ipynb notebooks to py files
c.ContentsManager.default_jupytext_formats = "ipynb,py"
# Use the percent format when saving as py
c.ContentsManager.preferred_jupytext_formats_save = "py:percent"
c.ContentsManager.outdated_text_notebook_margin = float("inf")
EOT
