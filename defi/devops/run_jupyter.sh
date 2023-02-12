#!/bin/bash -xe
jupyter-notebook --port=8888 --no-browser --ip=* --NotebookApp.token='' --NotebookApp.password='' --allow-root
