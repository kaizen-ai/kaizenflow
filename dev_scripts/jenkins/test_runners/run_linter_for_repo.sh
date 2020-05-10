#!/bin/bash -xe

# Run (ignoring the rc).
# Because of #713.
OPTS="--skip_files core/notebooks/gallery_timeseries_study.py"
# Ignore because of #1546
OPTS="${OPTS} --skip_files dev_scripts_p1/infra/test.txt"
CMD="linter.py -d . $OPTS --jenkins --num_threads serial"
execute $CMD
