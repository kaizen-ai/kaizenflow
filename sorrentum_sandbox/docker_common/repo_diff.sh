#!/bin/bash -xe

ROOT_SRC_DIR=$HOME/src/sorrentum1
#DST_SUBDIR="umd_data605_1"
DST_SUBDIR="umd_icorps"
#DST_SUBDIR="umd_defi"

# Diff `docker_common` dir.
#SRC_DIR=$ROOT_SRC_DIR/sorrentum_sandbox/docker_common; DST_DIR=$HOME/src/$DST_SUBDIR/docker_common; diff_to_vimdiff.py --dir1 $SRC_DIR --dir2 $DST_DIR

# Diff `devops` dir.
#SRC_DIR=$ROOT_SRC_DIR/sorrentum_sandbox/devops; DST_DIR=$HOME/src/$DST_SUBDIR/project/icorps; diff_to_vimdiff.py --dir1 $SRC_DIR --dir2 $DST_DIR

# Diff `devops` dir.
SRC_DIR=$ROOT_SRC_DIR/defi/devops; DST_DIR=$HOME/src/$DST_SUBDIR/project/icorps; diff_to_vimdiff.py --dir1 $SRC_DIR --dir2 $DST_DIR
