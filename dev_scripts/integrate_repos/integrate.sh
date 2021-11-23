# Diff everything.
if [[ 1 == 1 ]]; then
    dev_scripts/diff_to_vimdiff.py --dir1 $AMP_DIR --dir2 $CMAMP_DIR
    exit 0
fi;

SUBDIR=research_amp
#SUBDIR=im
#SUBDIR=im_v2
#SUBDIR=oms/devops
#SUBDIR=dev_scripts/double_integrate
#SUBDIR=optimizer
#SUBDIR=research/cc
# Diff dir.
if [[ 1 == 1 ]]; then
    dev_scripts/diff_to_vimdiff.py --dir1 $AMP_DIR/$SUBDIR --dir2 $CMAMP_DIR/$SUBDIR
    exit 0
fi;

if [[ 1 == 1 ]]; then
    # Sync dir cmamp -> amp.
    rsync --delete -au $CMAMP_DIR/$SUBDIR/ $AMP_DIR/$SUBDIR
else
    # Sync dir amp -> cmamp.
    rsync --delete -au $AMP_DIR/$SUBDIR/ $CMAMP_DIR/$SUBDIR
fi;
