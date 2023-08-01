#!/bin/bash -xe


replace_text.py \
    --old "np.float]" \
    --new "np.float64]" \
    --only_dirs "." \
    --ext "py,ipynb"

replace_text.py \
    --old "np.float:" \
    --new "np.float64:" \
    --only_dirs "." \
    --ext "py,ipynb"


replace_text.py \
    --old "np.float," \
    --new "np.float64," \
    --only_dirs "." \ 
    --ext "py,ipynb"
