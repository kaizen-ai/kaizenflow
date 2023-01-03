#!/bin/bash -xe

dir_names="amp/dev_scripts/cleanup_scripts dev_scripts/cleanup_scripts"

replace_text.py \
    --old "vwap.ret_0.vol_adj_2_hat" \
    --new "vwap.ret_0.vol_adj.lead2.hat" \
    --exclude_dirs "$dir_names"
