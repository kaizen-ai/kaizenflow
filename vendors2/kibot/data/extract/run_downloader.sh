#!/bin/bash -xe
vendors2/kibot/data/extract/download.py \
    --dataset 'sp_500_daily' \
    -u saggese@gmail.com -p s33f3c3c3 \
    --dry_run \
    --no_incremental

# sp_500_tickbidask
# sp_500_unadjusted_tickbidask
# sp_500_1min
# sp_500_unadjusted_1min
# sp_500_daily
# sp_500_unadjusted_daily
