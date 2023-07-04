#!/bin/bash -xe
# "Use the run_config_list script to run historical simulations" CmTask #4609.
# TODO(Dan): Fix dry run.
# OPTS="$OPTS --dry_run"
OPTS="$OPTS $*"

export ACTION="run_all_configs"
export DAG_BUILDER_CTOR_AS_STR="dataflow_orange.pipelines.C3.C3a_pipeline_tmp.C3a_DagBuilder_tmp"
export BACKTEST_CONFIG="ccxt_v7_1-all.5T.2022-06-01_2022-06-02"
export TRAIN_TEST_MODE="ins"
export TAG="run0"

# Specific of models that requires learning.
# export FIT_AT_BEGINNING=False

# Specific of out-of-sample prediction.
# export OOS_START_DATE_AS_STR="2022-09-01"

# This is needed when one wants to run a single config.
# export CONFIG_IDX=0

# Run.
/app/amp/dataflow_amp/system/Cx/run_Cx_historical_simulation.py \
    --action $ACTION \
    --dag_builder_ctor_as_str $DAG_BUILDER_CTOR_AS_STR\
    --backtest_config $BACKTEST_CONFIG \
    --train_test_mode $TRAIN_TEST_MODE \
    --tag $TAG \
    # --fit_at_beginning $FIT_AT_BEGINNING \
    # --oos_start_date_as_str $OOS_START_DATE_AS_STR \
    # --config_idx $CONFIG_IDX \
    $OPTS 2>&1
