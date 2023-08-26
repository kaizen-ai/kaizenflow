#!/bin/bash -xe
# TODO(Grisha): call Python code directly instead of using a shell wrapper, see
# CmTask4724 "Unwrap historical simulation script".
# OPTS="$OPTS --clean_dst_dir --no_confirm"
OPTS="$OPTS --num_threads serial"
# OPTS="$OPTS --skip_on_error"
# OPTS="$OPTS --dry_run"
OPTS="$OPTS $*"

# Train test modes. 
export TRAIN_TEST_MODE="ins"
# export TRAIN_TEST_MODE="ins_oos"
# export TRAIN_TEST_MODE="rolling"

# Experement builders.
# TODO(Grisha): this should become a function of `TRAIN_TEST_MODE`.
export EXPERIMENT_BUILDER="amp.dataflow.backtest.master_backtest.run_in_sample_tiled_backtest"
# export EXPERIMENT_BUILDER="amp.dataflow.backtest.master_backtest.run_ins_oos_tiled_backtest"
# export EXPERIMENT_BUILDER="amp.dataflow.backtest.master_backtest.run_rolling_tiled_backtest"

# TODO(Grisha): this should become a function of `DAG_BUILDER_CTOR_AS_STR`.
export DAG_BUILDER_NAME="C3a"
export DAG_BUILDER_CTOR_AS_STR="dataflow_orange.pipelines.C3.C3a_pipeline_tmp.C3a_DagBuilder_tmp"
export BACKTEST_CONFIG="ccxt_v7_1-all.5T.2022-06-01_2022-06-02"
export TAG="run0"
# Sometimes we want to set `DST_DIR` outside the current script, e.g., for the corresponding unit test.
# Uncomment when running a simulation.
# export DST_DIR="build_tile_configs.$DAG_BUILDER_NAME.$BACKTEST_CONFIG.$TRAIN_TEST_MODE.$TAG"

# Specific of models that requires learning.
export FIT_AT_BEGINNING=None

# Specific of out-of-sample prediction.
# export OOS_START_DATE_AS_STR="2022-09-01"

export CONFIG_BUILDER="amp.dataflow_amp.system.Cx.Cx_tile_config_builders.get_Cx_config_builder_for_historical_simulations(\"$DAG_BUILDER_CTOR_AS_STR\",$FIT_AT_BEGINNING,train_test_mode=\"$TRAIN_TEST_MODE\",backtest_config=\"$BACKTEST_CONFIG\")"
# export CONFIG_BUILDER="amp.dataflow_amp.system.Cx.Cx_tile_config_builders.get_Cx_config_builder_for_historical_simulations(\"$DAG_BUILDER_CTOR_AS_STR\",$FIT_AT_BEGINNING,train_test_mode=\"$TRAIN_TEST_MODE\",backtest_config=\"$BACKTEST_CONFIG\",oos_start_date_as_str=\"$OOS_START_DATE_AS_STR\")"

# Run.
/app/amp/dataflow/backtest/run_config_list.py \
    --experiment_builder $EXPERIMENT_BUILDER \
    --dst_dir $DST_DIR \
    --config_builder $CONFIG_BUILDER \
    $OPTS 2>&1

# Command line to run the pipeline for a single config. Used only for debugging.
# /app/amp/dataflow/backtest/run_config_stub.py --experiment_builder $EXPERIMENT_BUILDER --config_builder $CONFIG_BUILDER --config_idx 0 --dst_dir $DST_DIR | tee tmp.log.run_config_stub.txt
