We have repos with code that we want to run in prod but needs to be encrypted

The code is under `dev_scripts/encrypt_models`

There is a script in `dev_scripts/encrypt_models/encrypt.sh` with several 
command lines

# Encrypt only

- You can encrypt models (e.g., in lemonade) to test them locally
  ```
  > export MODEL_DIR=dataflow_lemonade
  > amp/dev_scripts/encrypt_models/encrypt_model.py \
        --input_dir $MODEL_DIR/pipelines
  ```

- To encrypt and test
    ```
    > amp/dev_scripts/encrypt_models/encrypt_model.py \
        --input_dir $MODEL_DIR/pipelines \
        --test \
        --model_dag_builder "C1a_DagBuilder" \
        --model_dag_builder_file C1/C1a_pipeline.py \
        -v DEBUG \
        2>&1 | tee encrypt_model.log
  
    > ls dataflow_lemonade/encrypted_pipelines/
    C1   C11  C3  C5  C7  C9           dag_builder_helpers.py  ohlcv_feature_grnx  ohlcv_features.py  residualized_returns  volatility_adjusted_returns
    C10  C2   C4  C6  C8  __init__.py  ohlcv_feature_gnrnx     ohlcv_feature_rgnx  pytransform        test 
    ```
  
- To test manually:
  ```
  docker> python -c "import dataflow_lemonade.encrypted_pipelines.C11.C11a_pipeline as f; a = f.C11a_DagBuilder(); print(a)"
  ```

# Release from lemonade to orange

## Encrypt

To encrypt models in lemonade and release to orange
```
# From dev_scripts/encrypt_models/encrypt.sh
> export SRC_DIR=$HOME/src_vc/lemonade1
> export MODEL_DIR=dataflow_lemonade
> export DST_DIR=$HOME/src/orange1
> export MODEL_NAME=C1
> cd $SRC_DIR
> amp/dev_scripts/encrypt_models/encrypt_model.py \
    --input_dir $MODEL_DIR/pipelines \
    --output_dir $MODEL_DIR/encrypted_pipelines \
    --release_dir $DST_DIR/$MODEL_DIR/pipelines \
    -v DEBUG \
    2>&1 | tee encrypt_model.log
```

## Test

- Test that the released model works in orange
  ```
  > cd $DST_DIR
  > i docker_bash
  # Test import.
  docker> python -c "import dataflow_lemonade.pipelines_encr.C5.C5a_pipeline as f; a = f.C5a_DagBuilder(); print(a)
  # Run test.
  docker> pytest dataflow_lemonade/system/C5/test/test_C5b_tiledbacktest.py::Test_C5b_TiledBacktest::test1
  ```

# Using the cross-platform flow

This is supported only partially
```
# Encrypt models with cross-compile options.
> dev_scripts/encrypt_model.py \
    --input_dir dataflow_lemonade/pipelines \
    --build_target "linux/amd64" \
    --model_dag_builder "C5a_DagBuilder" \
    --model_dag_builder_file "C5/C5a_pipeline.py \
    -v DEBUG
```