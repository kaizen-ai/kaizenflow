# pytest amp/dataflow/pipelines/examples/test/test_example1_tiledbacktest.py::Test_Example1_ForecastSystem::test1 -s --dbg --update_outcomes

if [[ 1 == 0 ]]; then
/app/dataflow/backtest/run_config_list.py \
    --experiment_builder dataflow.backtest.master_backtest.run_tiled_backtest \
    --config_builder 'dataflow.pipelines.example1.example1_configs.build_tile_configs("example1_v1-top2.1T.Jan2000")' \
    --dst_dir /app/dataflow/pipelines/examples/test/outcomes/Test_Example1_ForecastSystem.test1/tmp.scratch/run_model \
    -v DEBUG --clean_dst_dir --no_confirm --num_threads serial
# --dry_run
fi;

#/app/amp/dataflow/backtest/run_config_stub.py --experiment_builder 'amp.dataflow.backtest.master_backtest.run_tiled_backtest' --config_builder 'dataflow.pipelines.examples.example1_configs.build_tile_configs("kibot_v1-top1.5T.2020-01-01_2020-03-01")' --config_idx 0 --dst_dir /app/amp/dataflow/pipelines/examples/test/outcomes/Test_Example1_ForecastSystem.test1/tmp.scratch/run_model -v DEBUG

#/app/dataflow/backtest/run_config_stub.py --experiment_builder 'dataflow.backtest.master_backtest.run_tiled_backtest' --config_builder 'dataflow.pipelines.examples.example1_configs.build_tile_configs("example1_v1-top2.1T.Jan2000")' --config_idx 0 --dst_dir /app/dataflow/pipelines/examples/test/outcomes/Test_Example1_ForecastSystem.test1/tmp.scratch/run_model -v INFO 2>&1 | tee log.txt
