# pytest amp/dataflow/pipelines/examples/test/test_example1_tiledbacktest.py::Test_Example1_TiledBacktest::test1 -s --dbg --update_outcomes

#/app/amp/dataflow/model/run_experiment.py --experiment_builder amp.dataflow.model.master_experiment.run_tiled_experiment --config_builder 'dataflow.pipelines.examples.example1_configs.build_tile_configs("kibot_v1-top1.5T.JanFeb2020")' --dst_dir /app/amp/dataflow/pipelines/examples/test/outcomes/Test_Example1_TiledBacktest.test1/tmp.scratch/run_model -v DEBUG --clean_dst_dir --no_confirm --num_threads serial

/app/amp/dataflow/model/run_experiment_stub.py --experiment_builder 'amp.dataflow.model.master_experiment.run_tiled_experiment' --config_builder 'dataflow.pipelines.examples.example1_configs.build_tile_configs("kibot_v1-top1.5T.JanFeb2020")' --config_idx 0 --dst_dir /app/amp/dataflow/pipelines/examples/test/outcomes/Test_Example1_TiledBacktest.test1/tmp.scratch/run_model -v DEBUG
