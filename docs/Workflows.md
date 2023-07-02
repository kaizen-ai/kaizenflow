# Navigate stack of failed test

- Inside the container
  ```
  > pytest_log dataflow/model/test/test_tiled_flows.py::Test_evaluate_weighted_forecasts::test_combine_two_signals
  ...
  =================================== FAILURES ===================================
  __________ Test_evaluate_weighted_forecasts.test_combine_two_signals ___________
  Traceback (most recent call last):
    File "/app/dataflow/model/test/test_tiled_flows.py", line 78, in test_combine_two_signals
      bar_metrics = dtfmotiflo.evaluate_weighted_forecasts(
    File "/app/dataflow/model/tiled_flows.py", line 265, in evaluate_weighted_forecasts
      weighted_sum = hpandas.compute_weighted_sum(
  TypeError: compute_weighted_sum() got an unexpected keyword argument 'index_mode'
  ============================= slowest 3 durations ==============================
  2.18s call     dataflow/model/test/test_tiled_flows.py::Test_evaluate_weighted_forecasts::test_combine_two_signals
  0.01s setup    dataflow/model/test/test_tiled_flows.py::Test_evaluate_weighted_forecasts::test_combine_two_signals
  0.00s teardown dataflow/model/test/test_tiled_flows.py::Test_evaluate_weighted_forecasts::test_combine_two_signals
  ```

- Outside the container
  ```
  > i traceback
  ```
