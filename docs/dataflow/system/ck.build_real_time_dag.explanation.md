# Building Real-time Dag Explanation

<!-- toc -->

- [Real-time DAG](#real-time-dag)
- [Components involved](#components-involved)
- [Simulating a real-time DAG system](#simulating-a-real-time-dag-system)
- [Running a DAG system in real-time](#running-a-dag-system-in-real-time)

<!-- tocstop -->

# Real-time DAG

A common use case is to run a DAG in real time, i.e. as soon as the data is
available we evaluate the DAG.

We can run:

- In continuous mode (i.e., the DAG is continuously looping on the available
  data)
  ```
  while True:
    execute_dag(latest_data_df)
  ```

- In timed mode there is a clock, (e.g., every second) that triggers an
  evaluation
  ```
  while True:
    wait_until_next_clock()
    execute_dag(df_since_last_clock)
  ```
  - In this case one should guarantee that the DAG executes in less time than
    the clock period

# Components involved

Use synthetic data to feed the DAG system for a simplicity.

- Let's assume that data spans two interval 9:30am to 11:30am and has the second
  resolution covering 3 assets with ids 101, 102, 103

```python
# Generate synthetic OHLCV data.
tz = "America/New_York"
start_datetime = pd.Timestamp("2023-01-03 09:30:00", tz=tz)
end_datetime = pd.Timestamp("2023-01-03 11:30:00", tz=tz)
asset_ids = [101, 201, 301]
bar_duration = "1S"
#
random_ohlcv_bars = cofinanc.generate_random_ohlcv_bars(
    start_datetime,
    end_datetime,
    asset_ids,
    bar_duration=bar_duration,
)
```

- The data is in long form:
  - `start_datetime` represents the start timestamp of an event
  - `end_datetime` represents the end timestamp
  - The interval is [a, b)
  - `timestamp_db` represents the `knowledge_timestamp`, i.e. when the data has
    become available
  - For the demonstration purposes, we assume that the data arrives immediately
    (i.e., with no delay)
```
             start_datetime              end_datetime              timestamp_db         open         high          low        close       volume  asset_id
0 2023-01-03 09:35:00-05:00 2023-01-03 09:35:01-05:00 2023-01-03 09:35:01-05:00   999.874540   999.874540   999.874540   999.874540   999.874540       101
0 2023-01-03 09:35:00-05:00 2023-01-03 09:35:01-05:00 2023-01-03 09:35:01-05:00   999.874540   999.874540   999.874540   999.874540   999.874540       201
0 2023-01-03 09:35:00-05:00 2023-01-03 09:35:01-05:00 2023-01-03 09:35:01-05:00   999.874540   999.874540   999.874540   999.874540   999.874540       301
1 2023-01-03 09:35:01-05:00 2023-01-03 09:35:02-05:00 2023-01-03 09:35:02-05:00  1000.325254  1000.325254  1000.325254  1000.325254  1000.325254       101
1 2023-01-03 09:35:01-05:00 2023-01-03 09:35:02-05:00 2023-01-03 09:35:02-05:00  1000.325254  1000.325254  1000.325254  1000.325254  1000.325254       201
1 2023-01-03 09:35:01-05:00 2023-01-03 09:35:02-05:00 2023-01-03 09:35:02-05:00  1000.325254  1000.325254  1000.325254  1000.325254  1000.325254       301
...
```

- DAG systems have a concept of `event_loop` which is equivalent to a wall-clock
  - An `event_loop` is a Python `asyncio` construct to coordinate activities
    between coroutines
  - E.g., one can wait a certain number of seconds with `await asyncio.sleep(5)`

- We implemented a way to simulate an `event_loop` so that the passage of time
  doesn't correspond to actual seconds but it is "event-based"
  - E.g., a wait on a simulated `event_loop` doesn't need to wait N seconds but
    it's instantenous, although the bookkeeping of time is done properly
  - This is a way to simulate systems with accurate timing semantic without
    having actually to wait for time to pass

- Create a `ReplayedMarketData` to simulate the real-behavior of a data source
  ```
  with hasynci.solipsism_context() as event_loop:
      knowledge_datetime_col_name = "timestamp_db"
      data_start_timestamp = market_data["end_datetime"].min()
      timedelta = pd.Timedelta("1S")
      replayed_delay_in_mins_or_timestamp = data_start_timestamp + timedelta
      replayed_market_data, get_wall_clock_time = mdmadaex.get_ReplayedTimeMarketData_from_df(
          event_loop,
          replayed_delay_in_mins_or_timestamp,
          market_data
      )
  ```
  - The `ReplayedMarketData` replays the input data, i.e. certain rows become
    available only at a certain time
    - `replayed_delay_in_mins_or_timestamp` defines when the clock starts
      - In this case the clock starts 1 second later than the first event
        `end_datetime`
      - This is needed to pull the first row which `end_datetime` is `09:35:01`
      - This row becomes available only at `09:35:02` because we query like so
        [a, b)
      - E.g., if the clock starts earlier, at `09:35:01`, the first row
        `09:35:02` won't be available, since there is no data in the interval
        `[09:35:00, 09:35:01)`, the data starts at `09:35:01`
  - This component allows to replay the data in the same way that a real data
    source would do (e.g., the visible data depends on the clock)
  - It has a connection to the clock to know what data is available based on the
    knowledge timestamp
  - Note that a `ReplayedMarketData` is not a "simulated" component, it has the
    correct real-time behavior (in other words it relies on the simulated
    clock/event_loop to being simulated)

- You can put a `ReplayedMarketData` into a `DataSource` and run the data
  through a DAG
  - You can simulate an historical simulation with this set-up but it won't be
    using asyncio to synchronize like in the real-world
  - The historical simulation can be in one shot (i.e., all the data is
    processed) or bar-by-bar (with a clock advancing and the DAG computing the
    data one timed-chunk at the time)

- In general we can run a historical simulation vs a real-time execution
  - Historical simulation, `DataSource`, no `asyncio` wait
    - Non timed (i.e., no clock), but data can be fed in chunks
    - This is not event-based, it's "cycle based" (it's similar to tick-by-tick
      vs bar-by-bar), it's like continuous-time vs discrete-time
  - Real-time execution, `RealTimeDataSource`, `asyncio` wait
  - Simulated real-time execution, `RealTimeDataSource`, `asyncio` wait +
    solipsistic `event_loop`

- A market data piece can be connected to a DAG using a `RealTimeDataSource`
  node
  ```
  timedelta = pd.Timedelta("1S")
  knowledge_datetime_col_name = "timestamp_db"
  multiindex_output = True
  replayed_market_data_node = dtfsysonod.RealTimeDataSource(
      "replayed_market_data",
      replayed_market_data,
      timedelta,
      knowledge_datetime_col_name,
      multiindex_output,
  )
  ```
  - `timedelta` defines how much history to load (e.g., 1 second)
    - The interval is `[wall_clock_time - timedelta, wall_clock_time)`
  - `knowledge_datetime_col_name` defines which column to use to know if data is
    available or not

- Create a `DAG` object
  ```
  dag_builder = dtfapredoredop.Realtime_etl_DataObserver_DagBuilder()
  dag_config = dag_builder.get_config_template()
  dag = dag_builder.get_dag(dag_config)
  dag.insert_at_head(replayed_market_data_node)
  ```
  - The `DAG` is configured by the `DagBuilder` object which defines:
    - The list of nodes to run
    - Nodes configuration
    - Order of execution
  - We connect `RealTimeDataSource` to the `DAG` so that it also is able to pull
    the input data

- Create a `RealTimeDagRunner`
  ```
  # 6) Build a `RealTimeDagRunner`.
  bar_duration_in_secs = 1
  # Run the loop for 10 seconds.
  rt_timeout_in_secs_or_time = 10 * bar_duration_in_secs
  execute_rt_loop_kwargs = {
      # Should be the same clock as in `ReplayedMarketData`.
      "get_wall_clock_time": get_wall_clock_time,
      "bar_duration_in_secs": bar_duration_in_secs,
      "rt_timeout_in_secs_or_time": rt_timeout_in_secs_or_time,
  }
  dag_runner_kwargs = {
      "dag": dag,
      "fit_state": None,
      "execute_rt_loop_kwargs": execute_rt_loop_kwargs,
      "dst_dir": None,
      "get_wall_clock_time": get_wall_clock_time,
      "bar_duration_in_secs": bar_duration_in_secs,
      "set_current_bar_timestamp": True,
  }
  # 7) Run the `DAG` using a simulated clock.
  dag_runner = dtfsrtdaru.RealTimeDagRunner(**dag_runner_kwargs)
  result_bundles = hasynci.run(dag_runner.predict(), event_loop=event_loop)
  ```
  - Essentially the `DagRunner` defines how to run the `DAG`:
    - `bar_duration_in_secs` in this case defines how often to run the DAG
    - `rt_timeout_in_secs_or_time` defines for how long to run the `DAG`
    - `get_wall_clock_time` is the simulated clock which must be the same as in
      `MarketData` object

# Simulating a real-time DAG system

The section explains the behavior of
`dataflow_amp/system/realtime_etl_data_observer/scripts/run_realtime_etl_data_observer.py`.

The first event is `[09:35:00, 09:35:01)` and it is available at `09:35:01`.
```
                 start_datetime              end_datetime              timestamp_db       open       high        low      close     volume  asset_id
    0 2023-01-03 09:35:00-05:00 2023-01-03 09:35:01-05:00 2023-01-03 09:35:01-05:00  999.87454  999.87454  999.87454  999.87454  999.87454       101
    0 2023-01-03 09:35:00-05:00 2023-01-03 09:35:01-05:00 2023-01-03 09:35:01-05:00  999.87454  999.87454  999.87454  999.87454  999.87454       201
    0 2023-01-03 09:35:00-05:00 2023-01-03 09:35:01-05:00 2023-01-03 09:35:01-05:00  999.87454  999.87454  999.87454  999.87454  999.87454       301
```

The system will run for 10 seconds (i.e. ten one-second iterations).

The clock starts at `wall_clock_time=2023-01-03 09:35:02-05:00` and the system
tries to align on a grid. Since it is a one-second grid, we are already on a
grid, no need to align.
```
16:58:08 Task-1 hasyncio.py _wait_until:430                  wait_until_timestamp=2023-01-03 09:35:02-05:00, curr_timestamp=2023-01-03 09:35:02-05:00
16:58:08 Task-1 hasyncio.py _wait_until:446                  _align_on_grid: wall_clock_time=2023-01-03 09:35:02-05:00: sleep for 0.0 secs
```

Then the system starts the first iteration:
```
# Real-time loop: num_it=1: rt_time_out_in_secs=10 wall_clock_time='2023-01-03 09:35:02-05:00' real_wall_clock_time='2023-12-08 16:58:08.765220-05:00'
```

The system checks if the data is available, since there is no data delay,
waiting is done in no time.
```
### waiting on last bar: num_iter=0/120: current_bar_timestamp=2023-01-03 09:35:02-05:00 wall_clock_time=2023-01-03 09:35:02-05:00 last_db_end_time=2023-01-03 09:35:02-05:00
16:58:08 Task-3 hprint.py log_frame:604
```

Once the data is available the system pulls it to compute the DAG further. At
`09:35:02` the system queries for `[09:35:01, 09:35:02)` and as a result we
receive the rows with `end_datetime=09:35:01`.
```
                                     start_datetime              timestamp_db       open       high        low      close     volume  asset_id
end_datetime
2023-01-03 09:35:01-05:00 2023-01-03 09:35:00-05:00 2023-01-03 09:35:01-05:00  999.87454  999.87454  999.87454  999.87454  999.87454       101
2023-01-03 09:35:01-05:00 2023-01-03 09:35:00-05:00 2023-01-03 09:35:01-05:00  999.87454  999.87454  999.87454  999.87454  999.87454       201
2023-01-03 09:35:01-05:00 2023-01-03 09:35:00-05:00 2023-01-03 09:35:01-05:00  999.87454  999.87454  999.87454  999.87454  999.87454       301
```

Once data is pulled the system computes the DAG nodes one-by-one in the order
defined by the DAG. The output of each node is an input for the next one.
```
################################################################################
Executing method 'predict' for node topological_id=1 nid='filter_ath' ...
################################################################################

################################################################################
Executing method 'predict' for node topological_id=2 nid='resample' ...
################################################################################

################################################################################
Executing method 'predict' for node topological_id=3 nid='compute_feature' ...
################################################################################
```

Once all the nodes are computed, the cycle repeats. The only difference is that
the `wall_clock_time` has moved, it is `09:35:03` now which affects the input
data we receive.
```
# Real-time loop: num_it=2: rt_time_out_in_secs=10 wall_clock_time='2023-01-03 09:35:03-05:00' real_wall_clock_time='2023-12-08 16:58:09.117236-05:00'
```

At `09:35:03` the system queries for `[09:35:02, 09:35:03)` and as a result we
receive the rows with `end_datetime=09:35:02`
```
             start_datetime              end_datetime              timestamp_db         open         high          low        close       volume  asset_id
1 2023-01-03 09:35:01-05:00 2023-01-03 09:35:02-05:00 2023-01-03 09:35:02-05:00  1000.325254  1000.325254  1000.325254  1000.325254  1000.325254       101
1 2023-01-03 09:35:01-05:00 2023-01-03 09:35:02-05:00 2023-01-03 09:35:02-05:00  1000.325254  1000.325254  1000.325254  1000.325254  1000.325254       201
1 2023-01-03 09:35:01-05:00 2023-01-03 09:35:02-05:00 2023-01-03 09:35:02-05:00  1000.325254  1000.325254  1000.325254  1000.325254  1000.325254       301
```

The `DAG` is the same. The system just computes it multiple times on different
data as the clock moves forward.

The computation continues until it reaches the desired number of iterations
which is ten (configurable) one-second iterations. Note that it is possible not
to set the limit, the system will run forever in this case.

So we started at `09:35:02` and ended at `09:35:12` which is equivalent to 10
one-second iterations, i.e.:

- [09:35:01, 09:35:02)
- [09:35:02, 09:35:03)
- ...
- [09:35:11, 09:35:12)
```
################################################################################
bar_timestamp=2023-01-03 09:35:12-05:00
################################################################################
16:58:12 Task-1 hwall_clock_time.py set_current_bar_timestamp:105                timestamp=2023-01-03 09:35:12-05:00
16:58:12 Task-1 real_time.py execute_with_real_time_loop:433                     rt_timeout_in_secs_or_time=10, bar_duration_in_secs=1, num_it=10, num_iterations=10, is_done=True
16:58:12 - INFO  Task-1 real_time.py execute_with_real_time_loop:443    Exiting loop: num_it=10, num_iterations=10
```

# Running a DAG system in real-time

> TODO(Nina): Explain how to run a DAG system in real-time.
