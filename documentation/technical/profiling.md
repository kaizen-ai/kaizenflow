<!--ts-->
   * [Profiling end-to-end a command line](#profiling-end-to-end-a-command-line)
   * [Profiling Python code from command line](#profiling-python-code-from-command-line)
      * [line_profiler](#line_profiler)
         * [Install](#install)
         * [How to use](#how-to-use)
      * [pytest-profiling](#pytest-profiling)
         * [Install](#install-1)
         * [How to use](#how-to-use-1)
      * [cProfile](#cprofile)
   * [Profiling in a Jupyter notebook](#profiling-in-a-jupyter-notebook)
      * [Time profilers](#time-profilers)
         * [Overall](#overall)
         * [By function](#by-function)
      * [Memory profilers](#memory-profilers)



<!--te-->

# Profiling end-to-end a command line

- You can use the time-tested Linux `time` command to profile both time and
  memory

  ```bash
  > /usr/bin/time -v COMMAND 2>&1 | tee time.log

  Command being timed: "...COMMAND..."
  User time (seconds): 187.70
  System time (seconds): 16.27
  Percent of CPU this job got: 96%
  Elapsed (wall clock) time (h:mm:ss or m:ss): 3:31.38
  Average shared text size (kbytes): 0
  Average unshared data size (kbytes): 0
  Average stack size (kbytes): 0
  Average total size (kbytes): 0
  Maximum resident set size (kbytes): 13083892
  Average resident set size (kbytes): 0
  Major (requiring I/O) page faults: 0
  Minor (reclaiming a frame) page faults: 9911066
  Voluntary context switches: 235772
  Involuntary context switches: 724
  Swaps: 0
  File system inputs: 424
  File system outputs: 274320
  Socket messages sent: 0
  Socket messages received: 0
  Signals delivered: 0
  Page size (bytes): 4096
  Exit status: 0
  ```

- Information about the spent time are:

  ```bash
  User time (seconds): 187.70
  System time (seconds): 16.27
  Percent of CPU this job got: 96%
  Elapsed (wall clock) time (h:mm:ss or m:ss): 3:31.38
  ```

- The relevant part is the following line representing the amount of resident
  memory (which is ~13GB)
  ```bash
  ...
  Maximum resident set size (kbytes): 13083892
  ...
  ```

# Profiling Python code from command line

## `line_profiler`

- Profile a function line by line

- GitHub: https://github.com/pyutils/line_profiler

### Install

- Install with:
  ```bash
  > pip install line_profiler
  ```

### How to use

- Instrument the code to profile:

  ```bash
  import line_profiler

  profiler = line_profiler.LineProfiler()

  @profiler
  def function():
    ...
    profiler.print_stats()
  ```

## `pytest-profiling`

- Webpage: https://pypi.org/project/pytest-profiling

### Install

- Install with:
  ```bash
  > pip install pytest-profiling
  ```

### How to use

```bash
> pytest --profile ./amp/core/dataflow_model/test/test_pnl_simulator.py::TestPnlSimulator2::test_perf1 -s
```

## `cProfile`

- You need to run the code first with profiling enabled

- Example of command lines:

  ```bash
  # Profile a python script.
  > python -m cProfile -o prof.bin CMD

  # Profile a unit test.
  > python -m cProfile -o profile edgar/forms8/test/test_edgar_utils.py
  > python3 -m cProfile -o profile -m pytest edgar/forms8/test/test_edgar_utils.py::TestExtractTablesFromForms::test_table_extraction_example_2
  ```

- Then you can use the script `dev_scripts/process_prof.py` to automate some
  top-level statistics

- TODO(gp): Add examples

- Plotting the results

  ```bash
  > gprof2dot -f pstats profile | dot -Tpng -o output.png
  > gprof2dot -n 10 -f pstats profile | dot -Tpng -o output.png
  > gprof2dot -n 10 -f pstats profile -l "*extract_tables_from_forms*" | dot -Tpng -o output.png
  ```

- `gprof2dot` has lots of interesting options, e.g.,

  ```bash
  gprof2dot -h

    ...
    -n PERCENTAGE, --node-thres=PERCENTAGE
                          eliminate nodes below this threshold [default: 0.5]
    -e PERCENTAGE, --edge-thres=PERCENTAGE
                          eliminate edges below this threshold [default: 0.1]

    --node-label=MEASURE  measurements to on show the node (can be specified
                          multiple times): self-time, self-time-percentage,
                          total-time or total-time-percentage [default: total-
                          time-percentage, self-time-percentage]
    -z ROOT, --root=ROOT  prune call graph to show only descendants of specified
                          root function
    -l LEAF, --leaf=LEAF  prune call graph to show only ancestors of specified
                          leaf function
    --depth=DEPTH         prune call graph to show only descendants or ancestors
                          until specified depth
    --skew=THEME_SKEW     skew the colorization curve.  Values < 1.0 give more
                          variety to lower percentages.  Values > 1.0 give less
                          variety to lower percentages
    -p FILTER_PATHS, --path=FILTER_PATHS
                          Filter all modules not in a specified path
    ...
  ```

# Profiling in a Jupyter notebook

- You can find all of the examples below in action in the
  [time_memory_profiling_example.ipynb](https://github.com/alphamatic/amp/blob/master/core/notebooks/time_memory_profiling_example.ipynb)
  notebook.

## Time profilers

### Overall

- In a notebook, execute cell with
  [%time](https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-time)
  cell-magic:
  ```python
  %%time
  func()
  ```

### By function

- We prefer
  [cProfile](https://docs.python.org/2/library/profile.html#module-cProfile) for
  profiling and [gprof2dot](https://github.com/jrfonseca/gprof2dot) for
  visualization.

- The documentation does not state this, but
  [%prun](https://github.com/ipython/ipython/blob/master/IPython/core/magics/execution.py#L22)
  magic now
  [uses](https://github.com/ipython/ipython/blob/master/IPython/core/magics/execution.py#L22)
  cProfile under the hood, so we can use it in the notebook instead:

  ```python
  # We can suppress output to the notebook by specifying "-q".
  %prun -D tmp.pstats func()

  !gprof2dot -f pstats tmp.pstats | dot -Tpng -o output.png
  dspl.Image(filename="output.png")
  ```

- This will output something like this: ![](img/gprof2dot_output1.png)

- If you open the output image in the new tab, you can zoom in and look at the
  graph in detail.

- `gprof2dot`
  [supports thresholds](https://github.com/jrfonseca/gprof2dot#documentation)
  that make output more readable:

  ```python
  !gprof2dot -n 5 -e 5 -f pstats tmp.pstats | dot -Tpng -o output.png
  dspl.Image(filename="output.png")
  ```

- This will filter the output into something like this:
  ![](img/gprof2dot_output2.png)

## Memory profilers

- We prefer using
  [memory-profiler](https://github.com/pythonprofilers/memory_profiler).

- Peak memory

  ```python
  %%memit
  func()
  ```

- Memory by line
  ```python
  %mprun -f func func()
  ```
