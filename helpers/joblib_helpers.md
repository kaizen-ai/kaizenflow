<!--ts-->
   * [Using joblib_helper together with caching](#using-joblib_helper-together-with-caching)



<!--te-->
`joblib_helper.py` contains a layer to parallelize workloads

- A workload is expressed in terms of a `Workload` object which contains
  - A "workload function" to call
  - A string representing the name of the workload (e.g., the name of the
    function)
  - A list of `Task`s
    - Each `Task` corresponds to the parameters (in terms of `args` and
      `kwargs`) passed to the workload function

# Using `joblib_helper` together with caching

- The workload function doesn't have to be the one that is cached, but it can
  trigger caching of function results in the call stack
