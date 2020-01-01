<!--ts-->
      * [Where to save files for general consumption](#where-to-save-files-for-general-consumption)
      * [Releasing data sets](#releasing-data-sets)
         * [Why having these guidelines](#why-having-these-guidelines)
         * [Datasets are versioned](#datasets-are-versioned)
         * [Dataset reader](#dataset-reader)
         * [Dataset conventions](#dataset-conventions)
         * [Datasets are documented](#datasets-are-documented)
         * [Reproducibility](#reproducibility)
         * [Patching a dataset](#patching-a-dataset)
         * [Lifecycle of a dataset task](#lifecycle-of-a-dataset-task)
         * [Expose a dataset reader](#expose-a-dataset-reader)
         * ["Unofficial" datasets](#unofficial-datasets)
         * [Suggested data formats](#suggested-data-formats)
      * [Tabular data](#tabular-data)
      * [Unstructured data](#unstructured-data)
         * [Releasing a new dataset](#releasing-a-new-dataset)
      * [Data encyclopedia guidelines](#data-encyclopedia-guidelines)
      * [Design pattern for data processing](#design-pattern-for-data-processing)
         * [Organize scripts as pipelines](#organize-scripts-as-pipelines)
         * [Incremental behavior](#incremental-behavior)
         * [Run end-to-end](#run-end-to-end)
         * [Think about scalability](#think-about-scalability)
         * [Use command line for reproducibility](#use-command-line-for-reproducibility)
      * [Structure the code in terms of filters](#structure-the-code-in-terms-of-filters)
      * [Pipeline design pattern](#pipeline-design-pattern)
         * [Config contains everything](#config-contains-everything)
         * [Make the pipeline think only in terms of config](#make-the-pipeline-think-only-in-terms-of-config)
         * [Repeat the structure but not the implementation](#repeat-the-structure-but-not-the-implementation)
         * [Have a way to compare and pretty-print configs](#have-a-way-to-compare-and-pretty-print-configs)
         * [Save the config together with the results](#save-the-config-together-with-the-results)
         * [Use functions to allow total flexibility](#use-functions-to-allow-total-flexibility)
         * [Each function validates its own params](#each-function-validates-its-own-params)
         * [Bend but not break](#bend-but-not-break)
         * [Make configs hierarchical](#make-configs-hierarchical)
         * [Add verbose mode](#add-verbose-mode)
         * [Multi-process   memoization &gt; optimize one function](#multi-process--memoization--optimize-one-function)



<!--te-->

## Where to save files for general consumption

- In general we prefer to separate data that is:
  - Used only by one person: you can keep it in your working dir or in
    `/scratch`
  - Used by multiple people: you should move it to a central place (ideally on
    `S3` so it can be accessed by anywhere, or `/data`)

- When you change shared data:
  - Send an email to the team broadcasting the update
  - Try to keep things back compatible (e.g., make a copy of the old file adding
    a timestamp, instead of overwriting it)

- Ideally we would like to have accessors that abstract an interface, so that we
  can make changes without client code being affected
  - E.g., instead of having in all notebooks `pd.read_hd5('/s3/xyz_data/...')`
    one can implement a function with a (default) parameter pointing to the
    official file
  - If you see that your code that used to work doesn't work anymore, check the
    git log or related files to see if somebody made a change but did not
    propagate it everywhere

## Releasing data sets

- In the following there are guidelines for official datasets

### Why having these guidelines

- We need to take care of datasets as much as with code
- The problem we want to avoid is:
  - Generate a dataset
  - Do research / change code / tweak the data
  - Research is successful and we need to move to production
  - We don't know how the data was generated:
    - We waste time trying to reverse engineer our own work (absurd!)
    - In the worst case we need to trash all the work done

### Datasets are versioned

- If we create a new version of the same file by fixing a bug, we bump up the
  revision
  - Use a version suffix `foobar_v3.pq`
  - **Good**
    - `v0.3`
  - **Bad**
    - `v 0.3`
    - `v_0.3`
    - `V0.3`

### Dataset reader

- Each version has a reader in the Data Encyclopedia
- This should be the only way to access the data
- It is ok to add a version parameter to the reader to make it back compatible

### Dataset conventions

- Shared datasets should be written from HEAD. Do not write shared datasets with
  code that is not checked in.
- Datasets should have meaningful filenames with meaningful metadata (e.g., the
  data period that is covered in the usual `YYYYMMDD` format and a version
  number such as `v0.1`)
  - E.g., `TaskXYZ_edg_forms4_v0.1_20180923_20181201.pq`

- Dataset file needs to:
  - Be saved in the proper directory, which should be modeled after the code
    base
    - E.g.,`/s3/.../rp/...`
  - Have a reference to the GitHub task that was used to generate it
    - In this way one can refer to the task to understand the details
  - Have information about their version
  - Have reference to the interval of time that it is contained
    - In general we like [a, b) intervals
  - Have an extension of the format, even if they are directory, e.g.,
    - `.h5` (not`.hdf5`)
    - `.pkl` (not`.pickle`)
    - `.pq` (not`.parquet`)
  - Separate chunks of information with `_` and not `-`
    - The char `-` is not Linux friendly and should be avoided when possible
    - E.g., `/s3/.../rp/timestamp/Task777_timestamp_v0.7_20180101_20181201.pq`

### Datasets are documented

- Each dataset has documentation (e.g., DataManual gdoc, code, `md`) that
  explains:
  - Where is the original raw data that was used
  - How it was generated, e.g.,
    - Exact command line and options
    - An indication of how long it took (like "5 mins" vs "12 hours")
    - Commit hash, if needed
  - A changelog with the changes with respect to the previous version
  - A description of the fields
  - See Data Manual (aka DM)

### Reproducibility

- Generating a dataset needs to be simple and automatic
  - E.g., it's not ok to run too many scripts, notebooks, reading and saving the
    data, and so on
  - Ideally we want something that is "fire-and-forget"
    - I.e., we start a script and the data generation, sanity checks, and so on,
      run without any human intervention

### Patching a dataset

- Sometimes we "patch up" a data set
  - Read the old version of the data
  - Make some change
  - Save the new data back
- In these cases:
  - We still bump the revision probably a minor one (e.g., from `v0.3` to
    `v0.3.1`)
  - We need to follow the release process (e.g., update gdoc, reader in data
    encyclopedia)
  - We need to make sure that the change is also applied to the official release
    flow - kick off the process of generating the dataset from scratch - make
    sure that the dataset matches the patched version

### Lifecycle of a dataset task

- Each data set should have an umbrella issue with a list of all the desired
  features for that specific dataset
  - `EDG: Wishlist for timestamp data set #529`
  - Any time we find a bug or we want to improve the dataset we add something to
    the wishlist
  - The wishlist is used to prioritize the work
- From the wishlist we create bugs, ideally one bug per feature as usual
  - Once the work for the bug is completed we re-run the entire pipeline to
    generate the data set
  - Every time we change the data set, even to fix a bug in the previous
    release, we should bump up the version
  - We can use minor version e.g., `v0.4.1` for bug fixes and minor improvements
    (e.g., fix the name of a column) and major revisions (`v0.4` -> `v0.5`) for
    a new feature

### Expose a dataset reader

- Provide library functions for reading the data creating a nice interface for
  the data (e.g., `ravenpack/project_helpers/rp_sql_pickle.py`)

- Use a variable in the code to point to the official version of the data that
  is passed to the reader function, or a default parameter

- Pros
  - We don't have to change all the code when we release a new version of the
    data
  - Old code can still still use an older version of the data by specifying a
    previous path

- Add all the conversion functions that are needed so that we can add a level of
  indirection from the raw data (e.g., ET conversion, float to int conversion,
  renaming of columns...)

### "Unofficial" datasets

- Often it's a good idea to pre-compute some data structures, e.g., running a
  long SQL query, and then saving it for later use in a notebook.

- We refer to these datasets as "unofficial" datasets, since they are meant only
  for internal consumption, and we not necessarily distribute to people outside
  our group

- We should be able to regenerate any dataset from scratch in a deterministic
  way.

- Even if the data is just for your own consumption, at least document something
  in the notebook code

- Cons:
  - These datasets end up being used by multiple people, with misunderstandings
    on how data was generated and under which assumptions
  - The data is filtered several times in contradiction with DRY (Don't Repeat
    Yourself) principle, making difficult to understand how each data subset was
    generated
  - When it's time to put it in production, nobody remembers how the data was
    generated

### Suggested data formats

## Tabular data

- Best formats to store tabular data are, in order:
  - Parquet
    - [ParquetDataset](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html)
      are a good idea to keep things organized
    - (although we are still learning the limitations with it)
    - Please use `.pq` (not `.pqt` or `.parquet` as extensions)
  - H5 fixed format
    - [+] super fast
    - [-] can't be sliced
  - H5 table format
    - [+] can be sliced on created indices and columns
    - [-] slower, larger footprint
  - Pickle
    - [-] not portable across versions of pandas, python, and potentially
      different OS

## Unstructured data

- Pickle
  - [+] preserve python types
  - [+] more general
  - [+] faster
  - [-] incompatibility issues
- Json
  - [+] easy to inspect (‘The Power of Plain Text')

- One can save the same data both as pickle and as json, using the same name and
  different suffixes (`.pkl`, `.json`), to allow for both computer and human
  consumption.
  - This is not great because in violation of DRY, but it can be useful in some
    cases (e.g., small files that need to have types and so they are pickled,
    but we also want to be able to inspect them)

### Releasing a new dataset

- When you change the data reader interface, or you create a new version of the
  data set, do a quick grep looking for code that relies on that data set
  - Fix the simple places (e.g., when it's a function rename or adding a
    parameter)
  - File a bug for people indicating the code / notebooks that need to be fixed
    by the author
  - Try to do as much as possible to lower the burden for the team to transition
    to the new API

## Data encyclopedia guidelines

- Each section should

  1. Use a reader to read the data
     - The wrapper should use a constant to point to the data location
     - The wrapper does common transformations needed by each client
  2. Show some basic stats about the data (e.g., timespan, number of tickers)
  3. Print a snippet of the data
  4. Map id to other representation and vice versa

- Before each commit, do "Restart -> Restart & Run" to make sure nothing is
  broken (if so please file a bug)
- We want to unit tests some of the important notebooks like this

## Design pattern for data processing

### Organize scripts as pipelines

- One can organize complex computations in stages of a pipeline
  - E.g., to parse EDGAR forms
    - Download -> (raw data) -> header parser -> (pq data) -> XBLR / XML / XLS
      parser -> (pq data) -> custom transformation

- One should be able to run the entire pipeline or just a piece
  - E.g., one can run the header parser from the raw data, save the result to
    file, then read this file back, and run the XBLR parser

- Ideally one would always prefer to run the pipeline from scratch, but
  sometimes the stages are too expensive to compute over and over, so using
  chunks of the pipeline is better

- This can also mixed with the "incremental mode", so that if one stage has
  already been run and the intermediate data has been generated, that stage is
  skipped

- Each stage can save files in a `tmp_dir/stage_name`

- The code should be organized to allow these different modes of operations, but
  there is not always need to be super exhaustive in terms of command line
  options
  - E.g.,
    - Implement the various chunks of the pipeline in a library,
    - Separate functions that read / save data after a stage
    - Assemble the pieces into a throw-away script where you hard wire the file
      names and so on

### Incremental behavior

- Often we need to run the same code over and over
  - E.g., because the code fails on an unexpected point and then we need to
    re-run from the beginning
- To address this need we use options like:
  - `--incremental`
  - `--force`
  - `--start_date`
  - `--end_date`

- One can specify `start_date` / `end_date` to process only a subset of the data

- Check existence of the output file before start function (or a thread when
  using parallelism) which handle data of the corresponding period
  - If `--incremental` is set and the output file already exists, then skip the
    computation and report
    `python _LOG.info("Skipping processing file %s as requested", ...)`
  - If `--incremental`is not set:
    - If the output file exists then we issue a `_LOG.error` and abort the
      process
    - If the output file exists and param `--force` is set (to be sure that this
      is what the user wants), then report a `_LOG.warn` and overwrite the
      output file

- As final defense against mistakes, use `chmod a-w` on files that we believe
  should not be overwritten

### Run end-to-end

- Try to run flows end-to-end (and from scratch) so we can catch unexpected
  issues and code defensively
  - E.g., we find out that data is malformed in 0.01% of the cases and only
    running end-to-end we can catch all the weird cases
- This also helps spotting scalability issues early on
  - If it takes 1 hr for 1 month of data and we have 10 years of data is going
    to take 120 hours (=5 days) to run on the entire data set
  - Once we know this we can start thinking of how to address scalability issues

### Think about scalability

- Do experiments to try to understand if a code solution can scale to the
  dimension of the data we have to deal with
  - E.g., inserting data by doing SQL inserts of single rows are not scalable
    for pushing 100GB of data
- Remember that typically we need to run the same scripts multiple times (e.g.,
  for debug and / or production)
  - So long operations that we believe are one-time, and thus can be slow, end
    up being run multiple times

### Use command line for reproducibility

- Try to pass params through command line options when possible
  - In this way a command line contains all the set-up to run an experiment

## Structure the code in terms of filters

- Focus on building a set of "filters" split into different functions, rather
  than a monolithic flow
- Organize the code in terms of a sequence of transformations that can be run in
  sequence, e.g.,
  1. Create SQL tables
  2. Convert JSON data to CSV
  3. Normalize tables
  4. Load CSV files into SQL
  5. Sanity check the SQL (e.g., mismatching TR codes, missing dates)
  6. Patch up SQL (e.g., inserting missing TR codes and reporting them to us so
     we can check with TR)

## Pipeline design pattern

- Many of what said here applies also to notebooks, since a notebook is already
  structured as a pipeline

### Config contains everything

- Centralize all the options into a config that is defined at the beginning of
  the notebook, instead of having parameters interspersed in the entire
  notebook.
- The config controls what needs to be done (i.e., control flow) and how it's
  done.
  - E.g,, the SPY adjustment becomes a single function and the cell is:
    ```python
        if config["market_adjustment"]:
            market_adjust_from_config(config)
    ```

### Make the pipeline think only in terms of config

- Each function gets its params from the config. There is an adapter that adapts
  the config to the specific function allowing to keep the interfaces flexible
  - E.g.,

  ```python
     def market_adjust(in_sample_start, in_sample_end):
        ...
  ```

- The corresponding adapter is like:

  ```python
     def market_adjust_from_config(config):
         return market_adjust(config["in_sample_start"], config["in_sample_end"])
  ```

- In this way the pipeline (i.e., notebooks) just passes the config to all the
  functions and it's easy to maintain flexibility at the beginning of the
  research process
- Once the code is more stable, one can remove the adapters.

- This can be controversial and I usually do it only for functions whose
  interface I expect to change

### Repeat the structure but not the implementation

- In other words, the "structure", i.e., the control flow, is difficult to
  factor out across different notebooks
- It's ok to repeat the structure of the code (e.g., cut and paste the same
  invocation of functions in different notebooks) as long as the implementation
  of the pieces doing the actual work is factored out in a library

### Have a way to compare and pretty-print configs

- Self-explanatory
- `pprint()` is awesome to print data structures nicely

### Save the config together with the results

- In this way one can know exactly how the results were generated. Also saving
  info about, user, server, time, git version and conda env can help.

### Use functions to allow total flexibility

- Sometimes one piece of the pipeline becomes super complicated because one
  wants to do completely different things in different situations, so one ends
  up doing:

  ```python
  def stage(mode):
     if mode == "mode1":
        f(...)
     elif mode == "mode2":
        f(...)
        g(...)
     else:
        raise ValueError(...)
  ```

- Or tries to factor it out like

  ```python
  def stage(mode):
     if mode in ("mode1", "mode2"):
       f(...)
     if mode in ("mode2", ):
      g(...)
  ```

- Or creates a mini language passing strings that represents ways, e.g.,

```python
{ name:my_filter_namecolumn_name:name_of_matching_feature_column_in_dataframebucket: {
    bucket_name:my_bucket_namebucket_spec:
        (>, threshold_1) | (<, threshold_2) }
    bucket: {
        bucket_name:my_2nd_bucket_namebucket_spec: (==, special_value)
    } ...
}
```

- The problem with the mini-language is that no matter how complex it is, one
  ends up always wanting to do something beyond the limits of the mini-language
  (e.g., "I want if-then-else", "I want loops", ...)

- Another approach is to use python directly instead of inventing a new
  "mini-language" by passing the name of a function and then evaluate it
  dynamically

  ```python
  config["stage_func"] = {
      "function_name": "foo_bar",
      "params": ...
  }

  def stage(mode):
      f = eval(config["stage_func"]["function_name"])
      f(config["stage_func"]["params"])
  ```

- For instance one can compose masks / filtering in any possible way by creating
  functions that combine the filters in different ways and then passing the name
  of the function to the pipeline.

- One can also pass a pointer to the function directly instead of the name to be
  evaluated at later stage.

### Each function validates its own params

- This is basic separation of responsibility, but sometimes one wants to do some
  checks at the beginning to avoid to get an assertion too late
- Classical example: the name of the file to save the result is invalid and a
  long run crashes at the very end.

- In this case an approach is to separate function and param validation like:
  ```python
  def f(...):
     check_f_params(...)
  ```
  and then call `check_f_params()` also at the beginning when validating the
  config.

### Bend but not break

- Always bend and try not to break.
- E.g., to save a file, create the enclosing dir if missing, instead of having
  the OS throw an error

### Make configs hierarchical

- As soon as stages of the pipeline become hierarchical also the config becomes
  hierarchical (instead of flat) so that

  ```python
  def stage1(config):
     ...
     stage2(config["stage2"])
     ...
  ```

- E.g., `generate_sentiment_mask()`
  ```python
  config = {
     "generate_sentiment_mask": {
        "min": -0.5,
        "max": -0.5,
     }
  }
  ```

### Add verbose mode

- Use logging

- One problem is that to keep the notebook simple, one ends up having very
  complex that in the notebook were originals many cells with a lot of debug,
  plots in the middle. One approach is to have a "verbose" mode:

  ```python
     def f1(..., verbose=False):
         # work work
         if verbose:
            # plot, print, display ...
         f2(..., verbose=verbose)
  ```

- In this way when one wants to "debug" in the notebook one function (that
  corresponds to a lot of cells that have been collapsed to encapsulate /
  reuse), one can set the verbose mode and get all the debug spew, although one
  can't easily break and inspect after one "cell".

- Global and function-specific verbose mode can even become config params.

### Multi-process + memoization > optimize one function

- IMO it is better to keep functions simple even if slow, and then get speed-up
  by avoiding to recompute the same result and use coarse grain parallelism.
  Maybe obvious, maybe not.
