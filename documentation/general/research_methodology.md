<!--ts-->
   * [Common research patterns](#common-research-patterns)
   * [Config](#config)
      * [Config](#config-1)
         * [Old approach](#old-approach)
         * [New approach](#new-approach)
<!--te-->

# Common research patterns

## The "Model" notebook

- Consider a notebook implementing a model that we are debugging, studying,
  experimenting with

- Each model notebook:
  - Is entirely driven by a config which controls the phases executed, their
    params, ...
  - Writes the results (aka `ResultBundle`) in a file for further processing
  - Uses logging to get debugging info only when needed
    - Ideally no `print()` statements are used, so it's easy to move code from
      notebook to libs

## "Adding-a-loop-around" notebook
- This is a pretty general idiom since a notebook can be:
  - An exploratory analysis on a specific asset
  - A model to be run on each asset
  - Different models (parametrized on X) on the same asset
  - Different models on different assets
  - A model that we want to evaluate under a general grid of params

- **Proposed solution**
  - Pass config through an env var (as python code)
  - Run notebooks in batch / parallel (e.g., on AWS)
  - Save output as `html` / ipynb for further inspection
  - Save the results from each notebook in a `ResultBundle`

## Increase / decrease the level of detail of a notebook
- Often you alternate between running lots of tests in batch mode and then
  needing to run a few model with high level of details
    - E.g., you do a monster run of many models
    - One models gives weird results or crashes
    - How to debug it?

- **_Solution_**
    - Plug the config of the model that gives problem in the model notebook, run
      again, and debug it

## Hierarchical notebooks
- A notebook often becomes "part" of another notebook or more complex flow
  - E.g., a notebook is used to compute a feature (with lots of statistics),
    and the feature becomes a piece of a model (with lots of statistics)

- **_Solution_**
  - Move the code into a library and call the same code from multiple places,
    interleaving the code with stats / plots
  - **_Pros_**
    - One can reuse the same "config" to run both notebooks
  - **_Cons_**
    - We still have a bit of a code replication at the least for the structural
      part instantiating the pipeline
    - Unit test / static analysis / linter should help catch issues due to not
      following DRY

## Process outputs from different models
- Often we need to post-process different models together
    - E.g., comparing models, A/B testing, mixing models

- **_Solution_**
  - Run all the notebooks to generate `ResultBundle`s
  - Load the bundles from different runs
  - Process in a specialized notebook, e.g.,
    - A/B testing notebook
    - Mixing notebook

## Nested loops around a notebook
- In many cases there are multiple "loops" (one on configs and one on "models")
  and it's not clear how to solve it
  - E.g., we want to compare two models (e.g., with a different param X), each
    model run on a universe of assets

- **_Solution_**
  - We assume that a model can run on multiple assets
    - We need to keep clear what is part of the predictive model and what is part
      of mixing the different outputs
  - The simpler case of one model on one asset is represented by mixing =
    pass-through
  - This is very general since we can always think of a set of models, followed
    by an aggregation step

## Variant analysis
- **_Problem_**
  - You create a model in a notebook, then you need to run many models changing a
    set of params
  - You have replicated code in the single-model notebook vs the variant analysis

- **_Solution_**
  - Use the config approach
  - Ideally one could reuse the "loop-around" approach with a "variant_analysis"
    notebook reading all the results
    - The cons is that it's a bit clunky. It might be nice to have the model
      computation in the same notebook

## Convert notebook into a library
- **_Problem_**
  - Often we start with a notebook to write code / debug
  - The we need to convert it into a script
    - E.g., we want to unit test, automate some tasks, compose functionalities
  - Potentially we might need to go back to a notebook representation

- **_Solution_**
  - Use libraries and Jupytext
  - There is still the fact that the flow calling the libraries is replicated

# Config

## Config

- We recognize that configuring a complex pipeline is a complex task and the
  solution might end up being complex as well

- The trade-off is often between "explicit" and "implicit"
  - Explicit
    - Pros:
      - Flexibility
      - No hidden assumptions
    - Cons
      - Verbose
      - Redundant (and more error prone)

- We call the pieces of the pipeline, "blocks" (or "stages", "components")

## Config object
- The config is an object
- The code is in `core/config.py`

## Hierarchical config
- We want to clarify which parameter from the config are used by each block of
  the pipeline

- **Solution**:
  - Hierarchical config
    - Cons:
      - It can become very complex with too many level

## Mapping config params to free-standing functions
- Each block `..._from_config()` needs to specify which params it needs from the
  config

- **Solution:**
  - Use a dynamic check, like
    ```
    cfg.check_params(config, mandatory=[...], optional=[...])
    ```
    -   Pros:
        -   Fails fast
        -   Helps to document what are the actual interfaces of the blocks
    -   Cons:
        -   Documentation is different from the code, and they can drift apart
        -   You change the code, forget to change the check and the code fails and you get upset
    -   Each function checks its own parameters
        -   I.e., the constraints are local and do not percolate up

- When calling a block we pass the piece of the config that it needs
  ```
  df = pip.zscore_from_config(config["zscore"], df)
  ```

- Pros
  - All the pros of explicit
- Cons
  - All the cons of explicit

## Use functional approach, whenever possible
- Each block accepts a dataframe and returns a dataframe
  - A purely functional approach would require to make a copy of the dataframe
    before modifying it
    - We could rely on numpy and pandas being smart enough to make copy on write
      and / or a shallow copy, and garbage collector to remove unused copies
    - A faster / memory efficient approach is to modify the df in place, relying
      on the fact that we only add columns and not modify to ensure idempotency
- Preferred approach is to always add new columns to the df, unless the df.index
  is changed
- Pros:
    - Functional programming
    - Easier to debug
- Cons:
    - More memory / slower

# Building a processing pipeline

## Simple approach
- While the pipeline is still linear and simple we can just use functions taking
  a `Config` object
- The next step is use our `DataFlow` framework to build complex graph
  computations that can easily put in production

# Experiment workflows

## General description

- The pipeline for a particular model is located inside a notebook and a library
  - The notebook contains the "skeleton" of all necessary training stages, and
    the library the implementation of each stage
  - E.g., the NLP sentiment pipeline is in 
    `nlp/notebooks/PartTask1102_RP_Pipeline.py` and in `nlp/lstm_utils.py`
- The specific configuration of the stages are passed using a Config object.
- Config objects are generated using config builder functions for a particular
  research task.
- The experiments are usually run in bulk with different values of different
  parameters in Configs using the `nlp/run_configs.py` script.

## King of the Hill config

### Definition

- The King of the Hill (KOTH) is a pipeline and a config that describe the model
  with the best performance at a given moment
- KOTH is the model that is being improved
  - KOTH pipeline and config are used as a template to be improved:
  - E.g., for the NLP pipeline the config is `get_KOTH_config()` in
    `nlp/build_configs.py`

## Config builders

- Config builder functions are located in the directory of the project
  - E.g., for NLP sentiment pipeline in `nlp/build_configs.py`
- Any config builder functions is passed a KOTH config and a mapping of varying
  parameters to a list of their values.
  - KOTH config is used as a template.
  - A new config is created for each possible combination of varying parameters.
  - The output is a list of Config objects.

## Running experiments

- A general flow is:

### 1. Feature implementation
- File a GH Issue with the description of the feature
- Implement the feature in the library together with unit tests
  - E.g., `nlp/lstm_utils.py`
- Modify the KOTH pipeline (e.g., the official notebook) and config to wire the
  new feature
  - E.g., `nlp/notebooks/PartTask1102_RP_Pipeline.py`
- Experiment with the KOTH (as a different notebook derived from the KOTH, or
  with the KOTH directly) to make sure the feature works as expected inside the
  official pipeline
  - You should add / compute statistics and so on to make sure things are working
    properly
- Do a PR and check in the code

### 2. Experiment
- It's better to have one single notebook (derived from the current KOTH) with a
  switch to change the experiment variable (e.g., a quantile transformation of
  the y-variable), instead of one notebook for each configuration of the
  experiment 
  - Otherwise we are going to have so many notebooks with tons of redundancy
- Then you run each experiment changing the value of the variable manually and
  save the result with `publish_notebok.py`
- Even better you can use the config building mechanism and `run_notebook.py` to
  run the enter experiment from the comfort of one button

## Sharing results

- Results of each experiment are added to a shared directory on the dev server,
  named according to the GitHub task:
  - E.g., `/data/nlp_experiments/TaskXYZ_...`
- The config builder used to generate configs for the experiments is committed
  to the repo
  - E.g. `core.config_builders.get_TaskXYZ_configs`
- The notebook with analysis of the experiments that points towards the result
  is committed to the repo
  - E.g. `nlp/notebooks/TaskXYZ_postprocessing.ipynb`
- The notebooks are automatically saved as HTML via `publish_notebook.py`
- Results of the analysis are published in the gdoc.

## Crowning ceremony

- Each research Issue culminates with the proposal of new a KOTH.
- The new defined stage or an optimal value of a previously defined parameter is
  added to the template notebook at `nlp/notebooks/PartTask1102_RP_Pipeline.py`.
- The KOTH config is updated with an option to turn on/off the new stage.
- The result of the experiment is compared across our benchmarks (e.g. accuracy
  for all futures).
- The results are reported in the gdoc and KOTH config function is updated.
