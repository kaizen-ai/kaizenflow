

<!-- toc -->

- [Create an Airflow DAG](#create-an-airflow-dag)
  * [Prerequisites](#prerequisites)
  * [Assumptions](#assumptions)
  * [Imports](#imports)
  * [DAG Configuration](#dag-configuration)
  * [Task configuration](#task-configuration)
  * [DAG Initialization](#dag-initialization)
  * [Task initialization](#task-initialization)
  * [Telegram notification initialization](#telegram-notification-initialization)
  * [Airflow UI DAG graph preview](#airflow-ui-dag-graph-preview)
  * [Notes, tips and tricks](#notes-tips-and-tricks)

<!-- tocstop -->

# Create an Airflow DAG

In this tutorial, we are going to create an Airflow DAG `.py` file from scratch.
The DAG is going to be set up to run a daily download of OHLCV spot and futures
data from Binance and optionally send a notification to desired channels (email)
upon a potential failure.

The final `.py` from this tutorial can be
[here](https://github.com/cryptokaizen/cmamp/tree/master/im_v2/airflow/dags/)
(`preprod.download_periodic_daily_ohlcv_data_fargate.tutorial.py`). There are
code comments next to logical segments of the code to describe/summarize the
concepts described in the tutorial.

## Prerequisites

1. A running Apache Airflow environment
   - An Airflow environment created using our set-up scripts which possesses
     some key features
     - AWS ECS execution environment is available (its initialization should be
       part of the set-up script)
     - Airflow environment has access to AWS
2. Access to the Airflow UI
3. Access to the directory that Airflow uses to fetch DAG definitions from.
   _Note: Our usual flow is that the dev Airflow environment fetches DAGs from a
   location within a shared filesystem e.g.
   `/data/shared/airflow_preprod_new/dags`_
4. An S3 bucket to store data

## Assumptions

- This is not an Apache Airflow tutorial, it assumes basic knowledge of
  Airflow's core concepts
- This tutorial uses concepts from AWS ECS, it is possible to understand it
  without knowledge of how ECS works if one assumes the ECS part to be a "black
  box"

## Imports

1. Create a new python file stored in the Airflow DAG directory named
   `test.download_periodic_daily_ohlcv_data_fargate.py` (the format of the file
   name `{stage}.{dag_name}.py` is important)
2. Import necessary packages:
   - Packages from the standard library used general operations
   ```python
   import copy
   import datetime
   import os
   ```
   - Packages from our custom `airflow_utils` to provide unified interfaces for
     operations widely used by a majority of DAGs
   ```python
   import airflow_utils.ecs.operator as aiutecop
   import airflow_utils.misc as aiutmisc
   ```
   - Airflow-related packages
   ```python
   import airflow
   from airflow.models import Variable
   from airflow.operators.dummy_operator import DummyOperator
   ```

## DAG Configuration

In this section, we will be adding configuration variables/parameters. we found
it useful to specify those at the beginning of the file, this way if you need to
update the DAG to do the same things but with different parameters, you don't
have to search for references throughout the entire file.

1. Get the name of the current file we're in, the name will be used further down
   the DAG definition pipeline

```python
_FILENAME = os.path.basename(__file__)
```

2. Obtain the stage name from the filename using our library function (the stage
   is propagated across the DAG definition to determine which resources to use)

```python
_STAGE = aiutmisc.get_stage_from_filename(_FILENAME)
```

3. Some resources are created separately for each user, that's why we set a user
   identifier, in this way multiple people can work in the test (AKA dev stage)
   stage side by side

_Note: ask your organization admin to obtain your username if you are not sure_

```python
_USERNAME = "replace-with-your-username"
```

4. Set task deployment type. This is a mandatory ECS configuration parameter.
   Unless you are sure you need to use `EC2`, feel free to stick with `fargate`.

```python
_LAUNCH_TYPE = "fargate"
```

5. We are going to deploy an ECS task. The operator that will run the ECS task
   needs to know what kind of ECS task definition to base the task on. There is
   a library function that chooses the right task definition provided the
   configuration parameters set above

_Note: the boolean argument value is only relevant when working outside of the
test stage_

```python
_ECS_TASK_DEFINITION = aiutecop.get_task_definition(_STAGE, False, _USERNAME)
```

6. Airflow allows you to set a custom name for the DAG visible in the browser UI
   but in order not to introduce additional parameters we simply use the
   filename.

```python
_DAG_ID = aiutmisc.get_dag_id_from_filename(_FILENAME)
```

## Task configuration

The following configuration variables will drive the behavior of the tasks
within the DAG.

- Custom way of specifying a given extraction task so we can streamline task
  creation using a `for` loop later

```python
# List jobs in the following tuple format:
#  vendor, exchange, contract, data_type, universe
_JOBS = [
    ("ccxt", "binance", "spot", "ohlcv", "v7"),
    ("ccxt", "binance", "futures", "ohlcv", "v7.3"),
]
```

- The following variables help specify the dataset signature, more information
  in the `data_schema/` director

```python
_DOWNLOAD_MODE = "periodic_daily"
_ACTION_TAG = "downloaded_1min"
_DATA_FORMAT = "parquet"
# The value is implicit since this is an Airflow DAG.
_DOWNLOADING_ENTITY = "airflow"
```

- Optionally we can also add a DAG description.

```python
_DAG_DESCRIPTION = "Daily OHLCV data download."
```

- Using the `_STAGE` obtained earlier, we can now get the correct S3 bucket for
  the stage we operate in

  - Airflow allows storing variables in a key, value format in the UI, they can
    be fetched using the `Variable.get` interface
  - To add a new variable navigate to the Airflow UI and choose
    `Admin > Variables > Click the plus icon (add a new record)`

  _Note: Based on Airflow's documentation calling `Variable.get in the top-level
  DAG file code is a bad practice, we will eventually shift away from this
  approach_

```python
_S3_BUCKET_PATH = f"s3://{Variable.get(f'{_STAGE}_s3_data_bucket')}"
```

## DAG Initialization

1. Specify a schedule for the DAG

- Each Airflow DAG uses a schedule to determine when it should run, we can store
  the schedule as an Airflow variable, this way we can alter the DAG execution
  interval without altering the `.py` file itself

```python
_SCHEDULE = Variable.get(f"{_DAG_ID}_schedule")
```

2. Set default arguments

- These options can determine how the DAG behaves once it is executing (number
  of retries, should there be an email upon failure, etc.)

```python
default_args = {
  "retries": 1 if _STAGE in ["prod", "preprod"] else 0,
  "email": [Variable.get(f"{_STAGE}_notification_email")],
  "email_on_failure": True if _STAGE in ["prod", "preprod"] else False,
  "email_on_retry": False,
  "owner": "airflow",
}
```

3. Create a DAG object

- `max_active_runs` determines if it possible to run two instances of the same
  DAG in parallel
- `catchup` determined whether Airflow should try to schedule all tasks that
  hadn't been run from `start_date` up until the present. This parameter could
  be useful for example when there is a big volume of data that needs to be
  downloaded and a single script run would overload the HW resources, we could
  perform the download in an automated way chunk by chunk.
- `start_date` unless `catchup=True` is specified this can be an arbitrary date
  value from the past

```python
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=_SCHEDULE,
    catchup=False,
    start_date=datetime.datetime(2023, 3, 20, 0, 0, 0),
    tags=[_STAGE],
)
```

## Task initialization

1. We prepare a command executed by the container executed using ECS

```python
download_command = [
    "/app/amp/im_v2/common/data/extract/download_bulk.py",
    "--end_timestamp '{{ data_interval_end.replace(hour=0, minute=0, second=0) - macros.timedelta(minutes=1) }}'",
    "--start_timestamp '{{ data_interval_start.replace(hour=0, minute=0, second=0) }}'",
    "--vendor '{}'",
    "--exchange_id '{}'",
    "--universe '{}'",
    "--data_type '{}'",
    "--contract_type '{}'",
    "--aws_profile 'ck'",
    "--assert_on_missing_data",
    f"--s3_path '{_S3_BUCKET_PATH}'",
    f"--download_mode '{_DOWNLOAD_MODE}'",
    f"--downloading_entity '{_DOWNLOADING_ENTITY}'",
    f"--action_tag '{_ACTION_TAG}'",
    f"--data_format '{_DATA_FORMAT}'",
]
```

- The values set inside `{{ }}` are
  [Airflow Jinja templating](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
  feature.
- `data_interval_end` and `data_interval_start` are values dynamically set via
  Airflow based on the current run that's being executed
  - Example:
    - Let's have a DAG scheduled using a cron expression `30 1 * * *` (run every
      day at 1:30 AM)
    - On 20th October 2023 1:30AM when the DAG is scheduled the values will be:
      - `data_interval_start = 19th October 2023 1:30AM`
      - `data_interval_end = 20th October 2023 1:30AM`
  - `- macros.timedelta(minutes=1)` is applied to follow the convention of the
    downloading in the interval `[start_timestamp, end_timestamp)`
    - `macros` is a part of the Jinja templating engine and offers a subset of
      common standard library functionalities
- The unformatted `{}` will be filled dynamically in the next step

2. Create a set of tasks to place inside the DAG

- We utilize a for loop to dynamically initialize multiple jobs to run to avoid
  code repetition.

```python
for vendor, exchange, contract, data_type, universe in _JOBS:
```

- Dynamically fill the prepared command. It is necessary to use `deepcopy`
  because the variable is a list and we need to re-format the elements on each
  iteration of the loop

```python
curr_bash_command = copy.deepcopy(download_command)
curr_bash_command[3] = curr_bash_command[3].format(vendor)
curr_bash_command[4] = curr_bash_command[4].format(exchange)
curr_bash_command[5] = curr_bash_command[5].format(universe)
curr_bash_command[6] = curr_bash_command[6].format(data_type)
curr_bash_command[7] = curr_bash_command[7].format(contract)
```

- Get `EcsRunTaskOperator` using our library function

```python
downloading_task = aiutecop.get_ecs_run_task_operator(
    dag,
    _STAGE,
    f"download.{_DOWNLOAD_MODE}.{vendor}.{exchange}.{contract}.{universe}",
    curr_bash_command,
    _ECS_TASK_DEFINITION,
    256,
    512,
    assign_public_ip=True,
)
```

- Apply binary shift right operator `>>` which has a special meaning in the
  context of Airflow. It specifies the order of DAG tasks (`>>` is like an arrow
  in a directed acyclic graph)
  - Placing this inside the for loop ensures all downloading jobs run in
    parallel

```python
start_task >> downloading_task >> end_download >> end_dag
```

## Telegram notification initialization

Finally, we add a Telegram notification task which runs if one or more tasks
within a given DAG fails, we have a library function to obtain the task
instance:

```python
telegram_notification_task = aiutteop.get_telegram_operator(
    dag, _STAGE, "datapull", _DAG_ID, "{{ run_id }}"
)
```

On the backend, the function initializes the following operator:

```python
tg_operator = TelegramOperator(
    task_id="send_telegram_notification_on_failure",
    telegram_conn_id=f"{_STAGE}_telegram_conn",
    text=f"DAG {dag_id} failed \nrun ID: {run_id}",
    dag=dag,
    trigger_rule='one_failed'
)
```

Using the familiar `>>` operator we place the obtained task AFTER the
downloading task but BEFORE the dummy task to end the DAG, this allows us to set
the overall DAG as failed if one of the relevant tasks fails. Otherwise, if the
telegram notification task is the last one in the DAG, the standard logic of
Airflow would mark the DAG run as successful.

```python
end_download >> telegram_notification_task >> end_dag
```

## Airflow UI DAG graph preview

In the Airflow UI, we can preview the graph structure of our DAG

![Airflow UI DAG graph preview](figs/create_airflow_dag/image1.png)

## Notes, tips and tricks

- The high-level concept of running a DAG task using a set of
  `EcsRunTaskOperator`s is an omnipresent concept throughout most of the DAGs
  executed in our environment.
  - It allows us to avoid managing complex dependencies within the Airflow
    environment since each task runs in its isolated docker container wrapped in
    an ECS task
