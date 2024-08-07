import datetime
import logging
import os
import random
import sys
import time

import airflow
import airflow_utils.ecs.operator as aiutecop
import airflow_utils.misc as aiutmisc
import psutil
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Set up logging
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.getLevelName("INFO"),
    handlers=[logging.StreamHandler(sys.stdout)],
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)

_FILENAME = os.path.basename(__file__)

# This variable will be propagated throughout DAG definition as a prefix to
# names of Airflow configuration variables, allow to switch from test to preprod/prod
# in one line (in best case scenario).
# _STAGE = _FILENAME.split(".")[0]
dag_type = "trading"
components_dict = aiutmisc.extract_components_from_filename(_FILENAME, dag_type)
_STAGE = components_dict["stage"]
_REGION = (
    aiutecop.ASIA_REGION
    if components_dict["location"] == "tokyo"
    else aiutecop._EUROPE_REGION
)

# Constants and configurations
_DAG_ID = components_dict["dag_id"]
_DAG_DESCRIPTION = "DAG to stress test Airflow Kubernetes deployment"
_SCHEDULE = "@daily"
_DEFAULT_ARGS = {
    "owner": "shayan",
    "start_date": datetime.datetime(2022, 8, 1, 0, 0, 0),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=6),
    "email_on_failure": True,
    "email_on_retry": False,
}

# Create a DAG
dag = airflow.DAG(
    dag_id=_DAG_ID,
    description=_DAG_DESCRIPTION,
    schedule_interval=_SCHEDULE,
    default_args=_DEFAULT_ARGS,
    catchup=False,
    tags=[_STAGE],
)

# Dummy tasks for starting and ending the DAG
start_task = DummyOperator(task_id="start_stress_test", dag=dag)
end_task = DummyOperator(task_id="end_stress_test", dag=dag)


def log_resource_usage():
    cpu_percent = psutil.cpu_percent(interval=1)
    memory_info = psutil.virtual_memory()
    logger.info(f"CPU Usage: {cpu_percent}%")
    logger.info(
        f"Memory Usage: {memory_info.percent}% used of {memory_info.total} total"
    )


# Simulate high CPU load
def high_cpu_task():
    logger.info("Starting high CPU load simulation")
    log_resource_usage()
    print("Simulating high CPU load...")
    # Simulate CPU intensive computation
    [x**3 for x in range(9000000)]
    log_resource_usage()
    logger.info("Completed high CPU load simulation")


# Task to simulate high load
high_cpu_load_task = PythonOperator(
    task_id="high_cpu_load",
    python_callable=high_cpu_task,
    dag=dag,
)


# Simulate memory load
def memory_stress_task():
    logger.info("Starting memory load simulation")
    log_resource_usage()
    print("Simulating high memory load...")
    large_list = [x for x in range(9000000)]  # Consumes more memory
    log_resource_usage()
    logger.info("Completed memory load simulation")


# Task to simulate high load
memory_load_task = PythonOperator(
    task_id="high_memory_load",
    python_callable=memory_stress_task,
    dag=dag,
)


# Function to simulate failures
def simulate_failure():
    logger.info("Simulating potential task failure")
    log_resource_usage()
    # Simulate failures like network drop, high CPU/memory usage causing crash
    if random.random() < 0.1:  # 10% chance of failure
        logger.error("Simulated task failure due to random condition")
        raise Exception("Simulated task failure due to resource constraints")
    log_resource_usage()
    logger.info("Task completed without failure")


# Task to simulate failures
failure_task = PythonOperator(
    task_id="simulate_failure",
    python_callable=simulate_failure,
    dag=dag,
)


def simulate_etl_task():
    try:
        logger.info("Simulating ETL task")
        log_resource_usage()
        # Simulate variable CPU and memory load
        execution_time = random.randint(
            6, 36
        )  # Random execution time between 6 and 36 seconds
        data_size = random.randint(
            90, 900
        )  # Simulate processing 90 to 900 units of data
        time.sleep(execution_time)  # Simulate task execution
        if random.choice([True, False]):
            raise Exception("Simulated task failure due to resource constraint")
        log_resource_usage()
        logger.info("ETL task simulation completed")
    except Exception as e:
        print(f"ETL task Handled error: {str(e)}")


# Dynamically creating multiple instances of ETL simulation tasks
etl_simulation_tasks = [
    PythonOperator(
        task_id=f"etl_simulation_task_{i}",
        python_callable=simulate_etl_task,
        dag=dag,
    )
    for i in range(10)  # Adjustable based on the desired load
]


def cpu_intensive_load(duration):
    start_time = time.time()
    while time.time() - start_time < duration:
        [x**2 for x in range(10000)]  # perform CPU-intensive calculations


def memory_intensive_load(duration):
    start_time = time.time()
    large_list = []
    while time.time() - start_time < duration:
        large_list.append("a" * 10**6)  # Increase memory usage significantly
        if (
            len(large_list) * 10**6 > 500 * 10**6
        ):  # Limit to 500MB or any reasonable limit
            large_list = []


def simulate_autoscaling():
    # Simulate a task that would trigger autoscaling
    logger.info("Simulating autoscaling behavior")
    log_resource_usage()
    cpu_intensive_load(360)  # Run CPU intensive load for 6 minutes
    memory_intensive_load(360)  # Run memory intensive load for 6 minutes
    log_resource_usage()
    logger.info("Autoscaling simulation completed")


autoscaling_test = PythonOperator(
    task_id="autoscaling_behavior_test",
    python_callable=simulate_autoscaling,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,  # Ensures execution regardless of upstream task success/failure
)

# Define sequence of tasks
(
    start_task
    >> [high_cpu_load_task, memory_load_task]
    >> failure_task
    >> etl_simulation_tasks
)
etl_simulation_tasks >> autoscaling_test >> end_task
