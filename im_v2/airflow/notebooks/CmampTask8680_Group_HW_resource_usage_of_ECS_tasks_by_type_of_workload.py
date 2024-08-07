# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Analyze container HW usage in ECS per type of workload

# %% [markdown]
# - Each ECS tasks has a unique task ID in form of a hash
# - We perform the analysis per each region (results should be very similar if not identical)
# - We have two types of log statistics
#     - Table that shows us task ID with a concrete type of workload, e.g. python script or invoke target
#     - Table that shows us information about HW resource usage of a given container from CloudWatch Container Insights
# - Data spans 24 hour time interval
# - Strategy to compute the table of HW usage per type of workload
#     1. Parse the entrypoint - e.g. remove the parameters from the script/invoke calls because these will differ
#     2. Group by type of entrypoint and compute average maximum utilization of RAM/CPU

# %%
import pandas as pd

# %% [markdown]
# ## Load data

# %%
europe_task_ids = pd.read_csv("stockholm-logs-insights-results-tasks.csv")
europe_task_ids["region"] = "eu-north-1"
tokyo_task_ids = pd.read_csv("tokyo-logs-insights-results-tasks.csv")
tokyo_task_ids["region"] = "ap-northeast-1"

# %%
task_ids = pd.concat([europe_task_ids, tokyo_task_ids], axis=0)

# %%
europe_container_insights = pd.read_csv("stockholm-logs-insights-results-container-insights.csv")
europe_container_insights["region"] = "eu-north-1"
tokyo_container_insights = pd.read_csv("tokyo-logs-insights-results-container-insights.csv")
tokyo_container_insights["region"] = "ap-northeast-1"

# %%
container_insights = pd.concat([europe_container_insights, tokyo_container_insights], axis=0)

# %% [markdown]
# ## Pre-process

# %%
task_ids.head()

# %%
task_ids.shape

# %%
container_insights.head()

# %%
container_insights.shape

# %% [markdown]
# Number of rows might slightly differ since the query insights command that generated the data were executed
# a few minutes apart

# %% [markdown]
# Check the various starts of the commands

# %%
task_ids["cmd"].str[:16].unique()

# %% [markdown]
# Remove single quotes and `'mkdir /.dockerenv && ` it's not informative

# %%
task_ids["cmd"] = task_ids["cmd"].str.strip("'")
task_ids["cmd"] = task_ids["cmd"].str.strip()

# %%
task_ids["cmd"] = task_ids["cmd"].str.replace("mkdir /.dockerenv && ", "")

# %%
task_ids.head()

# %% [markdown]
# Log stream suffix matches the task hash, parse it

# %%
task_ids["TaskId"] = task_ids["@logStream"].str.split("/").str[-1]

# %% [markdown]
# ## Join datasets and compute statistics

# %%
container_insights["max_cpu_ut_pct"] = (
    container_insights["max(CpuUtilized)"] / container_insights["max(CpuReserved)"]
) * 100

# %%
container_insights["max_mem_ut_pct"] = (
    container_insights["max(MemoryUtilized)"] / container_insights["max(MemoryReserved)"]
) * 100

# %%
hw_utilization_data = task_ids.merge(container_insights, on="TaskId")

# %%
hw_utilization_data.head()

# %%
hw_utilization_data.shape


# %%
# These are heuristical observed cases
def parse_workload(command: str) -> str:
    if command.startswith("invoke") or command.startswith("python"):
        # invoke my_invoke --arg 1 --arg2
        # ->
        # invoke my_invoke
        return " ".join(command.split(" ")[:2])
    elif command.startswith("aws s3 sync"):
        return "aws s3 sync"
    elif command.startswith("cd /data/shared/ecs_tokyo/preprod/ && tar -czf"):
        return "tar -czf"
    else:
        # /app/amp/my_script.py --arg 1
        # ->
        # /app/amp/my_script.py
        return command.split(" ")[0]


# %%
hw_utilization_data["task_type"] = hw_utilization_data["cmd"].apply(parse_workload) 

# %%
hw_utilization_data["task_type"].unique()

# %%
hw_utilization_data.head()

# %%
hw_utilization_stats = hw_utilization_data.groupby("task_type")[["max_cpu_ut_pct","max_mem_ut_pct"]].agg(['mean', 'max'])

# %%
hw_utilization_stats

# %% [markdown]
# Necessary dependency for `to_markdown()`

# %%
# ! sudo /venv/bin/pip install tabulate

# %%
hw_utilization_stats.to_markdown("hw_utilizations_stats.md")

# %%
# ! sudo /venv/bin/pip install tabulate

# %%
