import datetime
import asyncio
import time
import random

import helpers.introspection as hintro


start_time = time.time()


mode = "true"


def print_message(msg):
    current_time = get_current_time()
    replayed_time = round(time.time() - start_time, 1)
    func_name = hintro.get_function_name(1)
    print(f"{current_time}: {replayed_time}: {func_name}: {msg}")


async def execute_task(max_delay_in_sec):
    delay_in_sec = random.random() * max_delay_in_sec
    await asyncio.sleep(delay_in_sec)


async def wait_on_db_1min_bar_data():
    """
    Wait until the DB has the bar for the current minute.
    """
    print_message("Waiting on DB: start")
    await execute_task(3)
    print_message("Waiting on DB: done")
    return "data"


async def execute_dag():
    current_time = get_current_time()
    if current_time.secs % 5 == 0:
        print_message("# Time to execute DAG!")
        # Wait for the DB to be updated.
        print_message("Waiting on DB bar: start")
        await wait_on_db_1min_bar_data()
        print_message("Waiting on DB bar: done")
        #
        print_message("Executing DAG: start")
        await execute_task(3)
        print_message("Executing DAG: done")


async def heartbeat():
    """
    Check that everything is running.
    """
    print_message("System is up")
    # The system is up.
    # write a file


async def sleep(interval_in_secs: int):
    if true_real_time:
        asyncio.sleep(interval)
    elif replayed_real_time:
        asyncio.sleep(interval)
    elif simulated_time:
        await external_clock()


def get_current_time():
    # Return true, replayed, or simulated time.
    #pass
    return datetime.datetime.now()


# async external_clock():
#     """
#     Pulse every second, real or simulated.
#     """
#     while True:


async def infinite_loop():
    while True:
        print_message()
        interval_in_secs = 5
        await asyncio.gather(
            asyncio.sleep(interval_in_secs),
            execute_dag(),
        )


asyncio.run(inifinte_loop)


# How to "simulate time"?
# - Maybe add a fake clock instead that is incremented every time?
# -