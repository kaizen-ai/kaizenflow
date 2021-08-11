import datetime
import asyncio
import time
import random

import helpers.introspection as hintro


#start_time = time.time()


mode = "true"


def print_message(msg):
    #current_time = get_current_time()
    #replayed_time = round(time.time() - start_time, 1)
    replayed_time = round(loop.time() - start_time, 1)
    func_name = hintro.get_function_name(1)
    #print(f"{current_time}: {replayed_time}: {func_name}: {msg}")
    print(f"{replayed_time}: {func_name}: {msg}")


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
    print_message("current_time=%s" % current_time)
    to_execute = current_time.second % 2 == 1
    if to_execute:
        print_message("# Time to execute DAG!")
        # Wait for the DB to be updated.
        print_message("Waiting on DB bar: start")
        await wait_on_db_1min_bar_data()
        print_message("Waiting on DB bar: done")
        #
        print_message("Executing DAG: start")
        await execute_task(3)
        print_message("Executing DAG: done")
    return to_execute


async def heartbeat():
    """
    Check that everything is running.
    """
    print_message("System is up")
    # The system is up.
    # write a file


async def sleep(interval_in_secs: int):
    if mode == "true":
        asyncio.sleep(interval)
    elif mode == "replayed":
        asyncio.sleep(interval)
    else:
        raise ValueError


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
    interval_in_secs = 5
    num_iters = 0
    while True:
        print_message("Infinite loop: start")
        rc = await asyncio.gather(
            asyncio.sleep(interval_in_secs),
            execute_dag(),
        )
        print(rc)
        print_message("Infinite loop: end")
        num_iters += 1
        if num_iters > 5:
            break



# async def infinite_loop():
#     print(loop.time())
#     await asyncio.sleep(60)
#     print(loop.time())

#
if True:
    import async_solipsism
    loop = async_solipsism.EventLoop()
else:
    loop = asyncio.new_event_loop()

asyncio.set_event_loop(loop)
start_time = loop.time()

#asyncio.run(infinite_loop())

#loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(infinite_loop())
finally:
    loop.close()



