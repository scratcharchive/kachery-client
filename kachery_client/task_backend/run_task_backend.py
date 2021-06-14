import json
import time
from typing import List
from .RequestedTask import RequestedTask
from os import error
from .taskfunction import find_taskfunction, all_taskfunction_ids
from .RegisteredTaskFunction import RegisteredTaskFunction
from .TaskBackend import TaskBackend
from .TaskJobManager import TaskJobManager


def run_task_backend(*, channels: List[str], task_function_ids: List[str]):
    task_functions: List[RegisteredTaskFunction] = []
    for function_id in task_function_ids:
        f = find_taskfunction(function_id)
        if f is None:
            raise Exception(f'Task function not found: {function_id}')
        for channel in channels:
            print(f'Registering task for channel {channel}: {function_id} ({f._task_function_type})')
            x = RegisteredTaskFunction(task_function_id=function_id, task_function_type=f._task_function_type, channel=channel)
            task_functions.append(x)
    function_ids_not_included = [id for id in all_taskfunction_ids() if id not in task_function_ids]
    if len(function_ids_not_included):
        print('WARNING: The following task functions are not registered:')
        print(', '.join([f"'{x}'" for x in function_ids_not_included]))
        print('')

    B = TaskBackend(registered_task_functions=task_functions)
    B.start()
    try:
        while True:
            B.process_events()
            time.sleep(0.1)
    finally:
        B.stop()
