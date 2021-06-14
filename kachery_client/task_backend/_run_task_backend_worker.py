import time
from multiprocessing.connection import Connection
from typing import List, Protocol

from .RegisteredTaskFunction import RegisteredTaskFunction
from .RequestedTask import RequestedTask

from .._daemon_connection import _daemon_url
from .._misc import _http_post_json


def _run_task_backend_worker(pipe_to_parent: Connection, registered_task_functions: List[RegisteredTaskFunction]):
    while True:
        while pipe_to_parent.poll():
            x = pipe_to_parent.recv()
            if isinstance(x, dict):
                type0 = x.get('type', '')
                if type0 == 'exit':
                    return
                else:
                    raise Exception(f'Unexpected message type in _run_task_backend_worker: {type0}')
            else:
                print(x)
                raise Exception('Unexpected message in _run_task_backend_worker')
        requested_tasks = _register_task_functions(registered_task_functions, timeout_sec=4)
        for requested_task in requested_tasks:
            pipe_to_parent.send({
                'type': 'request_task',
                'requested_task': requested_task
            })
        time.sleep(0.1)

def _register_task_functions(registered_task_functions: List[RegisteredTaskFunction], *, timeout_sec: float):
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/task/registerTaskFunctions'
    # export type RegisteredTaskFunction = {
    #     channelName: string
    #     taskFunctionId: TaskFunctionId
    #     taskFunctionType: TaskFunctionType
    # }
    # export interface TaskRegisterTaskFunctionsRequest {
    #     taskFunctions: RegisteredTaskFunction[]
    #     timeoutMsec: DurationMsec
    # }
    x = []
    for a in registered_task_functions:
        x.append({
            'channelName': a.channel,
            'taskFunctionId': a.task_function_id,
            'taskFunctionType': a.task_function_type
        })
    req_data = {
        'taskFunctions': x,
        'timeoutMsec': timeout_sec * 1000
    }
    x = _http_post_json(url, req_data, headers=headers)
    # export type RequestedTask = {
    #     channelName: ChannelName
    #     taskId: TaskId
    #     taskFunctionId: TaskFunctionId
    #     kwargs: TaskKwargs
    #     taskFunctionType: TaskFunctionType
    # }
    # export interface TaskRegisterTaskFunctionsResponse {
    #     requestedTasks: RequestedTask[]
    #     success: boolean
    # }
    if not x['success']:
        raise Exception(f'Unable to register task functions. Perhaps kachery daemon is not running.')
    requested_tasks = x['requestedTasks']
    ret: List[RequestedTask] = []
    for rt in requested_tasks:
        rt_channel_name = rt['channelName']
        rt_task_id = rt['taskId']
        rt_task_function_id = rt['taskFunctionId']
        rt_task_function_type = rt['taskFunctionType']
        rt_task_kwargs = rt['kwargs']
        for x in registered_task_functions:
            if x.channel == rt_channel_name and x.task_function_id == rt_task_function_id:
                if x.task_function_type == rt_task_function_type:
                    ret.append(RequestedTask(
                        registered_task_function=x,
                        kwargs=rt_task_kwargs,
                        task_id=rt_task_id
                    ))
                else:
                    print(f'Warning: mismatch in task function type for {rt_task_function_id}: {x.task_function_type} <> {rt_task_function_type}')
    return ret
