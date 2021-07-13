import sys
import time
from multiprocessing.connection import Connection
from typing import List, Protocol
from .._daemon_connection import _client_auth_code_info # a hack, see below

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
    failed_once = False
    while True:
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

        success_false_exception = False
        try:
            daemon_url, headers = _daemon_url() # exception may be here
            url = f'{daemon_url}/task/registerTaskFunctions'
            x = _http_post_json(url, req_data, headers=headers) # or exception may be here
            
            if not x['success']:
                success_false_exception = True
                print(x)
                raise Exception(f'Error registering task functions.') # or exception may be here
            if failed_once:
                print('Connection to daemon has been restored')
            break
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                print('Keyboard interrupt')
                raise
            if success_false_exception:
                print(f'Unexpected error registering tasks with kachery daemon.')
            else:
                print(f'Error registering tasks with kachery daemon. Perhaps kachery daemon is not running.')
            print(f'Will retry in 10 seconds')
            failed_once = True
            time.sleep(10)
            _client_auth_code_info['timestamp'] = 0 # a hack to force re-reading of client auth code
            
    # export type RequestedTask = {
    #     channelName: ChannelName
    #     taskId: TaskId
    #     taskHash: Sha1Hash
    #     taskFunctionId: TaskFunctionId
    #     kwargs: TaskKwargs
    #     taskFunctionType: TaskFunctionType
    # }
    # export interface TaskRegisterTaskFunctionsResponse {
    #     requestedTasks: RequestedTask[]
    #     success: boolean
    # }
    requested_tasks = x['requestedTasks']
    ret: List[RequestedTask] = []
    for rt in requested_tasks:
        rt_channel_name = rt['channelName']
        rt_task_id = rt['taskId']
        rt_task_hash = rt['taskHash']
        rt_task_function_id = rt['taskFunctionId']
        rt_task_function_type = rt['taskFunctionType']
        rt_task_kwargs = rt['kwargs']
        for x in registered_task_functions:
            if x.channel == rt_channel_name and x.task_function_id == rt_task_function_id:
                if x.task_function_type == rt_task_function_type:
                    ret.append(RequestedTask(
                        registered_task_function=x,
                        kwargs=rt_task_kwargs,
                        task_id=rt_task_id,
                        task_hash=rt_task_hash
                    ))
                else:
                    print(f'Warning: mismatch in task function type for {rt_task_function_id}: {x.task_function_type} <> {rt_task_function_type}')
    return ret
