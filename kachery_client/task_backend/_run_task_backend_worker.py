import sys
import time
from multiprocessing.connection import Connection
from typing import List, Union
from .._daemon_connection import _client_auth_code_info, _reset_client_auth_code # a hack, see below

from .RegisteredTaskFunction import RegisteredTaskFunction
from .RequestedTask import RequestedTask

from .._daemon_connection import _daemon_url
from .._misc import _http_post_json


def _run_task_backend_worker(pipe_to_parent: Connection, registered_task_functions: List[RegisteredTaskFunction], backend_id: Union[str, None]):
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
        requested_tasks = _register_task_functions(registered_task_functions, timeout_sec=15, backend_id=backend_id)
        for requested_task in requested_tasks:
            pipe_to_parent.send({
                'type': 'request_task',
                'requested_task': requested_task
            })
        time.sleep(0.1)

def _register_task_functions(registered_task_functions: List[RegisteredTaskFunction], *, timeout_sec: float, backend_id: Union[str, None]):
    failed_once = False
    while True:
        task_functions = []
        for a in registered_task_functions:
            task_functions.append({
                'channelName': a.channel,
                'taskFunctionId': a.task_function_id,
                'taskFunctionType': a.task_function_type
            })
        req_data = {
            'taskFunctions': task_functions,
            'backendId': backend_id,
            'timeoutMsec': timeout_sec * 1000
        }

        success_false_exception = False
        try:
            daemon_url, headers = _daemon_url() # exception may be here
            url = f'{daemon_url}/task/registerTaskFunctions'
            response = _http_post_json(url, req_data, headers=headers) # or exception may be here
            
            if not response['success']:
                success_false_exception = True
                print(response)
                raise Exception(f'Error registering task functions.') # or exception may be here
            if failed_once:
                _reset_client_auth_code() # force re-reading of client auth code
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
            _reset_client_auth_code() # force re-reading of client auth code
            
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
    requested_tasks = response['requestedTasks']
    ret: List[RequestedTask] = []
    for rt in requested_tasks:
        rt_channel_name = rt['channelName']
        rt_task_id = rt['taskId']
        rt_task_hash = rt['taskHash']
        rt_task_function_id = rt['taskFunctionId']
        rt_task_function_type = rt['taskFunctionType']
        rt_task_kwargs = rt['kwargs']
        
        for registered_task_function in registered_task_functions:
            if registered_task_function.channel == rt_channel_name and registered_task_function.task_function_id == rt_task_function_id:
                if registered_task_function.task_function_type == rt_task_function_type:
                    ret.append(RequestedTask(
                        registered_task_function=registered_task_function,
                        kwargs=rt_task_kwargs,
                        task_id=rt_task_id,
                        task_hash=rt_task_hash
                    ))
                else:
                    print(f'Warning: mismatch in task function type for {rt_task_function_id}: {registered_task_function.task_function_type} <> {rt_task_function_type}')
    return ret
