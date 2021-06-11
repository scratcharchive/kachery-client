from typing import Any, List, Protocol
from .._daemon_connection import _daemon_url
from .._misc import _http_post_json

class RegisteredTaskFunctionCallback(Protocol):
    def __call__(self, *, channel: str, task_hash: str, task_kwargs: Any):
        pass


class RegisteredTaskFunction:
    def __init__(self, *, task_function_id: str, channel: str, callback: RegisteredTaskFunctionCallback) -> None:
        self._task_function_id = task_function_id
        self._channel = channel
        self._callback = callback
    @property
    def task_function_id(self):
        return self._task_function_id
    @property
    def channel(self):
        return self._channel
    @property
    def callback(self):
        return self._callback

def register_task_functions(registered_task_functions: List[RegisteredTaskFunction], *, timeout_sec: float):
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/task/registerTaskFunctions'
    x = []
    for a in registered_task_functions:
        x.append({
            'channelName': a.channel,
            'taskFunctionId': a.task_function_id
        })
    req_data = {
        'taskFunctions': x,
        'timeoutMsec': timeout_sec * 1000
    }
    x = _http_post_json(url, req_data, headers=headers)
    if not x['success']:
        raise Exception(f'Unable to register task functions. Perhaps kachery daemon is not running.')
    requested_tasks = x['requestedTasks']
    for rt in requested_tasks:
        rt_channel_name = rt['channelName']
        rt_task_hash = rt['taskHash']
        rt_task_function_id = rt['taskFunctionId']
        rt_task_kwargs = rt['kwargs']
        for a in registered_task_functions:
            if a.task_function_id == rt_task_function_id and a.channel == rt_channel_name:
                a.callback(channel=rt_channel_name, task_hash=rt_task_hash, task_kwargs=rt_task_kwargs)