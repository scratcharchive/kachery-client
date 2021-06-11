from typing import Any, Tuple, Union
from ._daemon_connection import _daemon_url
from ._misc import _http_post_json, _http_get_json

class OutgoingTaskRequest:
    def __init__(self, *, channel: str, task_hash: str, task_result_url: Union[str, None], status: str, error_message: Union[str, None]):
        self._channel = channel
        self._task_hash = task_hash
        self._task_result_url = task_result_url
        self._status = status
        self._error_message = error_message
        self._downloaded_result: Union[Any, None] = None
    @property
    def status(self):
        return self._status
    @property
    def task_result_url(self):
        return self._task_result_url
    @property
    def task_result(self):
        if self._status != 'finished':
            raise Exception('Cannot get task result if status is not finished')
        if self._downloaded_result:
            return self._downloaded_result
        if not self._task_result_url:
            raise Exception('No task result url')
        self._downloaded_result = _http_get_json(self._task_result_url)
        return self._downloaded_result
    @property
    def error_message(self):
        return self._error_message
    def wait(self, timeout_sec: float):
        if self._status not in ['finished', 'error']:
            daemon_url, headers = _daemon_url()
            url = f'{daemon_url}/task/waitForTaskResult'
            req_data = {
                'channelName': self._channel,
                'taskHash': self._task_hash,
                'timeoutMsec': timeout_sec * 1000
            }
            x = _http_post_json(url, req_data, headers=headers)
            if not x['success']:
                print(x)
                raise Exception(f'Unable to wait on task')
            self._status = x['status']
            self._task_result_url = x.get('taskResultUrl', None)
            self._error_message = x.get('errorMessage', None)
        if self._status == 'error':
            raise Exception(f'Task error: {self._error_message}')
        if self._status == 'finished':
            return self.task_result
        return None

def request_task_result(*, task_function_id: str, task_kwargs: dict, channel: str) -> OutgoingTaskRequest:
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/task/requestTaskResult'
    req_data = {
        'channelName': channel,
        'taskFunctionId': task_function_id,
        'taskKwargs': task_kwargs,
        'timeoutMsec': 1000
    }
    x = _http_post_json(url, req_data, headers=headers)
    if not x['success']:
        raise Exception(f'Unable to load task result')
    status = x['status']
    task_hash = x['taskHash']
    task_result_url = x.get('taskResultUrl', None)
    error_message = x.get('errorMessage', None)
    return OutgoingTaskRequest(channel=channel, task_hash=task_hash, task_result_url=task_result_url, status=status, error_message=error_message)
    # status = x['status']
    # task_hash = x['taskHash']
    # error_message = x.get('errorMessage', None)
    # task_result_url = x.get('taskResultUrl', None)
    # return TaskResultOutput(status=status, task_hash=task_hash, error_message=error_message, task_result_url=task_result_url)