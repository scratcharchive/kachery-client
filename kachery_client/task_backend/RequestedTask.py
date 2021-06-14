from typing import Any, Union
from .RegisteredTaskFunction import RegisteredTaskFunction
from ._update_task_status import _update_task_status


class RequestedTask:
    def __init__(self, *, registered_task_function: RegisteredTaskFunction, kwargs: dict, task_id: str) -> None:
        self._registered_task_function = registered_task_function
        self._kwargs = kwargs
        self._task_id = task_id
        self._status = 'waiting'
    @property
    def registered_task_function(self):
        return self._registered_task_function
    @property
    def kwargs(self):
        return self._kwargs
    @property
    def task_id(self):
        return self._task_id
    @property
    def task_function_type(self):
        return self._registered_task_function.task_function_type
    def run(self):
        return self._registered_task_function.run(kwargs=self._kwargs)
    @property
    def status(self):
        return self._status
    def update_status(self, *, status: str, error_message: Union[str, None]=None, result: Union[Any, None]=None):
        self._status = status
        _update_task_status(channel=self.registered_task_function.channel, task_id=self.task_id, task_function_type=self.task_function_type, status=status, result=result, error_message=error_message)