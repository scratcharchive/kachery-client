import json
import atexit
from kachery_client.task_backend.TaskJobManager import TaskJobManager
import multiprocessing
import random
from typing import Dict, List, Protocol, cast

from .RegisteredTaskFunction import RegisteredTaskFunction
from .RequestedTask import RequestedTask

from ._run_task_backend_worker import _run_task_backend_worker


class OnRequestedTaskCallback(Protocol):
    def __call__(self, requested_task: RequestedTask):
        pass

class TaskBackend:
    def __init__(self, registered_task_functions: List[RegisteredTaskFunction]):
        self._task_backend_id = _random_string(10)

        self._task_job_manager = TaskJobManager()
        
        run_task_backend_pipe_to_parent, run_task_backend_pipe_to_child = multiprocessing.Pipe()
        self._run_task_backend_worker_process =  multiprocessing.Process(target=_run_task_backend_worker, args=(run_task_backend_pipe_to_parent, registered_task_functions))
        self._run_task_backend_pipe_to_worker = run_task_backend_pipe_to_child

        self._on_requested_task_callbacks: List[OnRequestedTaskCallback] = []
    def start(self):
        _running_task_backends[self._task_backend_id] = self
        self._run_task_backend_worker_process.start()
    def stop(self):
        self._run_task_backend_pipe_to_worker.send({'type': 'exit'})
        self._run_task_backend_worker_process.join()

        if self._task_backend_id in _running_task_backends:
            del _running_task_backends[self._task_backend_id]
    def process_events(self):
        if self._run_task_backend_pipe_to_worker.poll():
            msg = self._run_task_backend_pipe_to_worker.recv()
            type0 = msg['type']
            if type0 == 'request_task':
                requested_task = cast(RequestedTask, msg['requested_task'])
                self._handle_requested_task(requested_task)
            else:
                raise Exception(f'Unexpected message type in task backend: {type0}')
        self._task_job_manager.process_events()
    def _handle_requested_task(self, requested_task: RequestedTask):
        task_job_manager = self._task_job_manager
        existing_job_for_task = task_job_manager.get_existing_job_for_task(requested_task)
        function_id = requested_task.registered_task_function.task_function_id
        function_type = requested_task.registered_task_function.task_function_type
        print(f'Task requested: {function_id} ({function_type})')
        if existing_job_for_task is not None:
            return
        try:
            task_output = requested_task.run()
        except Exception as e:
            error_message = f'Error calling task: {str(e)}'
            print(f'Error in {function_id}: {error_message}')
            requested_task.update_status(status='error', error_message=error_message)
            return
        if hasattr(task_output, 'status'):
            # we assume the output is a hither function
            # only import hither if we are using hither functions
            import hither2 as hi
            if isinstance(task_output, hi.Job):
                task_job_manager.add_task_job(requested_task=requested_task, job=task_output)
            else:
                raise Exception('Task output is not a hither job.')
        else:
            try:
                json.dumps(task_output)
            except:
                error_message = f'Error json-serializing result'
                print(f'Error in {function_id}: {error_message}')
                requested_task.update_status(status='error', error_message=error_message)
                return
            print(f'Finished task: {function_id}')
            requested_task.update_status(status='finished', result=task_output)

_running_task_backends: Dict[str, TaskBackend] = {}

def _random_string(num_chars: int) -> str:
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(random.choice(chars) for _ in range(num_chars))

def _stop_all_task_backends():
    x = list(_running_task_backends.values())
    for s in x:
        s.stop()


atexit.register(_stop_all_task_backends)
