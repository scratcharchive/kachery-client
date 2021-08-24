from typing import Callable
from ._update_task_status import _update_task_status


def _run_task(task_function: Callable, kwargs: dict, *, channel: str):
    return_value = task_function(**kwargs)
    task_function_id = getattr(task_function, '_task_function_id')
    task_function_type = getattr(task_function, '_task_function_type')
    assert task_function_type == 'pure-calculation', 'Not a pure calculation task'
    task_hash = _compute_task_hash(task_function_id=task_function_id, kwargs=kwargs)
    _update_task_status(
        channel=channel,
        task_id=task_hash,
        task_function_id=task_function_id,
        task_hash=task_hash,
        task_function_type=task_function_type,
        status='finished',
        result=return_value,
        error_message=None
    )
    return return_value

def _compute_task_hash(task_function_id: str, kwargs: dict):
    task_data = {
        'functionId': task_function_id,
        'kwargs': kwargs
    }
    return _sha1_of_object(task_data)

def _sha1_of_object(x: dict):
    import simplejson
    import hashlib
    separators=(',', ':')
    txt = simplejson.dumps(x, separators=separators, indent=None, allow_nan=False, sort_keys=True)
    m = hashlib.sha1()
    m.update(txt.encode())
    return m.hexdigest()