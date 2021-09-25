from typing import Callable

from numpy import floor
from ._update_task_status import _update_task_status


def _run_task(task_function: Callable, kwargs: dict, *, channel: str):
    return_value = task_function(**kwargs)
    if hasattr(return_value, 'wait'):
        return_value = return_value.wait().return_value
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
    # the following is important so that 1000.0 gets serialized as 1000 (like javascript does it)
    x = _replace_float_by_int_when_appropriate(x)
    txt = simplejson.dumps(x, separators=separators, indent=None, allow_nan=False, sort_keys=True)
    m = hashlib.sha1()
    m.update(txt.encode())
    return m.hexdigest()

def _replace_float_by_int_when_appropriate(x):
    if isinstance(x, float):
        if x == floor(x):
            return int(x)
    elif isinstance(x, dict):
        ret = {}
        for k in x.keys():
            ret[k] = _replace_float_by_int_when_appropriate(x[k])
        return ret
    elif isinstance(x, tuple):
        return tuple([_replace_float_by_int_when_appropriate(a) for a in x])
    elif isinstance(x, list):
        return [_replace_float_by_int_when_appropriate(a) for a in x]
    else:
        return x
