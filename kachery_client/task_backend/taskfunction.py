from typing import Any, Callable, Dict, List, Union

_global_registered_taskfunctions_by_function_id: Dict[str, Callable] = {}

def find_taskfunction(function_id: str) -> Union[Callable, None]:
    if function_id in _global_registered_taskfunctions_by_function_id:
        return _global_registered_taskfunctions_by_function_id[function_id]
    else:
        return None

def all_taskfunction_ids() -> List[str]:
    return list(_global_registered_taskfunctions_by_function_id.keys())

def taskfunction(function_id: str, *, type: str):
    def wrap(f: Callable[..., Any]):
        setattr(f, '_task_function_type', type)
        setattr(f, '_task_function_id', function_id)
        _global_registered_taskfunctions_by_function_id[function_id] = f
        return f
    return wrap