from typing import Union
from ._daemon_connection import _daemon_url
from ._misc import _http_post_json


def _set(key: Union[str, dict, list], value: Union[str, dict, list]):
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/mutable/set'
    x = _http_post_json(url, dict(
        key=key,
        value=value
    ), headers=headers)
    if not x['success']:
        raise Exception(f'Unable to set value for key: {key}')

def _get(key: Union[str, dict, list]):
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/mutable/get'
    x = _http_post_json(url, dict(
        key=key
    ), headers=headers)
    if not x['success']:
        raise Exception(f'Unable to get value for key: {key}')
    found = x['found']
    if found:
        return x['value']
    else:
        return None

def _delete(key: Union[str, dict, list]):
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/mutable/delete'
    x = _http_post_json(url, dict(
        key=key
    ), headers=headers)
    if not x['success']:
        raise Exception(f'Unable to delete value for key: {key}')