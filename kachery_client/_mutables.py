from typing import Union
from ._daemon_connection import _daemon_url
from ._misc import _http_post_json


def _set(key: Union[str, dict, list], value: Union[str, dict, list], update=True) -> bool:
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/mutable/set'
    req = {'key': key, 'value': value}
    if update == False:
        req['update'] = update # don't include update if True to be compatible with old daemon
    x = _http_post_json(url, req, headers=headers)
    if 'success' not in x:
        raise Exception(f'Unexpected problem setting value for key: {key}')
    return x['success']

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
    if 'success' not in x:
        raise Exception(f'Unexpected problem deleting value for key: {key}')
    return x['success']
        