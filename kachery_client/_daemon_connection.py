import os
import time
import tempfile
from typing import List, Union, cast
from ._misc import _http_get_json

def _daemon_port():
    return os.getenv('KACHERY_DAEMON_PORT', 20431)

def _daemon_host():
    return os.getenv('KACHERY_DAEMON_HOST', 'localhost')

_client_auth_code_info = {
    'timestamp': 0,
    'code': ''
}

def _get_client_auth_code():
    elapsed = time.time() - _client_auth_code_info['timestamp']
    if elapsed > 60:
        _client_auth_code_info['code'] = _read_client_auth_code()
        _client_auth_code_info['timestamp'] = time.time()
    return _client_auth_code_info['code']

def _read_client_auth_code():
    ksd = _kachery_storage_dir()
    p = f'{ksd}/client-auth'
    if not os.path.isfile(p):
        raise Exception(f'Unable to find client auth file (perhaps daemon is not running): {p}')
    try:
        with open(p, 'r') as f:
            client_auth_code = f.read()
    except:
        raise Exception(f'Unable to read client auth file. Perhaps you do not have permission to access this daemon.')
    return client_auth_code


def _daemon_url(daemon_port=None, daemon_host=None, no_client_auth=False):
    if daemon_port is not None:
        port = daemon_port
    else:
        port = _daemon_port()
    if daemon_host is not None:
        host = daemon_host
    else:
        host = _daemon_host()
    if not no_client_auth:
        headers = {
            'KACHERY-CLIENT-AUTH-CODE': _get_client_auth_code()
        }
    else:
        headers = {}
    return f'http://{host}:{port}', headers

class _probe_result:
    def __init__(self, x: dict):
        self.probe_response = cast(dict, x)
        self.node_id = cast(str, x['nodeId'])
        ksd = os.getenv('KACHERY_STORAGE_DIR', cast(Union[str, None], x['kacheryStorageDir'] or None))
        if ksd is None:
            raise Exception('No kachery storage directory.')
        if not os.path.exists(ksd):
            raise Exception(f'Kachery storage directory does not exist: {ksd}')
        fname = f'{ksd}/kachery-node-id'
        if not os.path.exists(fname):
            raise Exception(f'File does not exist (perhaps daemon is not running or perhaps you are using an inconsistent version of kachery between daemon and client): {fname}')
        with open(fname, 'r') as f:
            node_id_from_file = f.read()
            if node_id_from_file != self.node_id:
                raise Exception(f'Inconsistent node ID between running daemon and kachery storage directory: {node_id_from_file} <> {self.node_id} ({fname})')
        self.kachery_storage_dir = ksd

class _buffered_probe_data:
    timestamp: float=0
    result: Union[None, _probe_result]=None

def _buffered_probe_daemon(daemon_port=None):
    elapsed_since_last = time.time() - _buffered_probe_data.timestamp
    if elapsed_since_last <= 10:
        return _buffered_probe_data.result
    res = _probe_daemon(daemon_port=daemon_port)
    _buffered_probe_data.timestamp = time.time()
    _buffered_probe_data.result = res
    return _buffered_probe_data.result

def _probe_daemon(daemon_port=None):
    daemon_url, headers = _daemon_url(daemon_port=daemon_port, no_client_auth=True)
    url = f'{daemon_url}/probe'
    try:
        x = _http_get_json(url)
    except Exception as e:
        return None
    res = _probe_result(x) if x is not None else None
    return res

def _kachery_offline_storage_dir_env_is_set():
    return os.getenv('KACHERY_OFFLINE_STORAGE_DIR', None) is not None

def _kachery_storage_dir():
    if _kachery_offline_storage_dir_env_is_set():
        return os.getenv('KACHERY_OFFLINE_STORAGE_DIR', None)
    else:
        p = _buffered_probe_daemon()
        if p is not None:
            return p.kachery_storage_dir
        else:
            return None

def _create_if_needed(dirpath: str) -> str:
    if not os.path.isdir(dirpath):
        try:
            os.mkdir(dirpath)
        except:
            # in case it was created elsewhere
            if not os.path.isdir(dirpath):
                raise Exception(f'Failed to create dir: {dirpath}')
    return dirpath

def _kachery_temp_dir() -> str:
    d = os.getenv('KACHERY_TEMP_DIR', None)
    if d is not None:
        return _create_if_needed(d)
    if _kachery_offline_storage_dir_env_is_set():
        return _create_if_needed(os.getenv('KACHERY_OFFLINE_STORAGE_DIR') + '/kachery-tmp')
    else:
        return _create_if_needed(tempfile.gettempdir() + '/kachery-tmp')

def _is_offline_mode():
    return _kachery_offline_storage_dir_env_is_set()
        
def _is_online_mode():
    if _is_offline_mode():
        return False
    return _kachery_storage_dir() is not None

def _get_node_id(daemon_port=None) -> str:
    x = _buffered_probe_daemon(daemon_port=daemon_port)
    assert x is not None, 'Unable to connect to daemon.'
    return x.node_id
