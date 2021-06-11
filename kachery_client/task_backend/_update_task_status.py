import requests
from typing import Any, Union, cast
import json
from .._daemon_connection import _daemon_url
from .._misc import _http_post_json


def _update_task_status(*, channel: str, task_hash: str, status: str, result: Union[Any, None]=None, error_message: Union[str, None]=None):
    if status == 'finished':
        if result is None:
            raise Exception('No result provided even though status is finished')
        result_content = json.dumps(result).encode()
        signed_url = _create_signed_task_result_upload_url(channel=channel, task_hash=task_hash, size=len(result_content))
        _http_put_bytes(signed_url, result_content)
    else:
        if result is not None:
            raise Exception('Result is not none even though status is not finished')
    
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/task/updateTaskStatus'
    req_data = {
        'channelName': channel,
        'taskHash': task_hash,
        'status': status
    }
    if error_message is not None:
        req_data['errorMessage'] = error_message
    x = _http_post_json(url, req_data, headers=headers)
    if not x['success']:
        raise Exception(f'Unable to update task status')

def _create_signed_task_result_upload_url(*, channel: str, task_hash: str, size: float):
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/task/createSignedTaskResultUploadUrl'
    req_data = {
        'channelName': channel,
        'taskHash': task_hash,
        'size': size
    }
    x = _http_post_json(url, req_data, headers=headers)
    if not x['success']:
        raise Exception(f'Unable to create signed task result upload url')
    return cast(str, x['signedUrl'])

def _http_put_bytes(url: str, data: bytes):
    headers = {
        'Content-Type': 'application/octet-stream',
        'Content-Length': f'{len(data)}'
    }
    req = requests.put(url, data=data, headers=headers)
    try:
        if req.status_code != 200:
            raise Exception(f'Error putting data: {req.status_code} {req.content.decode("utf-8")}')
    finally:
        req.close()
