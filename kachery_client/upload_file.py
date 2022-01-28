import hashlib
from typing import Any, Union
import numpy as np

from kachery_client.main import store_json, store_npy, store_text, store_pkl
from ._daemon_connection import _daemon_url, _connected_to_daemon
from ._load_file import _load_file, _load_json
from ._misc import _http_post_json, _parse_kachery_uri
from ._store_file import _store_file
from .task_backend._update_task_status import _http_put_bytes
from .enable_ephemeral import _use_ephemeral


def upload_file(path_or_uri: str, *, channel: str, single_chunk: bool=False) -> str:
    if path_or_uri.startswith('sha1://'):
        uri = path_or_uri
    else:
        uri = _store_file(path_or_uri)
    protocol, algorithm, hash0, additional_path, query = _parse_kachery_uri(uri)
    sha1 = hash0
    local_fname = _load_file(uri, local_only=True)
    if local_fname is None:
        raise Exception(f'Unable to find file locally: {uri}')

    if ('manifest' in query) and (not single_chunk):
        manifest_sha1 = query['manifest'][0]
        upload_file(f'sha1://{manifest_sha1}', channel=channel)
        manifest = _load_json(f'sha1://{manifest_sha1}')
        chunks = manifest['chunks']
        for ii, chunk in enumerate(chunks):
            print(f'Uploading chunk {ii} of {len(chunks)}')
            chunk_start = chunk['start']
            chunk_end = chunk['end']
            chunk_sha1 = chunk['sha1']
            with open(local_fname, 'rb') as f:
                f.seek(chunk_start)
                chunk_data = f.read(chunk_end - chunk_start)
                computed_sha1 = _sha1_of_data(chunk_data)
                if computed_sha1 != chunk_sha1:
                    raise Exception('Unexpected sha1 of chunk')
                _upload_file_content(file_content=chunk_data, sha1=chunk_sha1, channel=channel)
        return uri
    
    with open(local_fname, 'rb') as f:
        file_content = f.read()

    _upload_file_content(file_content=file_content, sha1=sha1, channel=channel)
    return uri

def upload_json(x: Union[dict, list, int, float, str], *, channel: str, basename: Union[str, None]=None, single_chunk: bool=False) -> str:
    uri = store_json(x, basename=basename)
    return upload_file(uri, channel=channel, single_chunk=single_chunk)

def upload_text(x: str, *, channel: str, basename: Union[str, None]=None, single_chunk: bool=False) -> str:
    uri = store_text(x, basename=basename)
    return upload_file(uri, channel=channel, single_chunk=single_chunk)

def upload_npy(x: np.ndarray, *, channel: str, basename: Union[str, None]=None, single_chunk: bool=False) -> str:
    uri = store_npy(x, basename=basename)
    return upload_file(uri, channel=channel, single_chunk=single_chunk)

def upload_pkl(x: Any, *, channel: str, basename: Union[str, None]=None, single_chunk: bool=False) -> str:
    uri = store_pkl(x, basename=basename)
    return upload_file(uri, channel=channel, single_chunk=single_chunk)

def _sha1_of_data(data: bytes):
    m = hashlib.sha1()
    m.update(data)
    return m.hexdigest()

def _url_exists(url: str):
    import requests
    r = requests.head(url)
    return r.status_code == requests.codes.ok

def _upload_file_content(*, file_content: bytes, sha1: str, channel: str) -> None:
    # first check if already in bucket
    from .direct_client.DirectClient import _get_bucket_base_url
    bucket_base_url = _get_bucket_base_url(channel)
    file_url = f'{bucket_base_url}/{channel}/sha1/{sha1[0]}{sha1[1]}/{sha1[2]}{sha1[3]}/{sha1[4]}{sha1[5]}/{sha1}'
    if _url_exists(file_url):
        return

    if _use_ephemeral():
        from .ephemeral.ephemeral_upload_file import ephemeral_upload_file
        return ephemeral_upload_file(sha1=sha1, file_content=file_content, channel=channel)
    if not _connected_to_daemon():
        raise Exception('Not connected to daemon and not in ephemeral mode.')
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/createSignedFileUploadUrl'
    req = {
        'channelName': channel,
        'sha1': sha1,
        'size': len(file_content)
    }
    resp = _http_post_json(url, req, headers=headers)
    success = resp['success']
    assert success, 'Problem creating signed file upload url'
    already_uploaded = resp['alreadyUploaded']
    if already_uploaded:
        return
    signed_url = resp['signedUrl']

    _http_put_bytes(signed_url, file_content)