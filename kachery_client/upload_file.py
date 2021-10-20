from ._daemon_connection import (_daemon_url)
from ._load_file import _load_file
from ._misc import _http_post_json, _parse_kachery_uri
from ._store_file import _store_file
from .task_backend._update_task_status import _http_put_bytes


def upload_file(path_or_uri: str, *, channel: str) -> str:
    if path_or_uri.startswith('sha1://'):
        uri = path_or_uri
    else:
        uri = _store_file(path_or_uri)
    protocol, algorithm, hash0, additional_path, query = _parse_kachery_uri(uri)
    sha1 = hash0

    local_fname = _load_file(uri, local_only=True)
    with open(local_fname, 'r') as f:
        file_content = f.read()

    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/createSignedFileUploadUrl'
    req = {
        'channelName': channel,
        'sha1': sha1
    }
    resp = _http_post_json(url, req, headers=headers)
    success = resp['success']
    assert success, 'Problem creating signed file upload url'
    already_uploaded = resp['alreadyUploaded']
    if already_uploaded:
        return
    signed_url = resp['signedUrl']

    _http_put_bytes(signed_url, file_content)
