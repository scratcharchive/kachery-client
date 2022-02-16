import os
import re
import time
import json
from typing import Any, Dict, Iterable, Optional, Tuple, Union
from urllib.parse import parse_qs

def _parse_string_to_protocol_and_basename(string: str) -> Union[Tuple[str, str], None]:
    expr = re.compile('^(https?://)?(.*?)/?$')
    match = expr.match(string)
    if (not match):
        return None
    protocol = match.group(1) or 'https'
    if (protocol[-3:] == '://'):
        protocol = protocol[:-3]
    uri = match.group(2)
    if (':' in uri or '/' in uri):
        return None
    return (protocol, uri)

def _get_envvar_uri(environment_var_name: str, default: str) -> Tuple[str, str]:
    endpoint = os.getenv(environment_var_name, default)
    res = _parse_string_to_protocol_and_basename(endpoint)
    if (not res):
        raise Exception(f'Error: invalid value for environment variable {environment_var_name} ({endpoint})')
    return res

def _get_kachery_hub_uri(with_protocol=True) -> str:
    (protocol, uri) = _get_envvar_uri('KACHERY_HUB_API_URI', 'kacheryhub.org')
    if (with_protocol):
        return f'{protocol}://{uri}'
    return uri

def _get_bitwooder_uri(with_protocol=True) -> str:
    (protocol, uri) = _get_envvar_uri('BITWOODER_API_URI', 'bitwooder.net')
    if (with_protocol):
        return f'{protocol}://{uri}'
    return uri

def _parse_kachery_uri(uri: str) -> Tuple[str, str, str, str, dict]:
    listA = uri.split('?')
    if len(listA) > 1:
        query = parse_qs(listA[1])
    else:
        query = {}
    list0 = listA[0].split('/')
    protocol = list0[0].replace(':', '')
    hash0 = list0[2]
    if '.' in hash0:
        hash0 = hash0.split('.')[0]
    additional_path = '/'.join(list0[3:])
    algorithm = None
    for alg in ['sha1', 'md5', 'key']:
        if protocol.startswith(alg):
            algorithm = alg
    if algorithm is None:
        raise Exception('Unexpected protocol: {}'.format(protocol))
    return protocol, algorithm, hash0, additional_path, query

def _http_post_json(url: str, data: dict, verbose: Optional[bool] = None, headers: dict = {}) -> dict:
    timer = time.time()
    if verbose is None:
        verbose = (os.environ.get('HTTP_VERBOSE', '') == 'TRUE')
    if verbose:
        print('_http_post_json::: ' + url)
    try:
        import requests
    except:
        raise Exception('Error importing requests *')
    req = requests.post(url, json=data, headers=headers)
    try:
        if req.status_code != 200:
            return dict(
                success=False,
                error='Error posting json: {} {}'.format(
                    req.status_code, req.content.decode('utf-8'))
            )
        if verbose:
            print('Elapsed time for _http_post_json: {}'.format(time.time() - timer))
        return json.loads(req.content)
    finally:
        req.close()

def _http_post_json_receive_json_socket(url: str, data: dict, verbose: Optional[bool] = None, headers: dict = {}) -> Tuple[Iterable[dict], Any]:
    timer = time.time()
    if verbose is None:
        verbose = (os.environ.get('HTTP_VERBOSE', '') == 'TRUE')
    if verbose:
        print('_http_post_json::: ' + url)
    try:
        import requests
    except:
        raise Exception('Error importing requests *')
    req = requests.post(url, json=data, stream=True, headers=headers)
    if req.status_code != 200:
        raise Exception('Error posting json: {} {}'.format(req.status_code, req.content.decode('utf-8')))
    class custom_iterator:
        def __init__(self):
            pass

        def __iter__(self):
            return self

        def __next__(self):
            buf = bytearray(b'')
            while True:
                c = req.raw.read(1)
                if len(c) == 0:
                    raise StopIteration
                if c == b'#':
                    size = int(buf)
                    x = req.raw.read(size)
                    obj = json.loads(x)
                    return obj
                else:
                    buf.append(c[0])
    return custom_iterator(), req

def _http_get_json(url: str, verbose: Optional[bool] = None, headers: dict = {}) -> dict:
    timer = time.time()
    if verbose is None:
        verbose = (os.environ.get('HTTP_VERBOSE', '') == 'TRUE')
    if verbose:
        print('_http_get_json::: ' + url)
    try:
        import requests
    except:
        raise Exception('Error importing requests *')
    req = requests.get(url, headers=headers)
    try:
        if req.status_code != 200:
            return dict(
                success=False,
                error='Error getting json: {} {}'.format(
                    req.status_code, req.content.decode('utf-8'))
            )
        if verbose:
            print('Elapsed time for _http_get_json: {}'.format(time.time() - timer))
        return json.loads(req.content)
    finally:
        req.close()

def _http_post_file(url: str, file_path: str, headers: dict = {}) -> dict:
    try:
        import requests
    except:
        raise Exception('Error importing requests *')
    with open(file_path, 'rb') as f:
        req = requests.post(url, data=f, headers=headers)
    try:
        if req.status_code != 200:
            raise Exception(f'Error posting file: {url} {file_path}')
        return json.loads(req.content)
    finally:
        req.close()

def _create_file_key(*, sha1, query):
    file_key: Dict[str, Union[str, dict]] = dict(
        sha1=sha1
    )
    if 'manifest' in query:
        file_key['manifestSha1'] = query['manifest'][0]
    if 'chunkOf' in query:
        v = query['chunkOf'][0].split('~')
        assert len(v) ==3, 'Unexpected chunkOf in URI query.'
        file_key['chunkOf'] = {
            'fileKey': {
                'sha1': v[0]
            },
            'startByte': int(v[1]),
            'endByte': int(v[2])
        }
    return file_key