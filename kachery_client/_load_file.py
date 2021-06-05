import sys
import os
import shutil
from typing import Any, Union
import simplejson
import numpy as np
from ._daemon_connection import _is_offline_mode, _is_online_mode, _daemon_url, _kachery_storage_dir
from ._misc import _create_file_key, _http_post_json_receive_json_socket, _parse_kachery_uri
from ._exceptions import LoadFileError
from ._local_kachery_storage import _local_kachery_storage_load_file, _local_kachery_storage_load_bytes
from ._safe_pickle import _safe_unpickle

def _load_file(uri: str, dest: Union[str, None]=None, local_only: bool=False) -> Union[str, None]:
    # handle old sha1dir system
    if uri.startswith('sha1dir://'):
        uri0 = _resolve_file_uri_from_dir_uri(uri)
        if uri0 is None:
            return None
        uri = uri0
    
    if not uri.startswith('sha1://'):
        if os.path.isfile(uri):
            local_path = uri
            if dest is not None:
                shutil.copyfile(local_path, dest)
                return dest
            else:
                return local_path
        else:
            raise Exception(f'Local file not found: {uri}')

    # first check the local kachery storage (if kachery storage dir is known)
    if _kachery_storage_dir():
        if True: # for debugging (not loading locally) switch to false
            protocol, algorithm, hash0, additional_path, query = _parse_kachery_uri(uri)
            if protocol != 'sha1':
                raise Exception(f'Protocol not supported: {protocol}')
            local_path = _local_kachery_storage_load_file(sha1_hash=hash0)
            if local_path is not None:
                if dest is not None:
                    shutil.copyfile(local_path, dest)
                    return dest
                else:
                    return local_path
    if _is_offline_mode():
        return None
    if not _is_online_mode():
        raise Exception('Not connected to daemon, and KACHERY_OFFLINE_STORAGE_DIR environment variable is not set.')
    
    if local_only:
        return None
    
    protocol, algorithm, hash0, additional_path, query = _parse_kachery_uri(uri)
    assert algorithm == 'sha1'
    file_key = _create_file_key(sha1=hash0, query=query)
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/loadFile'
    sock, req = _http_post_json_receive_json_socket(url, dict(
        fileKey=file_key
    ), headers=headers)
    try:
        for r in sock:
            try:
                type0 = r.get('type')
            except:
                raise Exception(f'Unexpected response from daemon: {r}: {uri}')
            if type0 == 'finished':
                print(f'Loaded file: {uri}')
                local_file_path: str = r['localFilePath']
                if not os.path.exists(local_file_path):
                    raise Exception(f'Unexpected in load_file: file does not exist: {local_file_path}')
                return local_file_path
            elif type0 == 'progress':
                bytes_loaded = r['bytesLoaded']
                bytes_total = r['bytesTotal']
                pct = (bytes_loaded / bytes_total) * 100
                print(f'Loaded {bytes_loaded} of {bytes_total} bytes ({pct:.1f} %): {uri}')
            elif type0 == 'error':
                return None
                # raise LoadFileError(f'Error loading file: {r["error"]}: {uri}')
            else:
                raise Exception(f'Unexpected message from daemon: {r}')
        # for url in _global_config['file_server_urls']:
        #     try:
        #         path = _load_file_from_file_server(uri=uri, dest=dest, file_server_url=url)
        #     except Exception as e:
        #         print(str(e))
        #         path = None
        #     if path:
        #         return path
        raise Exception(f'Unable to download file: {uri}')
    finally:
        req.close()

def _load_json(uri: str, local_only: bool=False) -> Union[dict, None]:
    local_path = _load_file(uri, local_only=local_only)
    if local_path is None:
        return None
    with open(local_path, 'r') as f:
        return simplejson.load(f)

def _load_text(uri: str, local_only: bool=False) -> Union[str, None]:
    local_path = _load_file(uri, local_only=local_only)
    if local_path is None:
        return None
    with open(local_path, 'r') as f:
        return f.read()

def _load_npy(uri: str, local_only: bool=False) -> Union[np.ndarray, Any, None]:
    local_path = _load_file(uri, local_only=local_only)
    if local_path is None:
        return None
    return np.load(local_path, allow_pickle=False)

def _load_pkl(uri: str, local_only: bool=False) -> Union[np.ndarray, None]:
    local_path = _load_file(uri, local_only=local_only)
    if local_path is None:
        return None
    return _safe_unpickle(local_path)

def _load_bytes(uri: str, start: Union[int, None], end: Union[int, None], write_to_stdout=False, local_only: bool=False) -> Union[bytes, None]:
    # handle old sha1dir system
    if uri.startswith('sha1dir://'):
        uri0 = _resolve_file_uri_from_dir_uri(uri)
        if uri0 is None:
            return None
        uri = uri0
    
    if not uri.startswith('sha1://'):
        if os.path.isfile(uri):
            local_path = uri
            return _load_bytes_from_local_file(local_path, start=start, end=end, write_to_stdout=write_to_stdout)
        else:
            raise Exception(f'Local file not found: {uri}')
    
    # first check the local kachery storage (if kachery storage dir is known)
    if _kachery_storage_dir():
        protocol, algorithm, hash0, additional_path, query = _parse_kachery_uri(uri)
        if protocol != 'sha1':
            raise Exception(f'Protocol not supported: {protocol}')
        bytes0 = _local_kachery_storage_load_bytes(sha1_hash=hash0, start=start, end=end, write_to_stdout=write_to_stdout)
        if bytes0 is not None:
            return bytes0
    
    if _is_offline_mode():
        return None
    if not _is_online_mode():
        raise Exception('Not connected to daemon, and KACHERY_OFFLINE_STORAGE_DIR environment variable is not set.')

    if local_only:
        return None
    
    protocol, algorithm, hash0, additional_path, query = _parse_kachery_uri(uri)
    if query.get('manifest'):
        manifest = _load_json(f'sha1://{query["manifest"][0]}')
        if manifest is None:
            print('Unable to load manifest')
            return None
        assert manifest['sha1'] == hash0, 'Manifest sha1 does not match expected.'
        data_chunks = []
        chunks_to_load = []
        for ch in manifest['chunks']:
            if start < ch['end'] and end > ch['start']:
                chunks_to_load.append(ch)
        for ii, ch in enumerate(chunks_to_load):
            if len(chunks_to_load) > 4:
                print(f'load_bytes: Loading chunk {ii + 1} of {len(chunks_to_load)}')
            chunk_uri = f'sha1://{ch["sha1"]}?chunkOf={hash0}~{ch["start"]}~{ch["end"]}'
            chunk_path = _load_file(chunk_uri)
            if chunk_path is None:
                print(f'Problem loading chunk: {chunk_uri}')
                return None
            start_byte = max(0, start - ch['start'])
            end_byte = min(ch['end']-ch['start'], end-ch['start'])
            a = _load_bytes(
                uri=chunk_path,
                start=start_byte,
                end=end_byte
            )
            if a is None:
                print(f'Unable to load bytes from chunk: {chunk_path} (start={start_byte}; end={end_byte})')
                return None
            data_chunks.append(a)
        return b''.join(data_chunks)
    
    path = _load_file(uri=uri)
    if path is None:
        print('Unable to load file.')
        return None
    bytes0 = _local_kachery_storage_load_bytes(sha1_hash=hash0, start=start, end=end, write_to_stdout=write_to_stdout)

def _load_bytes_from_local_file(local_fname: str, *, start: Union[int, None]=None, end: Union[int, None]=None, write_to_stdout: bool=False) -> Union[bytes, None]:
    size0 = os.path.getsize(local_fname)
    if start is None:
        start = 0
    if end is None:
        end = size0
    if start < 0 or start > size0 or end < start or end > size0:
        raise Exception('Invalid start/end range for file of size {}: {} - {}'.format(size0, start, end))
    if start == end:
        return bytes()
    with open(local_fname, 'rb') as f:
        f.seek(start)
        if write_to_stdout:
            ii = start
            while ii < end:
                nn = min(end - ii, 4096)
                data0 = f.read(nn)
                ii = ii + nn
                sys.stdout.buffer.write(data0)
            return None
        else:
            return f.read(end-start)

def _resolve_file_uri_from_dir_uri(dir_uri, local_only: bool=False):
    protocol, algorithm, hash0, additional_path, query = _parse_kachery_uri(dir_uri)
    assert protocol == algorithm + 'dir'
    dd = _load_json(algorithm + '://' + hash0, local_only=local_only)
    if dd is None:
        return None
    if additional_path:
        list0 = additional_path.split('/')
    else:
        list0 = []
    ii = 0
    while ii < len(list0):
        assert dd is not None
        name0 = list0[ii]
        if name0 in dd['dirs']:
            dd = dd['dirs'][name0]
        elif name0 in dd['files']:
            if ii + 1 == len(list0):
                hash1 = None
                algorithm1 = None
                for alg in ['sha1', 'md5']:
                    if alg in dd['files'][name0]:
                        hash1 = dd['files'][name0][alg]
                        algorithm1 = alg
                if hash1 is None:
                    return None
                return algorithm1 + '://' + hash1
            else:
                return None
        else:
            return None
        ii = ii + 1
    return None