import sys
import os
import shutil
from typing import Any, Union
import simplejson
import numpy as np
from ._daemon_connection import _daemon_url, _kachery_storage_dir, _connected_to_daemon
from ._misc import _create_file_key, _http_post_json_receive_json_socket, _parse_kachery_uri
from ._exceptions import LoadFileError
from ._local_kachery_storage import _local_kachery_storage_load_file, _local_kachery_storage_load_bytes
from ._safe_pickle import _safe_unpickle
from .enable_ephemeral import _use_ephemeral

def _load_file(uri: str, dest: Union[str, None]=None, *, local_only: bool=False, channel: Union[str, None]=None) -> Union[str, None]:
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

    if _use_ephemeral():
        from .ephemeral.ephemeral_load_file import ephemeral_load_file
        local_fname = ephemeral_load_file(uri, local_only=local_only, channel=channel)
        if local_fname is None:
            return None
        if dest is not None:
            shutil.copyfile(local_fname, dest)
            return dest
        else:
            return local_fname

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
    if not _connected_to_daemon():
        raise Exception('Not connected to a kachery daemon and not in ephemeral mode.')
    
    if local_only:
        return None
    
    if channel is None:
        return None
    
    protocol, algorithm, hash0, additional_path, query = _parse_kachery_uri(uri)
    assert algorithm == 'sha1'
    file_key = _create_file_key(sha1=hash0, query=query)
    daemon_url, headers = _daemon_url()
    url = f'{daemon_url}/loadFile'
    sock, req = _http_post_json_receive_json_socket(url, dict(
        fileKey=file_key,
        channelName=channel
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
                if bytes_total > 0:
                    pct = (bytes_loaded / bytes_total) * 100
                else:
                    pct = 100
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

def _load_json(uri: str, *, local_only: bool=False, channel: Union[str, None]=None) -> Union[dict, None]:
    local_path = _load_file(uri, local_only=local_only, channel=channel)
    if local_path is None:
        return None
    with open(local_path, 'r') as f:
        return simplejson.load(f)

def _load_text(uri: str, *, local_only: bool=False, channel: Union[str, None]=None) -> Union[str, None]:
    local_path = _load_file(uri, local_only=local_only, channel=channel)
    if local_path is None:
        return None
    with open(local_path, 'r') as f:
        return f.read()

def _load_npy(uri: str, *, local_only: bool=False, channel: Union[str, None]=None) -> Union[np.ndarray, Any, None]:
    local_path = _load_file(uri, local_only=local_only, channel=channel)
    if local_path is None:
        return None
    return np.load(local_path, allow_pickle=False)

def _load_pkl(uri: str, *, local_only: bool=False, channel: Union[str, None]=None) -> Union[np.ndarray, None]:
    local_path = _load_file(uri, local_only=local_only, channel=channel)
    if local_path is None:
        return None
    return _safe_unpickle(local_path)

def _load_bytes(uri: str, start: Union[int, None], end: Union[int, None], *, write_to_stdout=False, local_only: bool=False, channel: Union[str, None]=None) -> Union[bytes, None]: 
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
    
    if local_only:
        return None
    
    if channel is None:
        return None
    
    protocol, algorithm, hash0, additional_path, query = _parse_kachery_uri(uri)
    if query.get('manifest'):
        manifest = _load_json(f'sha1://{query["manifest"][0]}', channel=channel)
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
            chunk_path = _load_file(chunk_uri, channel=channel)
            if chunk_path is None:
                print(f'Problem loading chunk: {chunk_uri}')
                return None
            start_byte = max(0, start - ch['start'])
            end_byte = min(ch['end']-ch['start'], end-ch['start'])
            a = _load_bytes(
                uri=chunk_path,
                start=start_byte,
                end=end_byte,
                channel=channel
            )
            if a is None:
                print(f'Unable to load bytes from chunk: {chunk_path} (start={start_byte}; end={end_byte})')
                return None
            data_chunks.append(a)
        return b''.join(data_chunks)
    
    path = _load_file(uri=uri, channel=channel)
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