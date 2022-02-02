import os
import shutil
from typing import Union, Any, List
import numpy as np
from .._misc import _http_post_json, _parse_kachery_uri, _get_kachery_hub_uri
from .._temporarydirectory import TemporaryDirectory
from ..main import store_file, load_file, store_npy, store_pkl, store_text, store_json
from .._daemon_connection import _probe_daemon
from .._local_kachery_storage import _compute_file_hash
from .._safe_pickle import _safe_unpickle, _safe_pickle


_bucket_base_urls = {} # by channel

def _get_bucket_base_url(channel: str):
    endpoint = _get_kachery_hub_uri(with_protocol=True)
    if channel in _bucket_base_urls:
        return _bucket_base_urls[channel]
    response = _http_post_json(f'{endpoint}/api/getChannelBucketBaseUrl', {'channelName': channel})
    url = response['url']
    _bucket_base_urls[channel] = url
    return url

class DirectClient:
    def __init__(self, *, channel: Union[None, str]=None) -> None:
        self._channel = channel
        # get the channel bucket base url within the constructor
        # so we can throw an exception right away if there is a problem
        if self._channel is not None:
            _get_bucket_base_url(self._channel)
        # check whether we are connected to a daemon
        self._connected_to_daemon = _probe_daemon() is not None
    def load_file(
        self,
        uri: str,
        dest: Union[str, None]=None
    ) -> Union[str, None]:
        # parse the uri
        protocol, algorithm, sha1, additional_path, query = _parse_kachery_uri(uri)
        assert algorithm == 'sha1'
        if self._connected_to_daemon:
            # if we are connected to the daemon, let's first check the local kachery storage directory
            a = load_file(uri, local_only=True, dest=dest)
            if a is not None:
                return a
        else:
            # if we are not connected to daemon, get the ephemeral kachery storage directory
            kachery_storage_dir = _get_ephemeral_kachery_storage_dir()
            kachery_storage_parent_dir = f'{kachery_storage_dir}/sha1/{sha1[0]}{sha1[1]}/{sha1[2]}{sha1[3]}/{sha1[4]}{sha1[5]}'
            kachery_storage_file_name = f'{kachery_storage_parent_dir}/{sha1}'
            if os.path.exists(kachery_storage_file_name):
                # we have the file locally... return that
                if dest:
                    shutil.copyfile(kachery_storage_file_name, dest)
                    return dest
                else:
                    return kachery_storage_file_name
        if 'manifest' in query:
            # The uri has a manifest. But let's first check whether the file is stored on the bucket in its entirety
            aa = self.load_file(f'sha1://{sha1}') # no manifest included in the uri
            if aa is not None:
                return aa
            # The uri has a manifest, so we are going to load it in chunks
            manifest = self.load_json(f'sha1://{query["manifest"][0]}')
            # load the file chunks individually
            chunk_files = []
            for chunk in manifest['chunks']:
                chunk_sha1 = chunk['sha1']
                chunk_start = chunk['start']
                chunk_end = chunk['end']
                chunk_fname = self.load_file(f'sha1://{chunk_sha1}?chunkOf={sha1}~{chunk_start}~{chunk_end}')
                if chunk_fname is None:
                    return None
                chunk_files.append(chunk_fname)
            # we need to stitch the file together
            with TemporaryDirectory() as tmpdir:
                tmp_fname = f'{tmpdir}/concat.dat'
                _concatenate_file_chunks(chunk_files, tmp_fname)
                computed_sha1 = _compute_file_hash(tmp_fname, algorithm='sha1')
                if computed_sha1 != sha1:
                    raise Exception(f'Unexpected sha1 of concatenated file for {uri}')
                if self._connected_to_daemon:
                    a_uri = store_file(tmp_fname)
                    return load_file(a_uri, dest=dest)
                else:
                    if not os.path.exists(kachery_storage_parent_dir):
                        os.makedirs(kachery_storage_parent_dir)
                    shutil.copyfile(tmp_fname, kachery_storage_file_name)
                    if dest:
                        shutil.copyfile(kachery_storage_file_name, dest)
                        return dest
                    else:
                        return kachery_storage_file_name
        if self._channel is not None:
            with TemporaryDirectory() as tmpdir:
                url = _get_bucket_base_url(self._channel)
                # download to this local file:
                tmp_fname = f'{tmpdir}/file.dat'
                # download from this url:
                file_url = f'{url}/{self._channel}/sha1/{sha1[0]}{sha1[1]}/{sha1[2]}{sha1[3]}/{sha1[4]}{sha1[5]}/{sha1}'
                try:
                    _http_get_file(file_url, tmp_fname)
                except:
                    # if we didn't find the file in the bucket, return None
                    return None
                # verify that we have the correct SHA1 hash
                computed_sha1 = _compute_file_hash(tmp_fname, algorithm='sha1')
                if computed_sha1 != sha1:
                    raise Exception(f'Unexpected sha1 in downloaded file: {file_url}')

                if self._connected_to_daemon:
                    # if we are connected to the daemon, store the file in the local kachery storage
                    store_file(tmp_fname)
                    return load_file(uri, dest=dest)
                else:
                    # if we are not connected to daemon, store the file in the ephemeral local kachery storage
                    if not os.path.exists(kachery_storage_parent_dir):
                        os.makedirs(kachery_storage_parent_dir)
                    shutil.copyfile(tmp_fname, kachery_storage_file_name)
                    if dest:
                        shutil.copyfile(kachery_storage_file_name, dest)
                        return dest
                    else:
                        return kachery_storage_file_name
        else:
            # self._channel is None
            return None

    def load_json(self, uri: str) -> Union[dict, None]:
        import simplejson
        local_path = self.load_file(uri)
        if local_path is None:
            return None
        with open(local_path, 'r') as f:
            return simplejson.load(f)

    def load_text(self, uri: str) -> Union[str, None]:
        local_path = self.load_file(uri)
        if local_path is None:
            return None
        with open(local_path, 'r') as f:
            return f.read()

    def load_npy(self, uri: str) -> Union[np.ndarray, Any, None]:
        local_path = self.load_file(uri)
        if local_path is None:
            return None
        return np.load(local_path, allow_pickle=False)

    def load_pkl(self, uri: str) -> Union[np.ndarray, None]:
        local_path = self.load_file(uri)
        if local_path is None:
            return None
        return _safe_unpickle(local_path)
    
    def store_file(self, path: str, basename: Union[str, None]=None):
        if self._connected_to_daemon:
            return store_file(path, basename=basename)
        if basename is None:
            basename = 'file.dat'
        sha1 = _compute_file_hash(path, algorithm='sha1')
        uri = f'sha1://{sha1}/{basename}'
        kachery_storage_dir = _get_ephemeral_kachery_storage_dir()
        kachery_storage_parent_dir = f'{kachery_storage_dir}/sha1/{sha1[0]}{sha1[1]}/{sha1[2]}{sha1[3]}/{sha1[4]}{sha1[5]}'
        kachery_storage_file_name = f'{kachery_storage_parent_dir}/{sha1}'
        if os.path.exists(kachery_storage_file_name):
            return uri
        if not os.path.exists(kachery_storage_parent_dir):
            os.makedirs(kachery_storage_parent_dir)
        shutil.copyfile(path, kachery_storage_file_name)
        return uri
    
    def store_text(self, text: str, basename: Union[str, None]=None):
        if self._channel is not None:
            return store_text(text, basename=basename)
        if basename is None:
            basename = 'file.txt'
        with TemporaryDirectory() as tmpdir:
            fname = f'{tmpdir}/file.dat'
            with open(fname, 'w') as f:
                f.write(text)
            return self.store_file(path=fname, basename=basename)
    
    def store_json(self, object: Union[dict, list, int, float, str], basename: Union[str, None]=None, separators=(',', ':'), indent=None):
        if self._channel is not None:
            return store_json(object, basename=basename, separators=separators, indent=indent)
        import simplejson
        if basename is None:
            basename = 'file.json'
        txt = simplejson.dumps(object, separators=separators, indent=indent, allow_nan=False)
        return self.store_text(text=txt, basename=basename)
    
    def store_npy(self, array: np.ndarray, basename: Union[str, None]=None) -> str:
        if self._channel is not None:
            return store_npy(array, basename=basename)
        if basename is None:
            basename = 'file.npy'
        with TemporaryDirectory() as tmpdir:
            fname = tmpdir + '/array.npy'
            np.save(fname, array, allow_pickle=False)
            return self.store_file(fname, basename=basename)
    
    def store_pkl(self, x: Any, basename: Union[str, None]=None) -> str:
        if self._channel is not None:
            return store_pkl(x, basename=basename)
        if basename is None:
            basename = 'file.pkl'
        with TemporaryDirectory() as tmpdir:
            fname = tmpdir + '/file.pkl'
            _safe_pickle(fname, x)
            return self.store_file(fname, basename=basename)

def _get_ephemeral_kachery_storage_dir():
    from pathlib import Path
    homedir = str(Path.home())
    ksd = os.getenv('KACHERY_STORAGE_DIR', f'{homedir}/kachery-storage')
    if not os.path.exists(ksd):
        os.makedirs(ksd)
    return ksd

def _http_get_file(url, fname):
    from urllib import request
    request.urlretrieve(url, fname)

def _concatenate_file_chunks(chunk_fnames: List[str], concat_fname: str):
    with open(concat_fname, 'wb') as outf:
        for fname in chunk_fnames:
            with open(fname, 'rb') as f:
                outf.write(f.read())