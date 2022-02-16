import os
import base64
from typing import Union, Any
import shutil
import numpy as np
from ..direct_client.DirectClient import _get_ephemeral_kachery_storage_dir
from .._misc import _http_post_json, _parse_kachery_uri, _get_kachery_hub_uri
from .._temporarydirectory import TemporaryDirectory
from ..direct_client.DirectClient import _concatenate_file_chunks, _http_get_file
from .._local_kachery_storage import _compute_file_hash


_global = {
    'private_key_hex': None,
    'public_key_hex': None,
    'owner': None,
    'node_config': None
}

def _get_private_key_hex():
    if _global['private_key_hex'] is not None:
        return _global['private_key_hex']
    kachery_storage_dir = _get_ephemeral_kachery_storage_dir()
    private_key_fname = f'{kachery_storage_dir}/private.pem'
    if not os.path.exists(private_key_fname):
        return None
    with open(private_key_fname, 'r') as f:
        private_key = f.read()
    private_key_hex = _private_key_to_hex(private_key)
    _global['private_key_hex'] = private_key_hex
    return private_key_hex

def _get_public_key_hex():
    if _global['public_key_hex'] is not None:
        return _global['public_key_hex']
    kachery_storage_dir = _get_ephemeral_kachery_storage_dir()
    public_key_fname = f'{kachery_storage_dir}/public.pem'
    if not os.path.exists(public_key_fname):
        return None
    with open(public_key_fname, 'r') as f:
        public_key = f.read()
    public_key_hex = _public_key_to_hex(public_key)
    _global['public_key_hex'] = public_key_hex
    return public_key_hex

def _get_owner():
    if _global['owner'] is not None:
        return _global['owner']
    kachery_storage_dir = _get_ephemeral_kachery_storage_dir()
    owner_fname = f'{kachery_storage_dir}/owner'
    if not os.path.exists(owner_fname):
        return None
    with open(owner_fname, 'r') as f:
        owner = f.read()
    _global['owner'] = owner
    return owner

def _get_node_config():
    if _global['node_config'] is not None:
        return _global['node_config']
    private_key_hex = _get_private_key_hex()
    public_key_hex = _get_public_key_hex()
    owner_id = _get_owner()
    assert public_key_hex is not None, 'Unable to get public key and not connected to daemon.'
    assert private_key_hex is not None, 'Unable to get private key and not connected to daemon.'
    assert owner_id is not None, 'Unable to get owner ID and not connected to daemon.'
    body = {
        'type': 'getNodeConfig',
        'nodeId': public_key_hex,
        'ownerId': owner_id
    }
    signature = _sign_message(body, public_key_hex, private_key_hex)
    req = {
        'body': body,
        'nodeId': public_key_hex,
        'signature': signature
    }
    endpoint = _get_kachery_hub_uri(with_protocol=True)
    url = f'{endpoint}/api/kacheryNode'
    resp = _http_post_json(url, req)
    if not resp['found']:
        raise Exception('Node not found on kacheryhub')
    node_config = resp['nodeConfig']
    _global['node_config'] = node_config
    return node_config

def ephemeral_load_file(uri: str, *, local_only: bool=False) -> Union[str, None]:
    protocol, algorithm, sha1, additional_path, query = _parse_kachery_uri(uri)
    assert algorithm == 'sha1'
    kachery_storage_dir = _get_ephemeral_kachery_storage_dir()
    kachery_storage_parent_dir = f'{kachery_storage_dir}/sha1/{sha1[0]}{sha1[1]}/{sha1[2]}{sha1[3]}/{sha1[4]}{sha1[5]}'
    kachery_storage_file_name = f'{kachery_storage_parent_dir}/{sha1}'
    if os.path.exists(kachery_storage_file_name):
        # we have the file locally... return that
        return kachery_storage_file_name
    if 'manifest' in query:
        # The uri has a manifest. But let's first check whether the file is stored on the bucket in its entirety
        aa = _load_direct_from_channel_buckets(sha1) # no manifest included in the uri
        if aa is not None:
            return aa
        # The uri has a manifest, so we are going to load it in chunks
        manifest = _ephemeral_load_json(f'sha1://{query["manifest"][0]}')
        # load the file chunks individually
        chunk_files = []
        for chunk in manifest['chunks']:
            chunk_sha1 = chunk['sha1']
            chunk_start = chunk['start']
            chunk_end = chunk['end']
            chunk_fname = ephemeral_load_file(f'sha1://{chunk_sha1}?chunkOf={sha1}~{chunk_start}~{chunk_end}')
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
            if not os.path.exists(kachery_storage_parent_dir):
                os.makedirs(kachery_storage_parent_dir)
            shutil.copyfile(tmp_fname, kachery_storage_file_name)
            return kachery_storage_file_name
    bb = _load_direct_from_channel_buckets(sha1)
    if bb is not None:
        return bb
    return None

def _ephemeral_load_json(uri: str) -> Union[None, dict, list, int, float]:
    import simplejson
    local_path = ephemeral_load_file(uri)
    if local_path is None:
        return None
    with open(local_path, 'r') as f:
        return simplejson.load(f)

def _load_direct_from_channel_buckets(sha1: str):
    node_config = _get_node_config()
    kachery_storage_dir = _get_ephemeral_kachery_storage_dir()
    kachery_storage_parent_dir = f'{kachery_storage_dir}/sha1/{sha1[0]}{sha1[1]}/{sha1[2]}{sha1[3]}/{sha1[4]}{sha1[5]}'
    kachery_storage_file_name = f'{kachery_storage_parent_dir}/{sha1}'
    with TemporaryDirectory() as tmpdir:
        tmp_fname = f'{tmpdir}/file.dat'
        for ch in node_config['channelMemberships']:
            channel_name = ch['channelName']
            channel_bucket_base_url = ch['channelBucketBaseUrl']
            file_url = f'{channel_bucket_base_url}/{channel_name}/sha1/{sha1[0]}{sha1[1]}/{sha1[2]}{sha1[3]}/{sha1[4]}{sha1[5]}/{sha1}'
            try:
                _http_get_file(file_url, tmp_fname)
                downloaded = True
            except:
                downloaded = False
            if downloaded:
                if not os.path.exists(kachery_storage_parent_dir):
                    os.makedirs(kachery_storage_parent_dir)
                shutil.copyfile(tmp_fname, kachery_storage_file_name)
                return kachery_storage_file_name
    return None

ed25519PubKeyPrefix = "302a300506032b6570032100"
ed25519PrivateKeyPrefix = "302e020100300506032b657004220420"

def _public_key_to_hex(key: str) -> str:
    x = key.split('\n')
    if x[0] != '-----BEGIN PUBLIC KEY-----':
        raise Exception('Problem in public key format (1).')
    if x[2] != '-----END PUBLIC KEY-----':
        raise Exception('Problem in public key format (2).')
    ret = base64.b64decode(x[1]).hex()
    if not ret.startswith(ed25519PubKeyPrefix):
        raise Exception('Problem in public key format (3).')
    return ret[len(ed25519PubKeyPrefix):]

def _private_key_to_hex(key: str) -> str:
    x = key.split('\n')
    if x[0] != '-----BEGIN PRIVATE KEY-----':
        raise Exception('Problem in private key format (1).')
    if x[2] != '-----END PRIVATE KEY-----':
        raise Exception('Problem in private key format (2).')
    ret = base64.b64decode(x[1]).hex()
    if not ret.startswith(ed25519PrivateKeyPrefix):
        raise Exception('Problem in private key format (3).')
    return ret[len(ed25519PrivateKeyPrefix):]

def _public_key_from_hex(key_hex: str):
    a = base64.b64encode(bytes.fromhex(ed25519PubKeyPrefix + key_hex)).decode()
    return f'-----BEGIN PUBLIC KEY-----\n{a}\n-----END PUBLIC KEY-----'

def _private_key_from_hex(key_hex: str):
    a = base64.b64encode(bytes.fromhex(ed25519PrivateKeyPrefix + key_hex)).decode()
    return f'-----BEGIN PRIVATE KEY-----\n{a}\n-----END PRIVATE KEY-----'

def _deterministic_json_dumps(x: dict):
    import simplejson
    return simplejson.dumps(x, separators=(',', ':'), indent=None, allow_nan=False, sort_keys=True)

def _sha1_of_string(txt: str) -> str:
    import hashlib
    hh = hashlib.sha1(txt.encode('utf-8'))
    ret = hh.hexdigest()
    return ret

def _sign_message(msg: dict, public_key_hex: str, private_key_hex: str) -> str:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
    msg_json = _deterministic_json_dumps(msg)
    msg_hash = _sha1_of_string(msg_json)
    msg_bytes = bytes.fromhex(msg_hash)
    privk = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(private_key_hex))
    pubk = Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key_hex))
    signature = privk.sign(msg_bytes).hex()
    pubk.verify(bytes.fromhex(signature), msg_bytes)
    return signature

def _verify_signature(msg: dict, public_key_hex: str, signature: str):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    msg_json = _deterministic_json_dumps(msg)
    msg_hash = _sha1_of_string(msg_json)
    msg_bytes = bytes.fromhex(msg_hash)
    pubk = Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key_hex))
    try:
        pubk.verify(bytes.fromhex(signature), msg_bytes)
    except:
        return False
    return True

def _generate_keypair():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    privk = Ed25519PrivateKey.generate()
    pubk = privk.public_key()
    private_key_hex = privk.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption()
    ).hex()
    public_key_hex = pubk.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    ).hex()
    test_msg = {'a': 1}
    test_signature = _sign_message(test_msg, public_key_hex, private_key_hex)
    assert _verify_signature(test_msg, public_key_hex, test_signature)
    return public_key_hex, private_key_hex