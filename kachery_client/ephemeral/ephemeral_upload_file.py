import time
from .ephemeral_load_file import _get_private_key_hex, _get_public_key_hex, _get_owner, _sign_message
from .._misc import _http_post_json
from ..task_backend._update_task_status import _http_put_bytes


_global = {
    'channel_configs': {},
    'bitwooder_certs_by_channel': {}
}

def _get_channel_config(channel: str):
    if channel in _global['channel_configs']:
        return _global['channel_configs'][channel]
    private_key_hex = _get_private_key_hex()
    public_key_hex = _get_public_key_hex()
    body = {
        'type': 'getChannelConfig',
        'channelName': channel
    }
    signature = _sign_message(body, public_key_hex, private_key_hex)
    req = {
        'body': body,
        'nodeId': public_key_hex,
        'signature': signature
    }
    url = f'https://kacheryhub.org/api/kacheryNode'
    resp = _http_post_json(url, req)
    if not resp['found']:
        raise Exception('Channel config not found')
    channel_config = resp['channelConfig']
    _global['channel_configs'][channel] = channel_config
    return channel_config

def _get_bitwooder_cert_for_channel(channel: str):
    a = _global['bitwooder_certs_by_channel']
    if channel in a:
        x = a[channel]
        if not _cert_expired(x['cert']):
            return x['cert'], x['key']
    private_key_hex = _get_private_key_hex()
    public_key_hex = _get_public_key_hex()
    owner_id = _get_owner()
    body = {
        'type': 'getBitwooderCertForChannel',
        'nodeId': public_key_hex,
        'ownerId': owner_id,
        'channelName': channel
    }
    signature = _sign_message(body, public_key_hex, private_key_hex)
    req = {
        'body': body,
        'nodeId': public_key_hex,
        'signature': signature
    }
    url = f'https://kacheryhub.org/api/kacheryNode'
    resp = _http_post_json(url, req)
    cert = resp['cert']
    key = resp['key']
    a[channel] = {'cert': cert, 'key': key}
    return cert, key

def _cert_expired(cert):
    expires = cert['payload']['expires']
    expires_in_sec = (expires - time.time() * 1000) / 1000
    return (expires_in_sec < 30)

def ephemeral_upload_file(*, file_content: bytes, sha1: str, channel: str) -> None:
    channel_config = _get_channel_config(channel)
    bucket_base_url = channel_config['bucketBaseUrl']
    cert, key = _get_bitwooder_cert_for_channel(channel)
    bitwooder_resource_id = channel_config['bitwooderResourceId']
    signer_id = cert['payload']['delegatedSignerId']
    payload = {
        'type': 'getUploadUrls',
        'expires': time.time() * 1000 + 60 * 1000,
        'resourceId': bitwooder_resource_id,
        'filePaths': [f'{channel}/sha1/{sha1[0]}{sha1[1]}/{sha1[2]}{sha1[3]}/{sha1[4]}{sha1[5]}/{sha1}'],
        'sizes': [len(file_content)]
    }
    signature = _sign_message(payload, signer_id, key)
    req = {
        'type': 'getUploadUrls',
        'payload': payload,
        'auth': {
            'signerId': signer_id,
            'signature': signature,
            'delegationCertificate': cert
        }
    }
    url = f'https://bitwooder.net/api/resource'
    resp = _http_post_json(url, req)
    upload_urls = resp['uploadUrls']
    upload_url = upload_urls[0]
    _http_put_bytes(upload_url, file_content)