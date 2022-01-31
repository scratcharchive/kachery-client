import os, stat
from .._daemon_connection import _connected_to_daemon
from .ephemeral_load_file import _get_public_key_hex, _get_private_key_hex, _get_owner, _get_ephemeral_kachery_storage_dir, _generate_keypair, _public_key_from_hex, _private_key_from_hex


def config_ephemeral_node():
    if _connected_to_daemon():
        print('Cannot configure ephemeral node. You are connected to a kachery daemon.')
        return
    public_key_hex = _get_public_key_hex()
    if public_key_hex is not None:
        print(f'Node ID: {public_key_hex}')
        private_key_hex = _get_private_key_hex()
        owner_id = _get_owner()
        if private_key_hex is None:
            print('Found public key, but unable to find private key.')
            return
        if owner_id is None:
            print('Found public key, but unable to find owner.')
            return
        print(f'Ephemeral node is already configured at {_get_ephemeral_kachery_storage_dir()}')
        return
    kachery_storage_dir = _get_ephemeral_kachery_storage_dir()
    print(f'Configuring ephemeral node at {kachery_storage_dir}')
    owner_id = input('Enter the owner of this node (Google ID): ')
    public_key_hex, private_key_hex = _generate_keypair()
    if not os.path.exists(kachery_storage_dir):
        os.makedirs(kachery_storage_dir)
    with open(f'{kachery_storage_dir}/owner', 'w') as f:
        f.write(owner_id)
    with open(f'{kachery_storage_dir}/public.pem', 'w') as f:
        f.write(_public_key_from_hex(public_key_hex))
    private_pem_fname = f'{kachery_storage_dir}/private.pem'
    with open(private_pem_fname, 'w') as f:
        f.write(_private_key_from_hex(private_key_hex))
    os.chmod(private_pem_fname, stat.S_IRUSR|stat.S_IWUSR)
    public_key_hex = _get_public_key_hex()
    print(f'Node ID: {public_key_hex}')
    print(f'Ephemeral kachery node configured at {kachery_storage_dir}')
    