import os
import json
import kachery_client as kc
from ._misc import _get_kachery_hub_uri
from .ephemeral.ephemeral_load_file import _get_public_key_hex, _get_private_key_hex, _get_owner, _get_ephemeral_kachery_storage_dir, _public_key_from_hex, _private_key_from_hex, _public_key_to_hex, _private_key_to_hex

def setup_colab_ephemeral(config_file_path: str):
  kachery_storage_dir = _get_ephemeral_kachery_storage_dir()
  public_pem_fname = f'{kachery_storage_dir}/public.pem'
  if os.path.exists(public_pem_fname):
    with open(public_pem_fname, 'r') as f:
      public_key_hex2 = f.read()
  else:
    public_key_hex2 = None
  if os.path.exists(config_file_path):
    print(f'Using existing config file: {config_file_path}')
    with open(config_file_path, 'r') as f:
      x = json.load(f)
    public_key_hex = x['publicKey']
    private_key_hex = x['privateKey']
    owner_id = x['ownerId']
    
    if not os.path.exists(kachery_storage_dir):
      print(f'Creating {kachery_storage_dir}')
      os.makedirs(kachery_storage_dir)
    
    if public_key_hex != public_key_hex2:
      print(f'Writing configuration to {kachery_storage_dir}')
      with open(public_pem_fname, 'w') as f:
        f.write(_public_key_from_hex(x['publicKey']))
      with open(f'{kachery_storage_dir}/private.pem', 'w') as f:
        f.write(_private_key_from_hex(x['privateKey']))
      with open(f'{kachery_storage_dir}/owner', 'w') as f:
        f.write(x['ownerId'])
    node_id = public_key_hex
  else:
    if public_key_hex2 is None:
      print('Configuring ephemeral node')
      kc.config_ephemeral_node()
    x = {}
    with open(f'{kachery_storage_dir}/public.pem', 'r') as f:
      x['publicKey'] = _public_key_to_hex(f.read())
    with open(f'{kachery_storage_dir}/private.pem', 'r') as f:
      x['privateKey'] = _private_key_to_hex(f.read())
    with open(f'{kachery_storage_dir}/owner', 'r') as f:
      x['ownerId'] = f.read()
    print(f'Writing configuration to {config_file_path}')
    try:
      with open(config_file_path, 'w') as f:
        json.dump(x, f, indent=4)
    except:
      raise Exception('Unable to create configuration file. Perhaps you need to mount your google drive.')
    node_id = x['publicKey']
    owner_id = x['ownerId']
      
  print(f'Enabling ephemeral mode')
  kc.enable_ephemeral()

  endpoint = _get_kachery_hub_uri(with_protocol=True)
  print(f'Node ID: {node_id}')
  print(f'Owner: {owner_id}')
  print(f'Register or configure this node at {endpoint}')