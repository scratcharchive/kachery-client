from ._daemon_connection import _buffered_probe_daemon

_global = {
    'ephemeral_enabled': False
}

def enable_ephemeral(enabled: bool=True):
    _global['ephemeral_enabled'] = enabled

def _ephemeral_enabled():
    return _global['ephemeral_enabled']

def _use_ephemeral():
    return _ephemeral_enabled() and (_buffered_probe_daemon() is None)