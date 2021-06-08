from .version import __version__
from .main import load_bytes, load_feed, load_file, load_json, load_npy, load_pkl, load_subfeed, load_text
from .main import create_feed
from .main import store_file, store_json, store_npy, store_pkl, store_text, link_file
from .main import get, set, get_feed_id, get_string

from .cli import cli