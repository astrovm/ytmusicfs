"""
YTMusicFS - YouTube Music FUSE Filesystem

Access your YouTube Music library through a standard filesystem interface.
"""

__version__ = "0.1.0"

from ytmusicfs.cache import CacheManager
from ytmusicfs.client import YouTubeMusicClient
from ytmusicfs.filesystem import YouTubeMusicFS, mount_ytmusicfs, PathRouter
from ytmusicfs.processor import TrackProcessor
