"""Shared constants for the ytmusicfs package."""

from typing import Dict, Tuple


# Mapping of top-level directory names to their corresponding playlist types.
# The insertion order defines the presentation order for directory listings.
TOP_LEVEL_CATEGORIES: Dict[str, str] = {
    "playlists": "playlist",
    "liked_songs": "liked_songs",
    "albums": "album",
}

# Convenience tuples derived from ``TOP_LEVEL_CATEGORIES`` for reuse across
# modules without recalculating them repeatedly.
TOP_LEVEL_PATHS: Tuple[str, ...] = tuple(f"/{name}" for name in TOP_LEVEL_CATEGORIES)
ROOT_DIRECTORY_ENTRIES: Tuple[str, ...] = tuple(TOP_LEVEL_CATEGORIES.keys())

# Playlist types that should be treated as directories containing other entries
# rather than direct audio files. This is used to determine how getattr should
# respond for category items.
DIRECTORY_PLAYLIST_TYPES = {"playlist", "album"}

