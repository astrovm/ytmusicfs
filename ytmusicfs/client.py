#!/usr/bin/env python3

from typing import Dict, Optional, List
from ytmusicfs.auth_adapter import YTMusicAuthAdapter
import logging


class YouTubeMusicClient:
    """Client for interacting with the YouTube Music API.

    This class is responsible solely for making API calls to YouTube Music.
    Authentication and configuration are handled externally.
    """

    def __init__(
        self,
        auth_adapter: YTMusicAuthAdapter,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize the YouTube Music API client.

        Args:
            auth_adapter: Configured YTMusicAuthAdapter instance for authentication.
            logger: Optional logger instance; defaults to a new logger if None.
        """
        self.logger = logger or logging.getLogger("YouTubeMusicClient")
        self.ytmusic = auth_adapter
        self.logger.info("YouTubeMusicClient initialized with browser auth")

    def get_library_playlists(self, limit: int = 100) -> List[Dict]:
        """Fetch playlists from the user's library.

        Args:
            limit: Maximum number of playlists to retrieve (default: 100).

        Returns:
            List of playlist dictionaries.
        """
        try:
            return self.ytmusic.get_library_playlists(limit=limit)
        except Exception as e:
            self.logger.error(f"Failed to fetch library playlists: {e}")
            raise

    def get_liked_songs(self, limit: int = 10000) -> Dict:
        """Fetch liked songs from the user's library.

        Args:
            limit: Maximum number of songs to retrieve (default: 10000).

        Returns:
            Dictionary containing liked songs data.
        """
        try:
            return self.ytmusic.get_liked_songs(limit=limit)
        except Exception as e:
            self.logger.error(f"Failed to fetch liked songs: {e}")
            raise

    def get_playlist(self, playlist_id: str, limit: int = 10000) -> Dict:
        """Fetch a playlist by its ID.

        Args:
            playlist_id: The playlist ID.
            limit: Maximum number of tracks to retrieve (default: 10000).

        Returns:
            Dictionary containing playlist data.
        """
        try:
            return self.ytmusic.get_playlist(playlist_id, limit=limit)
        except Exception as e:
            self.logger.error(f"Failed to fetch playlist {playlist_id}: {e}")
            raise

    def get_library_artists(self, limit: int = 10000) -> List[Dict]:
        """Fetch artists from the user's library.

        Args:
            limit: Maximum number of artists to retrieve (default: 10000).

        Returns:
            List of artist dictionaries.
        """
        try:
            return self.ytmusic.get_library_artists(limit=limit)
        except Exception as e:
            self.logger.error(f"Failed to fetch library artists: {e}")
            raise

    def get_artist(self, artist_id: str) -> Dict:
        """Fetch an artist by their ID.

        Args:
            artist_id: The artist ID.

        Returns:
            Dictionary containing artist data.
        """
        try:
            return self.ytmusic.get_artist(artist_id)
        except Exception as e:
            self.logger.error(f"Failed to fetch artist {artist_id}: {e}")
            raise

    def get_library_albums(self, limit: int = 10000) -> List[Dict]:
        """Fetch albums from the user's library.

        Args:
            limit: Maximum number of albums to retrieve (default: 10000).

        Returns:
            List of album dictionaries.
        """
        try:
            return self.ytmusic.get_library_albums(limit=limit)
        except Exception as e:
            self.logger.error(f"Failed to fetch library albums: {e}")
            raise

    def get_album(self, album_id: str) -> Dict:
        """Fetch an album by its ID.

        Args:
            album_id: The album ID.

        Returns:
            Dictionary containing album data.
        """
        try:
            return self.ytmusic.get_album(album_id)
        except Exception as e:
            self.logger.error(f"Failed to fetch album {album_id}: {e}")
            raise

    def search(
        self,
        query: str,
        filter_type: Optional[str] = None,
        scope: Optional[str] = None,
        limit: int = 100,
        ignore_spelling: bool = False,
    ) -> List[Dict]:
        """Search YouTube Music.

        Args:
            query: Search query string (e.g., 'Oasis Wonderwall').
            filter_type: Filter for item types ('songs', 'videos', 'albums', etc.).
            scope: Search scope ('library', 'uploads', or None for full catalog).
            limit: Maximum number of results (default: 100).
            ignore_spelling: Ignore spelling suggestions if True.

        Returns:
            List of search result dictionaries.
        """
        self.logger.debug(
            f"Searching '{query}' with filter '{filter_type}' and scope '{scope}'"
        )
        try:
            results = self.ytmusic.search(
                query=query,
                filter=filter_type,
                scope=scope,
                limit=limit,
                ignore_spelling=ignore_spelling,
            )
            self.logger.debug(f"Search returned {len(results)} results")
            return results
        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            return []
