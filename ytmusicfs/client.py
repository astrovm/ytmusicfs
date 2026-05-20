#!/usr/bin/env python3

import logging
import time
from json import JSONDecodeError
from typing import Dict, List, Optional

from ytmusicfs.auth_adapter import YTMusicAuthAdapter

_API_ATTEMPTS = 3
_API_RETRY_DELAY_SECONDS = 1.0


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
        return self._call_with_json_retry(
            "fetch library playlists",
            self.ytmusic.get_library_playlists,
            limit=limit,
        )

    def get_liked_songs(self, limit: int = 10000) -> Dict:
        """Fetch liked songs from the user's library.

        Args:
            limit: Maximum number of songs to retrieve (default: 10000).

        Returns:
            Dictionary containing liked songs data.
        """
        return self._call_with_json_retry(
            "fetch liked songs",
            self.ytmusic.get_liked_songs,
            limit=limit,
        )

    def get_playlist(self, playlist_id: str, limit: int = 10000) -> Dict:
        """Fetch a playlist by its ID.

        Args:
            playlist_id: The playlist ID.
            limit: Maximum number of tracks to retrieve (default: 10000).

        Returns:
            Dictionary containing playlist data.
        """
        return self._call_with_json_retry(
            f"fetch playlist {playlist_id}",
            self.ytmusic.get_playlist,
            playlist_id,
            limit=limit,
        )

    def get_library_artists(self, limit: int = 10000) -> List[Dict]:
        """Fetch artists from the user's library.

        Args:
            limit: Maximum number of artists to retrieve (default: 10000).

        Returns:
            List of artist dictionaries.
        """
        return self._call_with_json_retry(
            "fetch library artists",
            self.ytmusic.get_library_artists,
            limit=limit,
        )

    def get_artist(self, artist_id: str) -> Dict:
        """Fetch an artist by their ID.

        Args:
            artist_id: The artist ID.

        Returns:
            Dictionary containing artist data.
        """
        return self._call_with_json_retry(
            f"fetch artist {artist_id}",
            self.ytmusic.get_artist,
            artist_id,
        )

    def get_library_albums(self, limit: int = 10000) -> List[Dict]:
        """Fetch albums from the user's library.

        Args:
            limit: Maximum number of albums to retrieve (default: 10000).

        Returns:
            List of album dictionaries.
        """
        return self._call_with_json_retry(
            "fetch library albums",
            self.ytmusic.get_library_albums,
            limit=limit,
        )

    def get_album(self, album_id: str) -> Dict:
        """Fetch an album by its ID.

        Args:
            album_id: The album ID.

        Returns:
            Dictionary containing album data.
        """
        return self._call_with_json_retry(
            f"fetch album {album_id}",
            self.ytmusic.get_album,
            album_id,
        )

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

    def _call_with_json_retry(self, operation: str, func, *args, **kwargs):
        for attempt in range(1, _API_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except JSONDecodeError:
                if attempt == _API_ATTEMPTS:
                    self.logger.error(
                        "Failed to %s: empty or non-JSON response after %s attempts",
                        operation,
                        _API_ATTEMPTS,
                    )
                    raise

                self.logger.warning(
                    "YouTube Music returned an empty or non-JSON response while "
                    "trying to %s; retrying (%s/%s)",
                    operation,
                    attempt,
                    _API_ATTEMPTS,
                )
                time.sleep(_API_RETRY_DELAY_SECONDS)
            except Exception as exc:
                self.logger.error("Failed to %s: %s", operation, exc)
                raise

        raise RuntimeError(f"Failed to {operation}")
