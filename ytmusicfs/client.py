#!/usr/bin/env python3

from typing import Dict, Optional, List
from ytmusicfs.utils.oauth_adapter import YTMusicOAuthAdapter
import logging


class YouTubeMusicClient:
    """Client for interacting with YouTube Music API."""

    def __init__(
        self,
        auth_file: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        browser: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize the YouTube Music API client.

        Args:
            auth_file: Path to authentication file (OAuth token)
            client_id: OAuth client ID (required for OAuth authentication)
            client_secret: OAuth client secret (required for OAuth authentication)
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
            logger: Logger instance to use
        """
        # Set up logger
        self.logger = logger or logging.getLogger("YouTubeMusicClient")

        try:
            # Use YTMusicOAuthAdapter for OAuth support
            self.ytmusic = YTMusicOAuthAdapter(
                auth_file=auth_file,
                client_id=client_id,
                client_secret=client_secret,
                logger=self.logger,
                browser=browser,
            )
            self.logger.info(f"Authentication successful with OAuth method!")
        except Exception as e:
            self.logger.error(f"Error during authentication: {e}")
            self.logger.error(
                "Try regenerating your authentication file with ytmusicfs-oauth"
            )
            raise

    def get_library_playlists(self, limit: int = 100) -> List[Dict]:
        """Get playlists from the user's library.

        Args:
            limit: Maximum number of playlists to fetch

        Returns:
            List of playlist dictionaries
        """
        return self.ytmusic.get_library_playlists(limit=limit)

    def get_liked_songs(self, limit: int = 10000) -> Dict:
        """Get liked songs from the user's library.

        Args:
            limit: Maximum number of songs to fetch

        Returns:
            Dictionary containing liked songs data
        """
        return self.ytmusic.get_liked_songs(limit=limit)

    def get_playlist(self, playlist_id: str, limit: int = 10000) -> Dict:
        """Get a playlist by ID.

        Args:
            playlist_id: The playlist ID
            limit: Maximum number of tracks to fetch

        Returns:
            Dictionary containing playlist data
        """
        return self.ytmusic.get_playlist(playlist_id, limit=limit)

    def get_library_artists(self, limit: int = 10000) -> List[Dict]:
        """Get artists from the user's library.

        Args:
            limit: Maximum number of artists to fetch

        Returns:
            List of artist dictionaries
        """
        return self.ytmusic.get_library_artists(limit=limit)

    def get_artist(self, artist_id: str) -> Dict:
        """Get an artist by ID.

        Args:
            artist_id: The artist ID

        Returns:
            Dictionary containing artist data
        """
        return self.ytmusic.get_artist(artist_id)

    def get_library_albums(self, limit: int = 10000) -> List[Dict]:
        """Get albums from the user's library.

        Args:
            limit: Maximum number of albums to fetch

        Returns:
            List of album dictionaries
        """
        return self.ytmusic.get_library_albums(limit=limit)

    def get_album(self, album_id: str) -> Dict:
        """Get an album by ID.

        Args:
            album_id: The album ID

        Returns:
            Dictionary containing album data
        """
        return self.ytmusic.get_album(album_id)

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
            query: Search query string (e.g., 'Oasis Wonderwall')
            filter_type: Filter for item types. Allowed values: 'songs', 'videos', 'albums',
                        'artists', 'playlists', 'community_playlists', 'featured_playlists', 'uploads'.
                        Default: None (includes all types)
            scope: Search scope. Allowed values: 'library', 'uploads'.
                  Default: None (searches the entire YouTube Music catalog)
            limit: Maximum number of search results to return
            ignore_spelling: Whether to ignore YT Music spelling suggestions

        Returns:
            List of search result dictionaries
        """
        self.logger.debug(
            f"Searching for '{query}' with filter '{filter_type}' and scope '{scope}'"
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
            self.logger.error(f"Error during search: {e}")
            return []
