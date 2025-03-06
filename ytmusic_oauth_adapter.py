#!/usr/bin/env python3

import os
import sys
import json
import logging
from typing import Optional, Dict, Any

from ytmusicapi import YTMusic, OAuthCredentials


class YTMusicOAuthAdapter:
    """
    Adapter class for YTMusic that properly handles OAuth authentication
    according to the ytmusicapi documentation.
    """

    def __init__(
        self,
        auth_file: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the YTMusic OAuth adapter.

        Args:
            auth_file: Path to the OAuth token file created by setup_oauth()
            client_id: OAuth client ID from Google Cloud Console
            client_secret: OAuth client secret from Google Cloud Console
            logger: Optional logger instance
        """
        self.auth_file = auth_file
        self.client_id = client_id
        self.client_secret = client_secret
        self.logger = logger or logging.getLogger(__name__)
        self.ytmusic = None

        # Validate inputs
        if not os.path.exists(auth_file):
            raise FileNotFoundError(f"Auth file not found: {auth_file}")

        if not client_id or not client_secret:
            self.logger.warning(
                "Client ID and secret not provided. Token refresh may not work."
            )

        # Initialize YTMusic with OAuth
        self._initialize_ytmusic()

    def _initialize_ytmusic(self) -> None:
        """Initialize the YTMusic instance with OAuth credentials."""
        try:
            # Create OAuth credentials object if client ID and secret are provided
            oauth_credentials = None
            if self.client_id and self.client_secret:
                oauth_credentials = OAuthCredentials(
                    client_id=self.client_id, client_secret=self.client_secret
                )

            # Initialize YTMusic with OAuth
            self.ytmusic = YTMusic(
                auth=self.auth_file, oauth_credentials=oauth_credentials
            )

            # Test connection
            self.ytmusic.get_library_playlists(limit=1)
            self.logger.info(
                "Successfully authenticated with YouTube Music using OAuth"
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize YTMusic with OAuth: {e}")
            raise

    def __getattr__(self, name: str) -> Any:
        """
        Delegate method calls to the underlying YTMusic instance.

        Args:
            name: Name of the attribute or method to access

        Returns:
            The attribute or method from the YTMusic instance
        """
        if self.ytmusic is None:
            raise RuntimeError("YTMusic instance not initialized")

        if hasattr(self.ytmusic, name):
            return getattr(self.ytmusic, name)

        raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")


# Example usage
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test YTMusic OAuth Adapter")
    parser.add_argument("--auth-file", required=True, help="Path to OAuth token file")
    parser.add_argument("--client-id", help="OAuth client ID")
    parser.add_argument("--client-secret", help="OAuth client secret")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        # Initialize adapter
        adapter = YTMusicOAuthAdapter(
            auth_file=args.auth_file,
            client_id=args.client_id,
            client_secret=args.client_secret,
        )

        # Test getting playlists
        playlists = adapter.get_library_playlists(limit=5)
        print(f"Success! Found {len(playlists)} playlists")
        for i, playlist in enumerate(playlists[:5], 1):
            print(f"  {i}. {playlist['title']}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
