#!/usr/bin/env python3

from typing import Optional, Any
from ytmusicapi import YTMusic, OAuthCredentials
from ytmusicfs.config import ConfigManager
import logging
import os


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
        browser: Optional[str] = None,
    ):
        """
        Initialize the YTMusic OAuth adapter.

        Args:
            auth_file: Path to the auth JSON file
            client_id: OAuth client ID from Google Cloud Console
            client_secret: OAuth client secret from Google Cloud Console
            logger: Optional logger instance
            browser: Browser to use for cookies (e.g., 'chrome', 'firefox', 'brave')
        """
        self.logger = logger or logging.getLogger(__name__)
        self.browser = browser
        self.ytmusic = None

        # Validate inputs
        if not os.path.exists(auth_file):
            raise FileNotFoundError(f"Auth file not found: {auth_file}")

        # Initialize config manager
        self.config = ConfigManager(auth_file=auth_file, logger=self.logger)

        # Use client_id/secret if provided, otherwise use ones from config
        self.client_id = client_id
        self.client_secret = client_secret

        # If not provided, try to load from config
        if not self.client_id or not self.client_secret:
            config_id, config_secret = self.config.get_credentials()
            self.client_id = self.client_id or config_id
            self.client_secret = self.client_secret or config_secret

        if not self.client_id or not self.client_secret:
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
                self.logger.debug("Created OAuth credentials object")
            else:
                self.logger.warning("Missing client_id or client_secret for OAuth")
                raise ValueError(
                    "Client ID and Client Secret are required for OAuth authentication"
                )

            # Initialize YTMusic with OAuth
            self.ytmusic = YTMusic(
                auth=str(self.config.auth_file), oauth_credentials=oauth_credentials
            )

            # Test connection with a lightweight call
            self.ytmusic.get_library_playlists(limit=1)
            self.logger.info(
                "Successfully authenticated with YouTube Music using OAuth"
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize YTMusic with OAuth: {e}")
            raise

    def refresh_token(self) -> bool:
        """
        Explicitly refresh the OAuth token.

        Returns:
            True if token was successfully refreshed, False otherwise
        """
        try:
            # Re-initialize the YTMusic instance which will refresh the token
            self._initialize_ytmusic()
            return True
        except Exception as e:
            self.logger.error(f"Failed to refresh OAuth token: {e}")
            return False

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
            attr = getattr(self.ytmusic, name)

            # If it's a method, wrap it to handle token refresh
            if callable(attr):

                def wrapped_method(*args, **kwargs):
                    try:
                        return attr(*args, **kwargs)
                    except Exception as e:
                        # If there's an error, try refreshing the token and retrying once
                        if (
                            "unauthorized" in str(e).lower()
                            or "authentication" in str(e).lower()
                        ):
                            self.logger.info(
                                "Authentication error, attempting to refresh token"
                            )
                            if self.refresh_token():
                                # Get the method from the new instance and try again
                                return getattr(self.ytmusic, name)(*args, **kwargs)
                        # If refresh didn't help or it wasn't an auth error, raise the original exception
                        raise

                return wrapped_method
            return attr

        raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")
