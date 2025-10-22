#!/usr/bin/env python3

from typing import Optional, Any
from ytmusicapi import YTMusic, OAuthCredentials
from ytmusicfs.config import ConfigManager
import json
import logging
import os
import time


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
        credentials_file: Optional[str] = None,
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
        self.config = ConfigManager(
            auth_file=auth_file,
            credentials_file=credentials_file,
            logger=self.logger,
        )

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
        attempt = 0
        while True:
            attempt += 1
            try:
                oauth_credentials = self._create_oauth_credentials()

                # Initialize YTMusic with OAuth
                self.ytmusic = YTMusic(
                    auth=str(self.config.auth_file),
                    oauth_credentials=oauth_credentials,
                )

                # Test connection with a lightweight call
                self.ytmusic.get_library_playlists(limit=1)
                self.logger.info(
                    "Successfully authenticated with YouTube Music using OAuth"
                )
                return

            except Exception as e:
                self.logger.error(f"Failed to initialize YTMusic with OAuth: {e}")

                should_retry = attempt == 1 and self._refresh_oauth_token()
                if not should_retry:
                    raise

                self.logger.info(
                    "Retrying YTMusic initialization after refreshing OAuth token"
                )

    def _create_oauth_credentials(self) -> OAuthCredentials:
        """Create an OAuthCredentials instance using known client credentials."""

        if not self.client_id or not self.client_secret:
            self.logger.warning("Missing client_id or client_secret for OAuth")
            raise ValueError(
                "Client ID and Client Secret are required for OAuth authentication"
            )

        self.logger.debug("Created OAuth credentials object")
        return OAuthCredentials(client_id=self.client_id, client_secret=self.client_secret)

    def _refresh_oauth_token(self) -> bool:
        """Attempt to refresh the stored OAuth access token."""

        if not self.client_id or not self.client_secret:
            self.logger.error(
                "Cannot refresh OAuth token without client credentials available"
            )
            return False

        try:
            with open(self.config.auth_file, "r", encoding="utf-8") as auth_fp:
                oauth_data = json.load(auth_fp)
        except FileNotFoundError:
            self.logger.error(f"OAuth auth file not found at {self.config.auth_file}")
            return False
        except json.JSONDecodeError as exc:
            self.logger.error(
                f"OAuth auth file at {self.config.auth_file} is invalid JSON: {exc}"
            )
            return False

        refresh_token = oauth_data.get("refresh_token")
        if not refresh_token:
            self.logger.error(
                "OAuth auth file does not contain a refresh_token; cannot refresh"
            )
            return False

        try:
            oauth_credentials = OAuthCredentials(
                client_id=self.client_id, client_secret=self.client_secret
            )
            refreshed = oauth_credentials.refresh_token(refresh_token)
        except Exception as exc:
            self.logger.error(f"Failed to refresh OAuth token via API: {exc}")
            return False

        access_token = refreshed.get("access_token")
        if not access_token:
            self.logger.error("Refresh response did not include a new access_token")
            return False

        oauth_data["access_token"] = access_token

        if "refresh_token" in refreshed and refreshed["refresh_token"]:
            oauth_data["refresh_token"] = refreshed["refresh_token"]

        expires_in = refreshed.get("expires_in")
        if expires_in is not None:
            try:
                expires_in_value = int(expires_in)
            except (TypeError, ValueError):
                expires_in_value = None
            else:
                oauth_data["expires_in"] = expires_in_value
                oauth_data["expires_at"] = int(time.time()) + expires_in_value

        try:
            with open(self.config.auth_file, "w", encoding="utf-8") as auth_fp:
                json.dump(oauth_data, auth_fp, indent=2)
        except Exception as exc:
            self.logger.error(f"Failed to update OAuth auth file: {exc}")
            return False

        self.logger.info("Refreshed OAuth access token and updated auth file")
        return True

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
