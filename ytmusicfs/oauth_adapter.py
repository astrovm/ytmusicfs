#!/usr/bin/env python3

import logging
import os
from typing import Any, Optional

from ytmusicapi import OAuthCredentials, YTMusic

from ytmusicfs.config import ConfigManager
from ytmusicfs.oauth_utils import apply_server_client_version


def _mask(value: Optional[str]) -> str:
    """Return a masked representation of sensitive credential values."""

    if not value:
        return "<missing>"

    if len(value) <= 8:
        return f"{value[0]}***{value[-1]}"

    return f"{value[:4]}…{value[-4:]}"


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

        self.logger.debug(
            "Initializing YTMusic client with auth file %s", self.config.auth_file
        )
        self.logger.debug(
            "Resolved OAuth credentials for adapter: client_id=%s, client_secret=%s",
            _mask(self.client_id),
            _mask(self.client_secret),
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

            apply_server_client_version(self.ytmusic, logger=self.logger)

            # Test connection with a lightweight call
            self.ytmusic.get_library_playlists(limit=1)
            self.logger.info(
                "Successfully authenticated with YouTube Music using OAuth"
            )

        except Exception as e:
            self._log_oauth_error(e)
            raise

    def _log_oauth_error(self, error: Exception) -> None:
        """Log detailed information about OAuth initialization failures."""
        details = [f"{error.__class__.__name__}: {error}"]

        cause = getattr(error, "__cause__", None)
        if cause is not None:
            details.append(f"Caused by {cause.__class__.__name__}: {cause}")

        response = getattr(error, "response", None)
        if response is not None:
            status = getattr(response, "status_code", "unknown")
            reason = getattr(response, "reason", "")
            details.append(f"HTTP response status: {status} {reason}".strip())

            # Attempt to extract a meaningful payload from the response
            payload = None
            try:
                payload = response.json()
            except Exception:  # noqa: BLE001 - best effort to decode
                text = getattr(response, "text", None)
                if text:
                    payload = text

            if payload:
                payload_str = str(payload)
                if len(payload_str) > 500:
                    payload_str = payload_str[:497] + "..."
                details.append(f"HTTP response payload: {payload_str}")

        self.logger.error(
            "Failed to initialize YTMusic with OAuth. %s",
            " | ".join(details),
        )

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
