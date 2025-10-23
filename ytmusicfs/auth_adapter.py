#!/usr/bin/env python3

"""Authentication adapter for ytmusicapi using browser headers."""

from pathlib import Path
from typing import Any, Optional
import logging

from ytmusicapi import YTMusic


class YTMusicAuthAdapter:
    """Thin wrapper that instantiates :class:`ytmusicapi.YTMusic`.

    The adapter is responsible for validating that the browser header file
    exists and logging helpful diagnostics when authentication fails.  The
    resulting :class:`YTMusic` instance exposes the complete API surface so the
    rest of the application can continue delegating calls transparently.
    """

    def __init__(self, auth_file: str, logger: Optional[logging.Logger] = None) -> None:
        """Load browser authentication headers and create a YTMusic client."""
        self.logger = logger or logging.getLogger(__name__)
        self.auth_path = Path(auth_file)

        if not self.auth_path.exists():
            raise FileNotFoundError(f"Auth file not found: {self.auth_path}")

        try:
            self.ytmusic = YTMusic(auth=str(self.auth_path))
            # Perform a very small request so we fail fast if the headers are
            # invalid.  `get_library_playlists` is inexpensive and available to
            # every authenticated account.
            self.ytmusic.get_library_playlists(limit=1)
        except Exception as exc:  # pragma: no cover - defensive logging path
            self.logger.error(
                "Failed to initialise YTMusic client with browser headers: %s",
                exc,
            )
            raise

        self.logger.info("Authenticated with YouTube Music using browser headers")

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying :class:`YTMusic` client."""
        return getattr(self.ytmusic, name)
