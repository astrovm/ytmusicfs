#!/usr/bin/env python3

"""Authentication adapter for ytmusicapi using browser headers."""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from ytmusicapi import YTMusic

from ytmusicfs.http_utils import _build_sapisidhash

_YT_ORIGIN = "https://music.youtube.com"


class YTMusicAuthAdapter:
    """Thin wrapper that instantiates :class:`ytmusicapi.YTMusic`.

    The adapter is responsible for validating that the browser header file
    exists and logging helpful diagnostics when authentication fails.  The
    resulting :class:`YTMusic` instance exposes the complete API surface so the
    rest of the application can continue delegating calls transparently.
    """

    def __init__(
        self,
        auth_file: str,
        logger: Optional[logging.Logger] = None,
        browser: Optional[str] = None,
        yt_dlp_utils: Optional[Any] = None,
    ) -> None:
        """Load browser authentication headers and create a YTMusic client."""
        self.logger = logger or logging.getLogger(__name__)
        self.auth_path = Path(auth_file)
        self.browser = browser
        self.yt_dlp_utils = yt_dlp_utils

        try:
            self.ytmusic = self._create_client()
            self._validate_client()
        except Exception as exc:  # pragma: no cover - defensive logging path
            self.logger.error(
                "Failed to initialise YTMusic client: %s",
                exc,
            )
            raise

    def _create_client(self) -> YTMusic:
        if self.browser and self.yt_dlp_utils:
            try:
                auth = self._build_browser_auth()
                client = YTMusic(auth=auth)
                self.logger.info(
                    "Authenticated with YouTube Music using %s browser cookies",
                    self.browser,
                )
                return client
            except Exception as exc:
                if not self.auth_path.exists():
                    raise
                self.logger.warning(
                    "Browser cookie auth failed, falling back to %s: %s",
                    self.auth_path,
                    exc,
                )

        if not self.auth_path.exists():
            raise FileNotFoundError(f"Auth file not found: {self.auth_path}")

        client = YTMusic(auth=str(self.auth_path))
        self.logger.info("Authenticated with YouTube Music using browser headers")
        return client

    def _build_browser_auth(self) -> Dict[str, str]:
        cookies = self.yt_dlp_utils.extract_browser_cookies(self.browser)
        auth_header = _build_sapisidhash(cookies, _YT_ORIGIN)
        if not auth_header:
            raise ValueError(
                f"No YouTube SAPISID cookies found in {self.browser} browser profile"
            )

        cookie_header = "; ".join(
            f"{name}={value}" for name, value in sorted(cookies.items())
        )
        return {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Authorization": auth_header,
            "Content-Type": "application/json",
            "Cookie": cookie_header,
            "Origin": _YT_ORIGIN,
            "Referer": _YT_ORIGIN + "/",
            "User-Agent": "Mozilla/5.0",
            "X-Goog-AuthUser": "0",
            "X-Origin": _YT_ORIGIN,
        }

    def _validate_client(self) -> None:
        # Perform a very small request so we fail fast if auth is invalid.
        result = self.ytmusic.get_library_playlists(limit=1)
        if not result:
            raise ValueError(
                "YouTube Music returned an empty playlist list. "
                "Your browser auth may not match an active YouTube Music session."
            )

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying :class:`YTMusic` client."""
        return getattr(self.ytmusic, name)
