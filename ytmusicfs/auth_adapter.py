#!/usr/bin/env python3

"""Authentication adapter for ytmusicapi using browser cookies."""

import logging
import time
from json import JSONDecodeError
from typing import Any, Dict, Optional

from ytmusicapi import YTMusic

from ytmusicfs.http_utils import _build_sapisidhash

_YT_ORIGIN = "https://music.youtube.com"
_VALIDATION_ATTEMPTS = 3
_VALIDATION_RETRY_DELAY_SECONDS = 1.0


class YTMusicAuthAdapter:
    """Thin wrapper that instantiates :class:`ytmusicapi.YTMusic`.

    The adapter builds fresh YouTube Music auth headers from local browser
    cookies and validates them with a small library request.
    """

    def __init__(
        self,
        browser: str,
        yt_dlp_utils: Any,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Create a YTMusic client authenticated from browser cookies."""
        self.logger = logger or logging.getLogger(__name__)
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
        auth = self._build_browser_auth()
        client = YTMusic(auth=auth)
        self.logger.info(
            "Authenticated with YouTube Music using %s browser cookies",
            self.browser,
        )
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
        for attempt in range(1, _VALIDATION_ATTEMPTS + 1):
            try:
                result = self.ytmusic.get_library_playlists(limit=1)
                break
            except JSONDecodeError as exc:
                if attempt == _VALIDATION_ATTEMPTS:
                    raise RuntimeError(
                        "YouTube Music auth validation returned an empty or "
                        "non-JSON response after "
                        f"{_VALIDATION_ATTEMPTS} attempts"
                    ) from exc

                self.logger.warning(
                    "YouTube Music auth validation returned an empty or "
                    "non-JSON response; retrying (%s/%s)",
                    attempt,
                    _VALIDATION_ATTEMPTS,
                )
                time.sleep(_VALIDATION_RETRY_DELAY_SECONDS)

        if not result:
            raise ValueError(
                "YouTube Music returned an empty playlist list. "
                "Your browser auth may not match an active YouTube Music session."
            )

    def __getattr__(self, name: str) -> Any:
        """Delegate attribute access to the underlying :class:`YTMusic` client."""
        return getattr(self.ytmusic, name)
