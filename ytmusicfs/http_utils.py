#!/usr/bin/env python3

"""Utility helpers for working with HTTP request metadata."""

from typing import Mapping, Optional, Dict, Any


_HEADER_BLOCKLIST = {"host", "content-length"}


def sanitize_headers(headers: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    """Return a copy of *headers* safe for use with ``requests``.

    ``yt-dlp`` may include pseudo headers such as ``Host`` or keys with
    ``None`` values that cause YouTube's CDN to reject the request with ``403``.
    The downloader only needs real HTTP headers with string values, so this
    helper removes any blocked keys and normalises the remainder to strings.
    """

    if not headers:
        return {}

    sanitized: Dict[str, str] = {}
    for key, value in headers.items():
        if value is None:
            continue
        key_str = str(key)
        if key_str.lower() in _HEADER_BLOCKLIST:
            continue
        sanitized[key_str] = str(value)
    return sanitized


def sanitize_cookies(
    cookies: Optional[Mapping[str, Any]]
) -> Optional[Dict[str, str]]:
    """Return cookie mapping compatible with ``requests``.

    ``yt-dlp`` may provide cookies with ``None`` values; ``requests`` treats
    them as literal ``"None"`` strings which makes authentication fail.  We
    filter out falsey entries and ensure all values are strings.
    """

    if not cookies:
        return None

    sanitized: Dict[str, str] = {}
    for key, value in cookies.items():
        if value is None:
            continue
        sanitized[str(key)] = str(value)

    return sanitized or None

