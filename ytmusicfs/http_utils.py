#!/usr/bin/env python3

"""Utility helpers for working with HTTP request metadata."""

from typing import Mapping, Optional, Dict, Any, Tuple


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


def merge_cookie_sources(
    headers: Dict[str, str], cookies: Optional[Dict[str, str]]
) -> Tuple[Dict[str, str], Optional[Dict[str, str]]]:
    """Merge cookie information from headers and mapping for ``requests``.

    ``yt-dlp`` sometimes returns both a ``Cookie`` header string and an
    accompanying cookie mapping.  ``requests`` prefers the mapping over the
    header, so any credentials present only in the header would otherwise be
    dropped.  This helper extracts cookies from the header, merges them with the
    mapping (with the mapping taking precedence) and removes the redundant
    header entry.
    """

    cookie_header_key = None
    for key in list(headers.keys()):
        if key.lower() == "cookie":
            cookie_header_key = key
            break

    if cookie_header_key is None:
        return headers, cookies

    cookie_header_value = headers.pop(cookie_header_key)
    header_cookies: Dict[str, str] = {}
    if cookie_header_value:
        for part in cookie_header_value.split(";"):
            name, sep, value = part.strip().partition("=")
            if not sep:
                continue
            header_cookies[name.strip()] = value.strip()

    if not header_cookies:
        return headers, cookies

    merged = dict(header_cookies)
    if cookies:
        merged.update(cookies)

    return headers, merged

