#!/usr/bin/env python3

"""Utility helpers for working with HTTP request metadata."""

from __future__ import annotations

import hashlib
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

_YT_ORIGIN = "https://music.youtube.com"


_HEADER_BLOCKLIST = {"host", "content-length"}


def _is_sapisidhash(value: str | None) -> bool:
    """Return ``True`` when *value* looks like a SAPISID-derived signature."""

    if not value:
        return False
    return value.strip().lower().startswith("sapisidhash ")


def sanitize_headers(headers: Mapping[str, Any] | None) -> dict[str, str]:
    """Return a copy of *headers* safe for use with ``requests``.

    ``yt-dlp`` may include pseudo headers such as ``Host`` or keys with
    ``None`` values that cause YouTube's CDN to reject the request with ``403``.
    The downloader only needs real HTTP headers with string values, so this
    helper removes any blocked keys and normalises the remainder to strings.
    """

    if not headers:
        return {}

    sanitized: dict[str, str] = {}
    for key, value in headers.items():
        if value is None:
            continue
        key_str = str(key)
        if key_str.lower() in _HEADER_BLOCKLIST:
            continue
        sanitized[key_str] = str(value)
    return sanitized


def _ensure_origin_headers(headers: dict[str, str]) -> dict[str, str]:
    """Augment *headers* with the defaults expected by YouTube's CDN.

    ``yt-dlp`` occasionally omits headers such as ``Origin`` or ``Referer`` in
    environments where authentication relies on browser cookies.  Google
    requires these headers to validate the ``SAPISIDHASH`` signature - without
    them the API rejects the request with ``HTTP 403``.  We therefore make sure
    the canonical YouTube Music origin headers are present before issuing the
    request.
    """

    defaults = [
        ("Origin", _YT_ORIGIN),
        ("Referer", _YT_ORIGIN + "/"),
        ("User-Agent", "Mozilla/5.0"),
        ("Accept", "*/*"),
        ("Accept-Language", "en-US,en;q=0.5"),
        ("Accept-Encoding", "identity"),
    ]

    existing_keys = {key.lower(): key for key in headers}

    for name, value in defaults:
        key_lower = name.lower()
        if key_lower in existing_keys:
            continue
        headers[name] = value
        existing_keys[key_lower] = name

    return headers


def _build_sapisidhash(
    cookies: Mapping[str, Any] | None, origin: str = _YT_ORIGIN
) -> str | None:
    """Return an ``Authorization`` header value based on SAPISID cookies.

    When the browser cookies include ``SAPISID`` or ``__Secure-3PAPISID`` the
    YouTube API expects requests to be signed using the ``SAPISIDHASH`` scheme
    (see https://developers.google.com/identity/sign-in/web/backend-auth).  The
    helper mirrors Chrome's behaviour by hashing the cookie with the request
    origin and the current Unix timestamp.
    """

    if not cookies:
        return None

    for key in ("SAPISID", "__Secure-3PAPISID", "__Secure-3PSID"):
        if key in cookies:
            sapisid = str(cookies[key])
            break
    else:
        return None

    timestamp = int(time.time())
    data = f"{timestamp} {sapisid} {origin}".encode()
    digest = hashlib.sha1(data).hexdigest()
    return f"SAPISIDHASH {timestamp}_{digest}"


def _set_sapisidhash_authorization(
    headers: dict[str, str],
    existing_auth_key: str | None,
    auth_source: Mapping[str, Any] | None,
) -> None:
    if not auth_source:
        return

    origin_key = next(key for key in headers if key.lower() == "origin")
    auth_header = _build_sapisidhash(auth_source, headers[origin_key])
    if not auth_header:
        return

    current_auth = headers.get(existing_auth_key) if existing_auth_key else None
    if existing_auth_key and not _is_sapisidhash(current_auth):
        return

    if existing_auth_key and existing_auth_key != "Authorization":
        headers.pop(existing_auth_key, None)
    headers["Authorization"] = auth_header


def sanitize_cookies(cookies: Mapping[str, Any] | None) -> dict[str, str] | None:
    """Return cookie mapping compatible with ``requests``.

    ``yt-dlp`` may provide cookies with ``None`` values; ``requests`` treats
    them as literal ``"None"`` strings which makes authentication fail.  We
    filter out falsey entries and ensure all values are strings.
    """

    if not cookies:
        return None

    sanitized: dict[str, str] = {}
    for key, value in cookies.items():
        if value is None:
            continue
        sanitized[str(key)] = str(value)

    return sanitized or None


def merge_cookie_sources(
    headers: dict[str, str], cookies: dict[str, str] | None
) -> tuple[dict[str, str], dict[str, str] | None]:
    """Merge cookie information from headers and mapping for ``requests``.

    ``yt-dlp`` sometimes returns both a ``Cookie`` header string and an
    accompanying cookie mapping.  ``requests`` prefers the mapping over the
    header, so any credentials present only in the header would otherwise be
    dropped.  This helper extracts cookies from the header, merges them with the
    mapping (with the mapping taking precedence) and removes the redundant
    header entry.
    """

    existing_auth_key = next(
        (key for key in headers if key.lower() == "authorization"),
        None,
    )

    cookie_header_key = None
    for key in list(headers.keys()):
        if key.lower() == "cookie":
            cookie_header_key = key
            break

    if cookie_header_key is None:
        headers = _ensure_origin_headers(headers)
        _set_sapisidhash_authorization(headers, existing_auth_key, cookies)
        return headers, cookies

    cookie_header_value = headers.pop(cookie_header_key)
    header_cookies: dict[str, str] = {}
    if cookie_header_value:
        for part in cookie_header_value.split(";"):
            name, sep, value = part.strip().partition("=")
            if not sep:
                continue
            header_cookies[name.strip()] = value.strip()

    headers = _ensure_origin_headers(headers)
    if existing_auth_key is None:
        existing_auth_key = next(
            (key for key in headers if key.lower() == "authorization"),
            None,
        )

    merged = dict(header_cookies)
    if cookies:
        merged.update(cookies)

    auth_source = merged or cookies

    _set_sapisidhash_authorization(headers, existing_auth_key, auth_source)

    cookie_result: dict[str, str] | None
    if merged:
        cookie_result = merged
    elif cookies:
        cookie_result = dict(cookies)
    else:
        cookie_result = None

    return headers, cookie_result


def ensure_headers_and_cookies(
    headers: Mapping[str, Any] | None,
    cookies: Mapping[str, Any] | None,
) -> tuple[dict[str, str], dict[str, str] | None]:
    """Sanitise and augment the headers/cookies pair for outbound requests."""

    sanitized_headers = sanitize_headers(headers)
    sanitized_cookies = sanitize_cookies(cookies)
    return merge_cookie_sources(sanitized_headers, sanitized_cookies)
