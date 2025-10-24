#!/usr/bin/env python3

"""Tests for HTTP utility helpers."""

import hashlib

from ytmusicfs.http_utils import ensure_headers_and_cookies


def test_preserves_existing_authorization_header():
    headers = {
        "Authorization": "Bearer ya29.A0ARrdaMockToken",
        "Cookie": "SAPISID=sapi_cookie; VISITOR_INFO1_LIVE=value",
    }
    cookies = {"SAPISID": "sapi_cookie"}

    merged_headers, merged_cookies = ensure_headers_and_cookies(headers, cookies)

    assert merged_headers["Authorization"] == "Bearer ya29.A0ARrdaMockToken"
    assert "Cookie" not in merged_headers
    assert merged_cookies == {
        "SAPISID": "sapi_cookie",
        "VISITOR_INFO1_LIVE": "value",
    }


def test_generates_sapisidhash_when_missing_authorization():
    headers = {"Some-Header": "value"}
    cookies = {"SAPISID": "another_cookie"}

    merged_headers, merged_cookies = ensure_headers_and_cookies(headers, cookies)

    assert merged_headers["Origin"] == "https://music.youtube.com"
    assert merged_headers["Authorization"].startswith("SAPISIDHASH ")
    assert merged_cookies == {"SAPISID": "another_cookie"}


def test_refreshes_sapisidhash_when_stale(monkeypatch):
    headers = {
        "Authorization": "SAPISIDHASH 1111111111_deadbeef",
        "Cookie": "SAPISID=fresh_cookie",
    }
    cookies = {"SAPISID": "fresh_cookie"}

    fixed_timestamp = 1_700_000_000

    monkeypatch.setattr(
        "ytmusicfs.http_utils.time.time", lambda: fixed_timestamp
    )

    merged_headers, merged_cookies = ensure_headers_and_cookies(headers, cookies)

    expected_digest = hashlib.sha1(
        f"{fixed_timestamp} fresh_cookie https://music.youtube.com".encode("utf-8")
    ).hexdigest()
    expected_auth = f"SAPISIDHASH {fixed_timestamp}_{expected_digest}"

    assert merged_headers["Authorization"] == expected_auth
    assert merged_cookies == {"SAPISID": "fresh_cookie"}
