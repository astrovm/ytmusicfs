#!/usr/bin/env python3

"""Tests for HTTP utility helpers."""

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
