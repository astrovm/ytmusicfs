#!/usr/bin/env python3

"""Tests for HTTP utility helpers."""

import hashlib
import threading
from unittest.mock import patch

import pytest

from ytmusicfs import http_utils
from ytmusicfs.http_utils import (
    ensure_headers_and_cookies,
    http_get,
    http_head,
    http_session,
    sanitize_cookies,
    sanitize_headers,
)


class TestEnsureHeadersAndCookies:
    def test_preserves_existing_authorization_header(self):
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

    def test_generates_sapisidhash_when_missing_authorization(self):
        headers = {"Some-Header": "value"}
        cookies = {"SAPISID": "another_cookie"}

        merged_headers, merged_cookies = ensure_headers_and_cookies(headers, cookies)

        assert merged_headers["Origin"] == "https://music.youtube.com"
        assert merged_headers["Authorization"].startswith("SAPISIDHASH ")
        assert merged_cookies == {"SAPISID": "another_cookie"}

    def test_refreshes_sapisidhash_when_stale(self, monkeypatch):
        headers = {
            "Authorization": "SAPISIDHASH 1111111111_deadbeef",
            "Cookie": "SAPISID=fresh_cookie",
        }
        cookies = {"SAPISID": "fresh_cookie"}

        fixed_timestamp = 1_700_000_000

        monkeypatch.setattr("ytmusicfs.http_utils.time.time", lambda: fixed_timestamp)

        merged_headers, merged_cookies = ensure_headers_and_cookies(headers, cookies)

        expected_digest = hashlib.sha1(
            f"{fixed_timestamp} fresh_cookie https://music.youtube.com".encode()
        ).hexdigest()
        expected_auth = f"SAPISIDHASH {fixed_timestamp}_{expected_digest}"

        assert merged_headers["Authorization"] == expected_auth
        assert merged_cookies == {"SAPISID": "fresh_cookie"}

    def test_replaces_lowercase_sapisidhash_header_with_canonical_key(self):
        headers = {"authorization": "sapisidhash 1_old"}
        cookies = {"__Secure-3PAPISID": "secure"}

        merged_headers, _ = ensure_headers_and_cookies(headers, cookies)

        assert "authorization" not in merged_headers
        assert merged_headers["Authorization"].startswith("SAPISIDHASH ")
        assert merged_headers["Authorization"] != "sapisidhash 1_old"

    def test_signs_with_existing_custom_origin(self, monkeypatch):
        monkeypatch.setattr("ytmusicfs.http_utils.time.time", lambda: 10)
        headers = {"origin": "https://www.youtube.com"}

        merged_headers, _ = ensure_headers_and_cookies(headers, {"SAPISID": "s"})

        digest = hashlib.sha1(b"10 s https://www.youtube.com").hexdigest()
        assert merged_headers["Authorization"] == f"SAPISIDHASH 10_{digest}"
        assert "Origin" not in merged_headers

    def test_leaves_authorization_unset_without_cookies(self):
        merged_headers, merged_cookies = ensure_headers_and_cookies(None, None)

        assert "Authorization" not in merged_headers
        assert merged_headers["Referer"] == "https://music.youtube.com/"
        assert merged_cookies is None

    def test_leaves_authorization_unset_without_sapisid_cookie(self):
        merged_headers, merged_cookies = ensure_headers_and_cookies(
            {"Cookie": "PREF=1; malformed"}, None
        )

        assert "Authorization" not in merged_headers
        assert merged_cookies == {"PREF": "1"}


class TestSanitizeHeaders:
    def test_sanitize_headers_drops_blocked_and_none_values(self):
        headers = {"Host": "x", "Content-Length": "3", "X-None": None, "X-Num": 5}

        assert sanitize_headers(headers) == {"X-Num": "5"}

    def test_sanitize_headers_returns_empty_dict_for_missing_headers(self):
        assert sanitize_headers(None) == {}


class TestSanitizeCookies:
    def test_sanitize_cookies_drops_none_values(self):
        assert sanitize_cookies({"A": None, "B": 2}) == {"B": "2"}

    @pytest.mark.parametrize("cookies", [None, {}, {"A": None}])
    def test_sanitize_cookies_returns_none_when_nothing_usable(self, cookies):
        assert sanitize_cookies(cookies) is None


class TestHttpSession:
    def test_http_session_is_reused_within_a_thread(self):
        assert http_session() is http_session()

    def test_http_session_is_separate_per_thread(self):
        other = []
        thread = threading.Thread(target=lambda: other.append(http_session()))
        thread.start()
        thread.join()

        assert other[0] is not http_session()
        other[0].close()

    def test_http_session_rejects_response_cookies(self):
        session = http_session()

        assert session.cookies.get_policy().allowed_domains() == ()


class TestHttpGet:
    def test_http_get_uses_thread_session(self):
        with patch.object(http_utils.http_session(), "get") as mock_get:
            result = http_get("https://example.com", timeout=3)

        assert result is mock_get.return_value
        mock_get.assert_called_once_with("https://example.com", timeout=3)


class TestHttpHead:
    def test_http_head_uses_thread_session(self):
        with patch.object(http_utils.http_session(), "head") as mock_head:
            result = http_head("https://example.com", allow_redirects=True)

        assert result is mock_head.return_value
        mock_head.assert_called_once_with("https://example.com", allow_redirects=True)
