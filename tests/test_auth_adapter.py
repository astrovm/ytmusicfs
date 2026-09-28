#!/usr/bin/env python3

from json import JSONDecodeError
from unittest.mock import Mock, patch

import pytest

from ytmusicfs.auth_adapter import YTMusicAuthAdapter


class TestYTMusicAuthAdapter:
    @patch("ytmusicfs.auth_adapter.YTMusic")
    def test_browser_cookie_auth_builds_ytmusic_headers(self, mock_ytmusic):
        client = mock_ytmusic.return_value
        client.get_library_playlists.return_value = [{"title": "Playlist"}]
        ytdlp = Mock()
        ytdlp.extract_browser_cookies.return_value = {
            "SAPISID": "sapisid",
            "SID": "sid",
        }

        adapter = YTMusicAuthAdapter(
            browser="brave",
            yt_dlp_utils=ytdlp,
        )

        assert adapter.ytmusic is client
        auth = mock_ytmusic.call_args.kwargs["auth"]
        assert auth["Authorization"].startswith("SAPISIDHASH ")
        assert "SAPISID=sapisid" in auth["Cookie"]
        assert "SID=sid" in auth["Cookie"]
        assert auth["X-Origin"] == "https://music.youtube.com"
        ytdlp.extract_browser_cookies.assert_called_once_with("brave")

    @patch("ytmusicfs.auth_adapter.YTMusic")
    def test_browser_cookie_auth_drops_session_tracking_cookies(self, mock_ytmusic):
        client = mock_ytmusic.return_value
        client.get_library_playlists.return_value = [{"title": "Playlist"}]
        ytdlp = Mock()
        ytdlp.extract_browser_cookies.return_value = {
            "SAPISID": "sapisid",
            "SID": "sid",
            "ST-123": "tracking",
            "ST-long": "x" * 100_000,
            "PREF": "pref",
        }

        YTMusicAuthAdapter(
            browser="brave",
            yt_dlp_utils=ytdlp,
        )

        auth = mock_ytmusic.call_args.kwargs["auth"]
        assert "SAPISID=sapisid" in auth["Cookie"]
        assert "SID=sid" in auth["Cookie"]
        assert "PREF=pref" in auth["Cookie"]
        assert "ST-123=" not in auth["Cookie"]
        assert "ST-long=" not in auth["Cookie"]

    @patch("ytmusicfs.auth_adapter.YTMusic")
    def test_browser_cookie_auth_accepts_empty_playlist_list(self, mock_ytmusic):
        client = mock_ytmusic.return_value
        client.get_library_playlists.return_value = []
        ytdlp = Mock()
        ytdlp.extract_browser_cookies.return_value = {
            "SAPISID": "sapisid",
            "SID": "sid",
        }

        adapter = YTMusicAuthAdapter(
            browser="brave",
            yt_dlp_utils=ytdlp,
        )

        assert adapter.ytmusic is client
        client.get_library_playlists.assert_called_once_with(limit=1)

    def test_browser_cookie_auth_requires_sapisid(self):
        ytdlp = Mock()
        ytdlp.extract_browser_cookies.return_value = {"SID": "sid"}

        with pytest.raises(ValueError, match="SAPISID"):
            YTMusicAuthAdapter(
                browser="brave",
                yt_dlp_utils=ytdlp,
            )

    @patch("ytmusicfs.auth_adapter.time.sleep")
    @patch("ytmusicfs.auth_adapter.YTMusic")
    def test_browser_cookie_auth_retries_transient_non_json_validation(
        self, mock_ytmusic, mock_sleep
    ):
        client = mock_ytmusic.return_value
        client.get_library_playlists.side_effect = [
            JSONDecodeError("Expecting value", "", 0),
            [{"title": "Playlist"}],
        ]
        ytdlp = Mock()
        ytdlp.extract_browser_cookies.return_value = {
            "SAPISID": "sapisid",
            "SID": "sid",
        }

        adapter = YTMusicAuthAdapter(
            browser="brave",
            yt_dlp_utils=ytdlp,
        )

        assert adapter.ytmusic is client
        assert client.get_library_playlists.call_count == 2
        mock_sleep.assert_called_once_with(1.0)

    @patch("ytmusicfs.auth_adapter.time.sleep")
    @patch("ytmusicfs.auth_adapter.YTMusic")
    def test_browser_cookie_auth_reports_persistent_non_json_validation(
        self, mock_ytmusic, mock_sleep
    ):
        client = mock_ytmusic.return_value
        client.get_library_playlists.side_effect = JSONDecodeError(
            "Expecting value", "", 0
        )
        ytdlp = Mock()
        ytdlp.extract_browser_cookies.return_value = {
            "SAPISID": "sapisid",
            "SID": "sid",
        }

        with pytest.raises(
            RuntimeError, match="empty or non-JSON response after 3 attempts"
        ):
            YTMusicAuthAdapter(
                browser="brave",
                yt_dlp_utils=ytdlp,
            )

        assert client.get_library_playlists.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("ytmusicfs.auth_adapter._VALIDATION_ATTEMPTS", 0)
    @patch("ytmusicfs.auth_adapter.YTMusic")
    def test_browser_cookie_auth_rejects_validation_without_attempts(
        self, mock_ytmusic
    ):
        ytdlp = Mock()
        ytdlp.extract_browser_cookies.return_value = {"SAPISID": "sapisid"}

        # Skipping validation must not silently count as valid auth.
        with pytest.raises(RuntimeError, match="made no attempts"):
            YTMusicAuthAdapter(browser="brave", yt_dlp_utils=ytdlp)

        mock_ytmusic.return_value.get_library_playlists.assert_not_called()

    @patch("ytmusicfs.auth_adapter.YTMusic")
    def test_browser_cookie_auth_propagates_other_validation_errors(self, mock_ytmusic):
        mock_ytmusic.return_value.get_library_playlists.side_effect = PermissionError(
            "401"
        )
        ytdlp = Mock()
        ytdlp.extract_browser_cookies.return_value = {"SAPISID": "sapisid"}

        with pytest.raises(PermissionError, match="401"):
            YTMusicAuthAdapter(browser="brave", yt_dlp_utils=ytdlp)

        assert mock_ytmusic.return_value.get_library_playlists.call_count == 1

    @patch("ytmusicfs.auth_adapter.YTMusic")
    def test_unknown_attributes_delegate_to_ytmusic_client(self, mock_ytmusic):
        client = mock_ytmusic.return_value
        client.get_library_playlists.return_value = []
        client.get_song.return_value = {"videoId": "abc"}
        ytdlp = Mock()
        ytdlp.extract_browser_cookies.return_value = {"SAPISID": "sapisid"}

        adapter = YTMusicAuthAdapter(browser="brave", yt_dlp_utils=ytdlp)

        assert adapter.get_song("abc") == {"videoId": "abc"}
        client.get_song.assert_called_once_with("abc")
