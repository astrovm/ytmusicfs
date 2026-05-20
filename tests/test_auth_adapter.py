#!/usr/bin/env python3

from json import JSONDecodeError
from unittest.mock import Mock, patch

import pytest

from ytmusicfs.auth_adapter import YTMusicAuthAdapter


@patch("ytmusicfs.auth_adapter.YTMusic")
def test_browser_cookie_auth_builds_ytmusic_headers(mock_ytmusic):
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


def test_browser_cookie_auth_requires_sapisid():
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
    mock_ytmusic, mock_sleep
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
    mock_ytmusic, mock_sleep
):
    client = mock_ytmusic.return_value
    client.get_library_playlists.side_effect = JSONDecodeError("Expecting value", "", 0)
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
