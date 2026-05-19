#!/usr/bin/env python3

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
        auth_file="/missing/browser.json",
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
@patch("ytmusicfs.auth_adapter.Path.exists", return_value=True)
def test_auth_file_used_without_browser(mock_exists, mock_ytmusic):
    client = mock_ytmusic.return_value
    client.get_library_playlists.return_value = [{"title": "Playlist"}]

    YTMusicAuthAdapter(auth_file="/tmp/browser.json")

    mock_ytmusic.assert_called_once_with(auth="/tmp/browser.json")


@patch("ytmusicfs.auth_adapter.Path.exists", return_value=False)
def test_browser_cookie_auth_requires_sapisid(mock_exists):
    ytdlp = Mock()
    ytdlp.extract_browser_cookies.return_value = {"SID": "sid"}

    with pytest.raises(ValueError, match="SAPISID"):
        YTMusicAuthAdapter(
            auth_file="/missing/browser.json",
            browser="brave",
            yt_dlp_utils=ytdlp,
        )
