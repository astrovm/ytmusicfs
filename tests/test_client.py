#!/usr/bin/env python3

from json import JSONDecodeError
from unittest.mock import Mock, patch

import pytest

from ytmusicfs.client import YouTubeMusicClient


def test_library_playlist_fetch_retries_transient_non_json_response():
    auth_adapter = Mock()
    auth_adapter.get_library_playlists.side_effect = [
        JSONDecodeError("Expecting value", "", 0),
        [{"title": "Playlist", "playlistId": "PL123"}],
    ]
    client = YouTubeMusicClient(auth_adapter=auth_adapter)

    with patch("ytmusicfs.client.time.sleep") as mock_sleep:
        result = client.get_library_playlists(limit=1000)

    assert result == [{"title": "Playlist", "playlistId": "PL123"}]
    assert auth_adapter.get_library_playlists.call_count == 2
    mock_sleep.assert_called_once_with(1.0)


def test_library_playlist_fetch_raises_after_persistent_non_json_response():
    auth_adapter = Mock()
    auth_adapter.get_library_playlists.side_effect = JSONDecodeError(
        "Expecting value", "", 0
    )
    client = YouTubeMusicClient(auth_adapter=auth_adapter)

    with patch("ytmusicfs.client.time.sleep") as mock_sleep:
        with pytest.raises(JSONDecodeError):
            client.get_library_playlists(limit=1000)

    assert auth_adapter.get_library_playlists.call_count == 3
    assert mock_sleep.call_count == 2
