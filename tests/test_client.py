#!/usr/bin/env python3

from json import JSONDecodeError
from unittest.mock import Mock, patch

import pytest

from ytmusicfs.client import YouTubeMusicClient


class TestYouTubeMusicClient:
    def test_library_playlist_fetch_retries_transient_non_json_response(self):
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

    def test_library_playlist_fetch_raises_after_persistent_non_json_response(self):
        auth_adapter = Mock()
        auth_adapter.get_library_playlists.side_effect = JSONDecodeError(
            "Expecting value", "", 0
        )
        client = YouTubeMusicClient(auth_adapter=auth_adapter)

        with (
            patch("ytmusicfs.client.time.sleep") as mock_sleep,
            pytest.raises(JSONDecodeError),
        ):
            client.get_library_playlists(limit=1000)

        assert auth_adapter.get_library_playlists.call_count == 3
        assert mock_sleep.call_count == 2

    def test_other_api_errors_are_raised_without_retry(self):
        auth_adapter = Mock()
        auth_adapter.get_album.side_effect = ValueError("bad album")
        client = YouTubeMusicClient(auth_adapter=auth_adapter)

        with (
            patch("ytmusicfs.client.time.sleep") as mock_sleep,
            pytest.raises(ValueError, match="bad album"),
        ):
            client.get_album("MPREb_1")

        assert auth_adapter.get_album.call_count == 1
        mock_sleep.assert_not_called()

    @pytest.mark.parametrize(
        ("method", "args", "kwargs"),
        [
            ("get_liked_songs", (), {"limit": 5}),
            ("get_playlist", ("PL1",), {"limit": 7}),
            ("get_library_artists", (), {"limit": 3}),
            ("get_artist", ("UC1",), {}),
            ("get_library_albums", (), {"limit": 9}),
            ("get_album", ("MPREb_1",), {}),
        ],
    )
    def test_fetch_methods_forward_arguments_and_return_adapter_result(
        self, method, args, kwargs
    ):
        auth_adapter = Mock()
        sentinel = {"result": method}
        getattr(auth_adapter, method).return_value = sentinel
        client = YouTubeMusicClient(auth_adapter=auth_adapter)

        assert getattr(client, method)(*args, **kwargs) is sentinel
        getattr(auth_adapter, method).assert_called_once_with(*args, **kwargs)

    def test_search_maps_filter_type_and_returns_results(self):
        auth_adapter = Mock()
        auth_adapter.search.return_value = [{"videoId": "abc"}]
        client = YouTubeMusicClient(auth_adapter=auth_adapter)

        result = client.search("Oasis", filter_type="songs", scope="library", limit=5)

        assert result == [{"videoId": "abc"}]
        auth_adapter.search.assert_called_once_with(
            query="Oasis",
            filter="songs",
            scope="library",
            limit=5,
            ignore_spelling=False,
        )

    def test_search_returns_empty_list_for_non_list_response(self):
        auth_adapter = Mock()
        auth_adapter.search.return_value = {"unexpected": "shape"}
        client = YouTubeMusicClient(auth_adapter=auth_adapter)

        assert client.search("Oasis") == []

    def test_rate_song_returns_response_dict(self):
        auth_adapter = Mock()
        auth_adapter.rate_song.return_value = {"ok": True}
        client = YouTubeMusicClient(auth_adapter=auth_adapter)

        assert client.rate_song("vid", "LIKE") == {"ok": True}
        auth_adapter.rate_song.assert_called_once_with("vid", "LIKE")

    def test_rate_song_returns_none_for_non_dict_response(self):
        auth_adapter = Mock()
        auth_adapter.rate_song.return_value = "ok"
        client = YouTubeMusicClient(auth_adapter=auth_adapter)

        assert client.rate_song("vid", "INDIFFERENT") is None
