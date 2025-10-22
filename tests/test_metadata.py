#!/usr/bin/env python3

import logging
import threading
import unittest
from unittest.mock import Mock

from ytmusicfs.metadata import MetadataManager


class TestMetadataManager(unittest.TestCase):
    def setUp(self):
        self.cache = Mock()
        self.cache.set = Mock()
        self.logger = logging.getLogger("test")
        self.thread_manager = Mock()
        self.thread_manager.create_lock.return_value = threading.RLock()
        self.content_fetcher = Mock()

        self.metadata = MetadataManager(
            cache=self.cache,
            logger=self.logger,
            thread_manager=self.thread_manager,
            content_fetcher=self.content_fetcher,
        )

    def test_get_video_id_from_cached_tracks(self):
        path = "/playlists/test/song.m4a"
        dir_path = "/playlists/test"
        cache_key = f"{dir_path}_processed"

        cache_state = {
            f"video_id:{path}": None,
            cache_key: [{"filename": "song.m4a", "videoId": "vid123"}],
        }

        self.cache.get.side_effect = lambda key: cache_state.get(key)
        self.cache.get_entry_type.return_value = "file"
        self.cache.get_file_attrs_from_parent_dir.return_value = None
        self.content_fetcher.get_playlist_entry_from_path.return_value = {
            "id": "PL123",
            "path": dir_path,
        }

        video_id = self.metadata.get_video_id(path)

        self.assertEqual(video_id, "vid123")
        self.cache.set.assert_called_once_with(f"video_id:{path}", "vid123")
        self.assertEqual(self.metadata.video_id_cache[path], "vid123")
        self.content_fetcher.fetch_playlist_content.assert_not_called()

    def test_get_video_id_refreshes_when_missing(self):
        path = "/playlists/test/song.m4a"
        dir_path = "/playlists/test"
        cache_key = f"{dir_path}_processed"

        cache_state = {
            f"video_id:{path}": None,
            cache_key: [],
        }

        self.cache.get.side_effect = lambda key: cache_state.get(key)
        self.cache.get_entry_type.return_value = "file"
        self.cache.get_file_attrs_from_parent_dir.return_value = None
        playlist_entry = {"id": "PL123", "path": dir_path}
        self.content_fetcher.get_playlist_entry_from_path.return_value = (
            playlist_entry
        )

        def refresh_side_effect(playlist_id, path_arg, force_refresh=True):
            cache_state[cache_key] = [
                {"filename": "song.m4a", "videoId": "vid456"}
            ]
            return ["song.m4a"]

        self.content_fetcher.fetch_playlist_content.side_effect = refresh_side_effect

        video_id = self.metadata.get_video_id(path)

        self.assertEqual(video_id, "vid456")
        self.content_fetcher.fetch_playlist_content.assert_called_once_with(
            "PL123", dir_path, force_refresh=True
        )
        self.cache.set.assert_called_with(f"video_id:{path}", "vid456")
        self.assertEqual(self.metadata.video_id_cache[path], "vid456")


if __name__ == "__main__":
    unittest.main()
