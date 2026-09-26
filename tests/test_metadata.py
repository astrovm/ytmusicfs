import errno
import logging
import unittest
from unittest.mock import MagicMock, Mock

from ytmusicfs.metadata import MetadataManager


class TestMetadataManager(unittest.TestCase):
    def setUp(self):
        self.cache = Mock()
        self.logger = logging.getLogger("test")
        self.thread_manager = Mock()
        self.thread_manager.create_lock.return_value = MagicMock()
        self.metadata = MetadataManager(self.cache, self.logger, self.thread_manager)

    def test_get_video_id_repairs_stale_directory_type_for_audio_path(self):
        path = "/playlists/Mix/Artist - Song.m4a"
        self.cache.get_entry_type.return_value = "directory"
        self.cache.get.return_value = "video123"

        result = self.metadata.get_video_id(path)

        self.assertEqual(result, "video123")
        self.cache.mark_valid.assert_called_once_with(path, is_directory=False)

    def test_get_video_id_rejects_non_audio_directory(self):
        self.cache.get_entry_type.return_value = "directory"

        with self.assertRaises(OSError) as context:
            self.metadata.get_video_id("/playlists/Mix")

        self.assertEqual(context.exception.errno, errno.EINVAL)

    def test_get_video_id_scans_processed_tracks_once(self):
        path = "/playlists/Mix/Artist - Song.m4a"
        self.cache.get_entry_type.return_value = "file"
        self.cache.get_file_attrs_from_parent_dir.return_value = None
        self.cache.get.side_effect = [
            None,
            [{"filename": "Artist - Song.m4a", "videoId": "video123"}],
        ]
        content_fetcher = Mock()
        content_fetcher.get_playlist_entry_from_path.return_value = {"id": "mix"}
        self.metadata.set_content_fetcher(content_fetcher)

        result = self.metadata.get_video_id(path)

        self.assertEqual(result, "video123")
        self.assertEqual(self.cache.get.call_count, 2)
        self.cache.set.assert_called_once_with(f"video_id:{path}", "video123")

    def test_get_video_id_returns_in_memory_hit_without_cache_lookup(self):
        path = "/liked_songs/Artist - Song.m4a"
        self.cache.get_entry_type.return_value = "file"
        self.metadata.video_id_cache[path] = "memo123"

        self.assertEqual(self.metadata.get_video_id(path), "memo123")
        self.cache.get.assert_not_called()

    def test_get_video_id_memoizes_persistent_cache_hit(self):
        path = "/liked_songs/Artist - Song.m4a"
        self.cache.get_entry_type.return_value = "file"
        self.cache.get.return_value = "video123"

        self.assertEqual(self.metadata.get_video_id(path), "video123")
        self.assertEqual(self.metadata.get_video_id(path), "video123")

        self.cache.get.assert_called_once_with(f"video_id:{path}")

    def test_get_video_id_uses_parent_listing_attrs(self):
        path = "/liked_songs/Artist - Song.m4a"
        self.cache.get_entry_type.return_value = "file"
        self.cache.get.return_value = None
        self.cache.get_file_attrs_from_parent_dir.return_value = {"videoId": "attr1"}

        self.assertEqual(self.metadata.get_video_id(path), "attr1")
        self.cache.set.assert_called_once_with(f"video_id:{path}", "attr1")
        self.assertEqual(self.metadata.video_id_cache[path], "attr1")

    def test_get_video_id_without_content_fetcher_raises_enoent(self):
        self.cache.get_entry_type.return_value = "file"
        self.cache.get.return_value = None
        self.cache.get_file_attrs_from_parent_dir.return_value = {"videoId": None}

        with self.assertRaises(OSError) as context:
            self.metadata.get_video_id("/liked_songs/Artist - Song.m4a")

        self.assertEqual(context.exception.errno, errno.ENOENT)

    def test_get_video_id_does_not_mark_unknown_track_as_valid(self):
        self.cache.get_entry_type.return_value = None
        self.cache.get.return_value = None
        self.cache.get_file_attrs_from_parent_dir.return_value = None
        content_fetcher = Mock()
        content_fetcher.get_playlist_entry_from_path.return_value = {"id": "mix"}
        self.metadata.set_content_fetcher(content_fetcher)

        with self.assertRaises(OSError):
            self.metadata.get_video_id("/playlists/Mix/Nobody - Nothing.m4a")

        self.cache.mark_valid.assert_not_called()

    def test_get_video_id_raises_enoent_when_directory_is_not_in_registry(self):
        self.cache.get_entry_type.return_value = "file"
        self.cache.get.return_value = None
        self.cache.get_file_attrs_from_parent_dir.return_value = None
        content_fetcher = Mock()
        content_fetcher.get_playlist_entry_from_path.return_value = None
        self.metadata.set_content_fetcher(content_fetcher)

        with self.assertRaises(OSError) as context:
            self.metadata.get_video_id("/playlists/Gone/Artist - Song.m4a")

        self.assertEqual(context.exception.errno, errno.ENOENT)
        content_fetcher.get_playlist_entry_from_path.assert_called_once_with(
            "/playlists/Gone"
        )

    def test_get_video_id_skips_malformed_and_id_less_processed_tracks(self):
        path = "/playlists/Mix/Artist - Song.m4a"
        self.cache.get_entry_type.return_value = "file"
        self.cache.get_file_attrs_from_parent_dir.return_value = None
        content_fetcher = Mock()
        content_fetcher.get_playlist_entry_from_path.return_value = {"id": "mix"}
        self.metadata.set_content_fetcher(content_fetcher)
        for tracks in (
            {"not": "a list"},
            [
                "not a dict",
                {"filename": "Other.m4a", "videoId": "other"},
                {"filename": "Artist - Song.m4a", "videoId": ""},
            ],
        ):
            with self.subTest(tracks=tracks):
                self.cache.get.side_effect = [None, tracks]
                with self.assertRaises(OSError) as context:
                    self.metadata.get_video_id(path)
                self.assertEqual(context.exception.errno, errno.ENOENT)
        self.cache.set.assert_not_called()

    def test_clear_cache_forgets_memoized_video_ids(self):
        path = "/liked_songs/Artist - Song.m4a"
        self.cache.get_entry_type.return_value = "file"
        self.metadata.video_id_cache[path] = "memo123"
        self.cache.get.return_value = "fresh456"

        self.metadata.clear_cache()

        self.assertEqual(self.metadata.video_id_cache, {})
        self.assertEqual(self.metadata.get_video_id(path), "fresh456")


if __name__ == "__main__":
    unittest.main()
